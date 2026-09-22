//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"example.com/pg-live-sync/internal/cdc"
	"example.com/pg-live-sync/internal/destination"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func brokerSeed() []string {
	if brokers := os.Getenv("KAFKA_BROKERS_INT"); brokers != "" {
		return splitAndTrim(brokers)
	}
	return []string{"localhost:9092"}
}

// The destination package's integration tests and these tests share the dest_test
// database (go test runs the two binaries in parallel), so every table name is
// unique per run.
func uniqueItTable(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()&0x7fffffffffffffff)
}

func destTestDSN() string {
	if dsn := os.Getenv("DESTINATION_DSN_INT"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5434/dest_test?sslmode=disable"
}

func integrationGateMain(t *testing.T) {
	t.Helper()
	if os.Getenv("PGCDC_INTEGRATION") != "1" {
		t.Skip("set PGCDC_INTEGRATION=1 to run the docker integration tests (make integration)")
	}
}

func ensureDestTestDatabaseMain(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5434/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to destination postgres database: %v", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dest_test')`).Scan(&exists); err != nil {
		t.Fatalf("check dest_test database: %v", err)
	}
	if !exists {
		if _, err := admin.Exec(ctx, `CREATE DATABASE dest_test`); err != nil {
			// A concurrent test package may have just created it (go test runs
			// package test binaries in parallel); tolerate that path.
			var stillExists bool
			if recheck := admin.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dest_test')`).Scan(&stillExists); recheck != nil || !stillExists {
				t.Fatalf("create dest_test database: %v", err)
			}
		}
	}
}

func newKafkaClient(t *testing.T, topic, group string) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerSeed()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// The crash-window test hands the group from one consumer to the next;
		// the default 60s rebalance timeout would make each handoff take a minute.
		kgo.RebalanceTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("create Kafka consumer: %v", err)
	}
	t.Cleanup(client.CloseAllowingRebalance)
	return client
}

// pollAll polls until want records are consumed or the timeout elapses. The
// bounded context guarantees the poll returns instead of blocking forever when
// fewer than want records ever arrive.
func pollAll(ctx context.Context, t *testing.T, client *kgo.Client, want int) []*kgo.Record {
	t.Helper()
	pollContext, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var got []*kgo.Record
	for {
		fetches := client.PollRecords(pollContext, want)
		if len(fetches.Errors()) > 0 {
			t.Fatalf("poll Kafka: %v", fetches.Errors())
		}
		got = append(got, fetches.Records()...)
		if len(got) >= want {
			return got[:want]
		}
		if pollContext.Err() != nil {
			t.Fatalf("timed out waiting for %d Kafka records, got %d", want, len(got))
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// applyRecordToGroup mirrors the writer's handleEvent bookkeeping (open the
// destination group transaction on sequence 1, validate ordering, buffer) WITHOUT
// the final closeGroup, so a test can drive each crash window by deciding what
// happens next: commit the destination transaction (which runs the batched
// apply) and crash before the Kafka offset, or crash before the destination
// commit at all.
func applyRecordToGroup(
	ctx context.Context,
	connection *pgx.Conn,
	group **groupState,
	record *kgo.Record,
	event cdc.Event,
) error {
	current := *group
	if current == nil {
		if event.Source.Sequence != 1 {
			return fmt.Errorf("test stream starts at sequence %d, want 1", event.Source.Sequence)
		}
		transaction, err := connection.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin destination transaction: %w", err)
		}
		current = &groupState{
			txid:       event.Source.TransactionID,
			count:      event.Source.TransactionEventCount,
			tx:         transaction,
			nextSeq:    1,
			lastRecord: record,
		}
	} else {
		if event.Source.TransactionID != current.txid {
			return fmt.Errorf("test stream mixed transactions %d and %d", current.txid, event.Source.TransactionID)
		}
		if event.Source.Sequence != current.nextSeq {
			return fmt.Errorf("test stream sequence %d, want %d", event.Source.Sequence, current.nextSeq)
		}
		current.lastRecord = record
	}
	current.buffer = append(current.buffer, bufferedEvent{record: record, event: event})
	current.lastRecord = record
	current.nextSeq++
	*group = current
	return nil
}

func crashEvent(id string, sequence int, idValue, table string) cdc.Event {
	value := idValue
	return cdc.Event{
		Version:   cdc.EventVersion,
		ID:        id,
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationInsert,
		Key:       cdc.Row{"id": &value},
		After:     cdc.Row{"id": &value, "name": &value},
		Source: cdc.SourceMetadata{
			TransactionID:         777,
			CommitLSN:             "0/1F00000",
			TransactionEndLSN:     "0/1F00000",
			CommitTime:            time.Now(),
			Sequence:              sequence,
			TransactionEventCount: 2,
		},
	}
}

// The two deterministic crash windows proven against real Kafka:
//
//	A. after the destination transaction committed, before the Kafka offset was
//	   committed - the replay must be an idempotent no-op marker conflict;
//	B. before the destination commit - the replay must apply the effects once.
//
// In both cases the effects land exactly once and the offset advances exactly to
// the end of the topic.
func TestCrashWindowCommitOrderAndIdempotency(t *testing.T) {
	integrationGateMain(t)
	ensureDestTestDatabaseMain(t)
	ctx := context.Background()
	table := uniqueItTable("it_crash")

	connection, err := pgx.Connect(ctx, destTestDSN())
	if err != nil {
		t.Fatalf("connect to dest_test: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	applier := destination.NewApplier(connection)
	if err := applier.EnsureMetaSchema(ctx); err != nil {
		t.Fatalf("ensure meta schema: %v", err)
	}

	if _, err := connection.Exec(ctx, `DROP TABLE IF EXISTS public.`+table+` CASCADE`); err != nil {
		t.Fatalf("drop %s: %v", table, err)
	}
	t.Cleanup(func() {
		_, _ = connection.Exec(context.Background(), `DROP TABLE IF EXISTS public.`+table+` CASCADE`)
		_, _ = connection.Exec(context.Background(),
			`DELETE FROM public.cdc_applied_events WHERE event_id LIKE 'it-crash-kafka-%'`)
	})
	if _, err := connection.Exec(ctx, `
		CREATE TABLE public.`+table+` (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			source_lsn PG_LSN NOT NULL DEFAULT '0/0',
			source_commit_time TIMESTAMPTZ NOT NULL DEFAULT now(),
			replicated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "it-crash-" + suffix
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokerSeed()...))
	if err != nil {
		t.Fatalf("create admin client: %v", err)
	}
	t.Cleanup(adminClient.Close)
	admin := kadm.NewClient(adminClient)
	if _, err := admin.CreateTopics(ctx, 1, 3, nil, topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	t.Cleanup(func() { _, _ = admin.DeleteTopics(context.Background(), topic) })

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerSeed()...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(producer.Close)
	for _, event := range []cdc.Event{
		crashEvent("it-crash-kafka-0000000000000001", 1, "42", table),
		crashEvent("it-crash-kafka-0000000000000002", 2, "43", table),
	} {
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		if results := producer.ProduceSync(ctx, &kgo.Record{Key: event.KafkaKey(), Value: payload}); results.FirstErr() != nil {
			t.Fatalf("produce %s: %v", event.ID, results.FirstErr())
		}
	}

	for _, crash := range []bool{true, false} {
		name := "crash_after_db_commit"
		if !crash {
			name = "crash_before_db_commit"
		}
		t.Run(name, func(t *testing.T) {
			group := "it-crash-" + suffix + "-" + name

			// First consumer: process the whole transaction, then hit the crash
			// window. The Kafka offset is never committed.
			clientOne := newKafkaClient(t, topic, group)
			observerOne := destination.NewObserver(clientOne, topic, group)
			records := pollAll(ctx, t, clientOne, 2)
			var groupOne *groupState
			for _, record := range records {
				var event cdc.Event
				if err := json.Unmarshal(record.Value, &event); err != nil {
					t.Fatalf("decode %s: %v", record.Key, err)
				}
				if err := applyRecordToGroup(ctx, connection, &groupOne, record, event); err != nil {
					t.Fatalf("apply %s pre-crash: %v", event.ID, err)
				}
			}
			if crash {
				if err := commitDestinationTransaction(context.Background(), "", applier, observerOne, groupOne); err != nil {
					t.Fatalf("commit destination transaction pre-crash: %v", err)
				}
			} else if groupOne != nil && groupOne.tx != nil {
				if err := groupOne.tx.Rollback(context.Background()); err != nil {
					t.Fatalf("roll back pre-crash transaction: %v", err)
				}
			}

			// Second consumer re-joins the same group; because the offset was
			// never advanced, every record arrives again and goes through the real
			// handleEvent -> closeGroup path, which commits the destination
			// transaction and then the Kafka offset.
			clientTwo := newKafkaClient(t, topic, group)
			observerTwo := destination.NewObserver(clientTwo, topic, group)
			replayed := pollAll(ctx, t, clientTwo, 2)
			var groupTwo *groupState
			for _, record := range replayed {
				var event cdc.Event
				if err := json.Unmarshal(record.Value, &event); err != nil {
					t.Fatalf("decode replayed %s: %v", record.Key, err)
				}
				if err := handleEvent(ctx, connection, clientTwo, "", applier, observerTwo, &groupTwo, record, event); err != nil {
					t.Fatalf("apply %s post-crash: %v", event.ID, err)
				}
			}
			clientTwo.CloseAllowingRebalance()

			var rowCount int
			if err := connection.QueryRow(ctx, `SELECT count(*) FROM public.`+table).Scan(&rowCount); err != nil {
				t.Fatalf("count rows: %v", err)
			}
			if rowCount != 2 {
				t.Fatalf("want exactly 2 rows after crash/replay, got %d", rowCount)
			}
			var markerCount int
			if err := connection.QueryRow(ctx,
				`SELECT count(*) FROM public.cdc_applied_events WHERE event_id = ANY($1::text[])`,
				[]string{"it-crash-kafka-0000000000000001", "it-crash-kafka-0000000000000002"}).Scan(&markerCount); err != nil {
				t.Fatalf("count markers: %v", err)
			}
			if markerCount != 2 {
				t.Fatalf("want exactly 2 markers, got %d", markerCount)
			}

			offsets, err := admin.FetchOffsetsForTopics(ctx, group, topic)
			if err != nil {
				t.Fatalf("fetch committed offsets: %v", err)
			}
			offset, ok := offsets.Lookup(topic, 0)
			if !ok || offset.Err != nil {
				t.Fatalf("no committed offset for group %s: %+v", group, offset)
			}
			if offset.At != 2 {
				t.Fatalf("committed offset must be 2 (processed 0 and 1), got %d", offset.At)
			}
		})
	}
}

// The Kafka offset seam itself: a committed record advances the group exactly to
// offset+1, which is the invariant the crash-window test relies on.
func TestCommitKafkaOffsetSeam(t *testing.T) {
	integrationGateMain(t)
	ctx := context.Background()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "it-seam-" + suffix
	group := "it-seam-" + suffix
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokerSeed()...))
	if err != nil {
		t.Fatalf("create admin client: %v", err)
	}
	t.Cleanup(adminClient.Close)
	admin := kadm.NewClient(adminClient)
	if _, err := admin.CreateTopics(ctx, 1, 3, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.DeleteTopics(context.Background(), topic) })

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerSeed()...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(producer.Close)
	event := crashEvent("it-seam-0000000000000000", 1, "1", uniqueItTable("it_seam"))
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if results := producer.ProduceSync(ctx, &kgo.Record{Value: payload}); results.FirstErr() != nil {
		t.Fatalf("produce: %v", results.FirstErr())
	}

	client := newKafkaClient(t, topic, group)
	record := pollAll(ctx, t, client, 1)[0]
	if err := commitKafkaOffsetAndAllowRebalance(ctx, client, &groupState{lastRecord: record}); err != nil {
		t.Fatalf("commit Kafka offset: %v", err)
	}
	client.CloseAllowingRebalance()

	offsets, err := admin.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		t.Fatalf("fetch committed offsets: %v", err)
	}
	offset, ok := offsets.Lookup(topic, 0)
	if !ok || offset.Err != nil {
		t.Fatalf("no committed offset for group %s: %+v", group, offset)
	}
	if offset.At != record.Offset+1 {
		t.Fatalf("committed offset must be record+1 (%d), got %d", record.Offset+1, offset.At)
	}
}
