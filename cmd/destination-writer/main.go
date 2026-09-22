package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/artie-mini-cdc/internal/cdc"
	"example.com/artie-mini-cdc/internal/destination"
	"example.com/artie-mini-cdc/internal/errclass"
	"example.com/artie-mini-cdc/internal/metrics"
	"example.com/artie-mini-cdc/internal/transport"
	"example.com/artie-mini-cdc/internal/retry"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	observerInterval   = 15 * time.Second
	snapshotSlotWait   = 60 * time.Second
	kafkaMaxFetchBytes = 4 << 20
)

type config struct {
	destinationDSN string
	sourceDSN      string
	slot           string
	publication    string
	kafkaBrokers   []string
	kafkaTopic     string
	kafkaGroup     string
}

// groupState tracks one source transaction being replayed into the destination.
// Every event of a source transaction lands in the same destination transaction,
// so the destination commits a whole source transaction atomically or not at all.
// buffer holds every event of the group: the whole transaction is batched into
// the destination transaction at commit time, and a poison event anywhere in it
// rolls the transaction back so the healthy events are re-applied individually.
type groupState struct {
	txid       uint32
	count      int
	nextSeq    int
	tx         pgx.Tx
	lastRecord *kgo.Record
	buffer     []bufferedEvent
	startedAt  time.Time
}

type bufferedEvent struct {
	record *kgo.Record
	event  cdc.Event
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, loadConfig()); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("destination writer stopped: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	metrics.StartServer(":" + envOrDefault("METRICS_PORT", "9091"))

	destinationConnection, err := transport.ConnectPostgres(ctx, cfg.destinationDSN)
	if err != nil {
		return fmt.Errorf("connect to destination PostgreSQL: %w", err)
	}
	defer func() { _ = destinationConnection.Close(context.Background()) }()

	applier := destination.NewApplier(destinationConnection)
	if err := applier.EnsureMetaSchema(ctx); err != nil {
		return err
	}

	kafkaOptions := []kgo.Opt{
		kgo.SeedBrokers(cfg.kafkaBrokers...),
		kgo.ConsumeTopics(cfg.kafkaTopic),
		kgo.ConsumerGroup(cfg.kafkaGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// #14 explicit Kafka-to-destination credit: one record is pulled from the
		// (internally buffered) fetch at a time and broker fetches are capped, so a
		// slow destination turns into Kafka-side lag instead of unbounded local
		// buffering between Kafka and the database.
		kgo.FetchMaxBytes(kafkaMaxFetchBytes),
	}
	kafkaOptions = append(kafkaOptions, transport.KafkaOptions()...)
	kafkaClient, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		return fmt.Errorf("create Kafka consumer: %w", err)
	}
	defer kafkaClient.CloseAllowingRebalance()
	if err := kafkaClient.Ping(ctx); err != nil {
		return fmt.Errorf("connect to Kafka: %w", err)
	}
	if err := verifySinglePartition(ctx, kafkaClient, cfg.kafkaTopic); err != nil {
		return err
	}

	// Initial snapshot: before the writer touches Kafka, the destination is
	// seeded from the source at the slot's consistent point. Live events then
	// apply on top of it, guarded by their commit LSNs, so snapshot rows and
	// live rows can never fight.
	snapshotter := destination.NewSnapshotter(cfg.sourceDSN, cfg.slot, cfg.publication, applier)
	if err := bootSnapshot(ctx, snapshotter, cfg.slot); err != nil {
		return err
	}

	observer := destination.NewObserver(kafkaClient, cfg.kafkaTopic, cfg.kafkaGroup)
	observer.Start(ctx, observerInterval)

	log.Printf("consuming Kafka topic %s as group %s", cfg.kafkaTopic, cfg.kafkaGroup)
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	return consume(ctx, destinationConnection, kafkaClient, cfg.sourceDSN, applier, observer)
}

func bootSnapshot(ctx context.Context, snapshotter *destination.Snapshotter, slot string) error {
	completed, err := snapshotter.Completed(ctx)
	if err != nil {
		return err
	}
	if !completed {
		log.Printf("initial snapshot not taken yet; waiting for replication slot %s", slot)
		if err := snapshotter.WaitForSlot(ctx, snapshotSlotWait); err != nil {
			return err
		}
		if err := snapshotter.RunSnapshot(ctx); err != nil {
			return err
		}
		if err := snapshotter.MarkCompleted(ctx); err != nil {
			return err
		}
	}
	return snapshotter.VerifyNotCorrupt(ctx)
}

// verifySinglePartition refuses a multi-partition topic: with more than one
// partition the events would no longer arrive globally ordered (#11).
func verifySinglePartition(ctx context.Context, client *kgo.Client, topic string) error {
	admin := kadm.NewClient(client)
	metadata, err := admin.Metadata(ctx, topic)
	if err != nil {
		return fmt.Errorf("fetch Kafka metadata for %q: %w", topic, err)
	}
	detail, ok := metadata.Topics[topic]
	if !ok {
		return fmt.Errorf("Kafka topic %q was not found", topic)
	}
	if detail.Err != nil {
		return fmt.Errorf("load Kafka topic %q: %w", topic, detail.Err)
	}
	if partitions := len(detail.Partitions); partitions != 1 {
		return fmt.Errorf("unsafe Kafka configuration: topic %q has %d partitions, want exactly 1 to preserve event order", topic, partitions)
	}
	return nil
}

func consume(
	ctx context.Context,
	connection *pgx.Conn,
	kafkaClient *kgo.Client,
	sourceDSN string,
	applier *destination.Applier,
	observer *destination.Observer,
) error {
	var group *groupState
	for {
		fetches := kafkaClient.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return shutdown(ctx, kafkaClient, sourceDSN, applier, observer, group)
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			// A broker outage surfaces here as poll errors. Re-poll inside a
			// backoff loop so the writer silently waits out the outage instead of
			// exiting and re-consuming from the last committed offset on restart.
			err := retry.Do(ctx, retry.Default, "poll Kafka", func() error {
				log.Printf("warn stage=kafka_poll_retry err=%v", fetchErrors)
				fetches = kafkaClient.PollRecords(ctx, 1)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
					return fmt.Errorf("poll Kafka: %v", fetchErrors)
				}
				return nil
			})
			if err != nil {
				return shutdown(ctx, kafkaClient, sourceDSN, applier, observer, group)
			}
		}

		for _, record := range fetches.Records() {
			var event cdc.Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				if group != nil {
					_ = group.tx.Rollback(context.Background())
				}
				return fmt.Errorf("decode Kafka record at partition %d offset %d: %w", record.Partition, record.Offset, err)
			}
			log.Printf(
				"trace stage=kafka_record_received event_id=%s operation=%s txid=%d seq=%d/%d source_commit_lsn=%s topic=%s partition=%d offset=%d key=%q",
				event.ID,
				event.Operation,
				event.Source.TransactionID,
				event.Source.Sequence,
				event.Source.TransactionEventCount,
				event.Source.CommitLSN,
				record.Topic,
				record.Partition,
				record.Offset,
				string(record.Key),
			)

			if err := handleEvent(ctx, connection, kafkaClient, sourceDSN, applier, observer, &group, record, event); err != nil {
				return err
			}
		}
	}
}

// handleEvent feeds one Kafka event into the current source-transaction group.
// Sequence 1 opens a group; the last sequence closes and commits it. Events are
// only buffered here; the whole source transaction is applied at commit time.
func handleEvent(
	ctx context.Context,
	connection *pgx.Conn,
	kafkaClient *kgo.Client,
	sourceDSN string,
	applier *destination.Applier,
	observer *destination.Observer,
	group **groupState,
	record *kgo.Record,
	event cdc.Event,
) error {
	current := *group
	if current == nil {
		if event.Source.Sequence != 1 {
			metrics.Errors.WithLabelValues("ordering").Inc()
			return fmt.Errorf("out-of-order Kafka stream: event %s starts at sequence %d, want 1", event.ID, event.Source.Sequence)
		}
		transaction, err := connection.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin destination transaction for source transaction %d: %w", event.Source.TransactionID, err)
		}
		current = &groupState{
			txid:       event.Source.TransactionID,
			count:      event.Source.TransactionEventCount,
			tx:         transaction,
			nextSeq:    1,
			lastRecord: record,
			startedAt:  time.Now(),
		}
	} else {
		if event.Source.TransactionID != current.txid {
			metrics.Errors.WithLabelValues("ordering").Inc()
			return fmt.Errorf(
				"out-of-order Kafka stream: event %s has transaction %d while %d is still open",
				event.ID, event.Source.TransactionID, current.txid)
		}
		if event.Source.Sequence != current.nextSeq {
			metrics.Errors.WithLabelValues("ordering").Inc()
			return fmt.Errorf(
				"out-of-order Kafka stream: event %s has sequence %d, want %d",
				event.ID, event.Source.Sequence, current.nextSeq)
		}
	}

	current.buffer = append(current.buffer, bufferedEvent{record: record, event: event})
	current.lastRecord = record
	current.nextSeq++

	metrics.MessagesProcessed.WithLabelValues(string(event.Operation)).Inc()
	metrics.SetLag(time.Since(event.Source.CommitTime).Seconds(), retentionSeconds())
	metrics.PendingEvents.Set(float64(len(current.buffer)))
	metrics.GroupAgeSeconds.Set(time.Since(current.startedAt).Seconds())

	if current.nextSeq-1 == current.count {
		// The source transaction is complete: commit the destination write first,
		// then the Kafka offset. A crash in between replays the group and the
		// markers make it a no-op, which is exactly-once-on-replay.
		if err := closeGroup(ctx, kafkaClient, sourceDSN, applier, observer, current); err != nil {
			return err
		}
		*group = nil
		return nil
	}
	*group = current
	return nil
}

// closeGroup applies the whole buffered source transaction to the destination,
// commits it, then commits the Kafka offset, and only then allows a
// consumer-group rebalance.
func closeGroup(
	ctx context.Context,
	kafkaClient *kgo.Client,
	sourceDSN string,
	applier *destination.Applier,
	observer *destination.Observer,
	group *groupState,
) error {
	if err := commitDestinationTransaction(ctx, sourceDSN, applier, observer, group); err != nil {
		return err
	}
	if err := commitKafkaOffsetAndAllowRebalance(ctx, kafkaClient, group); err != nil {
		return err
	}
	log.Printf(
		"trace stage=destination_group_processed txid=%d events=%d topic=%s partition=%d processed_offset=%d next_offset=%d",
		group.txid,
		group.count,
		group.lastRecord.Topic,
		group.lastRecord.Partition,
		group.lastRecord.Offset,
		group.lastRecord.Offset+1,
	)
	metrics.BatchDurationSeconds.Observe(time.Since(group.startedAt).Seconds())
	metrics.BatchSize.Observe(float64(len(group.buffer)))
	metrics.PendingEvents.Set(0)
	metrics.GroupAgeSeconds.Set(0)
	return nil
}

// commitDestinationTransaction applies the entire buffered source transaction as
// one pgx.Batch pipeline inside the single destination transaction, then commits
// it. It is a separate seam from commitKafkaOffsetAndAllowRebalance so tests can
// drive each crash window deterministically instead of killing the process: a
// crash here leaves the Kafka offset behind and the group replays, with
// deduplication markers making the replay a no-op.
//
// A poison event inside the batch can never apply: the transaction is rolled
// back, the healthy events of the group are re-applied individually, and the
// poison is quarantined. The Kafka offset still advances so the poison is not
// retried forever.
func commitDestinationTransaction(
	ctx context.Context,
	sourceDSN string,
	applier *destination.Applier,
	observer *destination.Observer,
	group *groupState,
) error {
	if group.tx == nil {
		return nil
	}
	events := make([]cdc.Event, 0, len(group.buffer))
	for _, buffered := range group.buffer {
		events = append(events, buffered.event)
	}

	var poison *destination.BatchError
	if err := applier.ApplyBatch(ctx, group.tx, events); err != nil {
		if !errors.As(err, &poison) {
			// Transient failure (destination database hiccup or malformed group):
			// abort the group and exit; the container restart replays the group
			// from the last committed offset and the markers make it a no-op.
			if group.tx != nil {
				_ = group.tx.Rollback(context.Background())
				group.tx = nil
			}
			metrics.Errors.WithLabelValues("apply").Inc()
			return err
		}
		if err := group.tx.Rollback(context.Background()); err != nil {
			group.tx = nil
			return fmt.Errorf("roll back quarantined group: %w", err)
		}
		group.tx = nil
		if err := replayHealthyEvents(ctx, sourceDSN, applier, observer, group, poison); err != nil {
			return err
		}
		if err := quarantinePoison(ctx, sourceDSN, applier, observer, group, poison); err != nil {
			return err
		}
		metrics.Errors.WithLabelValues("poison").Inc()
		return nil
	}

	if err := group.tx.Commit(context.Background()); err != nil {
		_ = group.tx.Rollback(context.Background())
		group.tx = nil
		return fmt.Errorf("commit destination transaction for source transaction %d: %w", group.txid, err)
	}
	group.tx = nil
	return nil
}

// replayHealthyEvents re-applies every event of a rolled-back, poisoned group
// except the poison event itself, each in its own transaction. Every other event
// still lands; the group was never committed, so nothing existed to lose.
func replayHealthyEvents(
	ctx context.Context,
	sourceDSN string,
	applier *destination.Applier,
	observer *destination.Observer,
	group *groupState,
	poison *destination.BatchError,
) error {
	for _, buffered := range group.buffer {
		if buffered.event.ID == poison.Event.ID {
			continue
		}
		applied, err := applier.Apply(ctx, buffered.event)
		if err != nil {
			if errclass.IsPermanent(err) {
				reason := err.Error()
				if deadLetterErr := applier.RecordDeadLetter(ctx, buffered.event, reason); deadLetterErr != nil {
					return deadLetterErr
				}
				observer.RecordDeadLetter()
				metrics.DeadLetters.Inc()
				log.Printf("warn stage=destination_poison_quarantined event_id=%s txid=%d reason=%q", buffered.event.ID, group.txid, reason)
				continue
			}
			return err
		}
		if applied {
			observer.RecordApplied()
		} else {
			observer.RecordDuplicate()
		}
	}
	return nil
}

// quarantinePoison records the poison event. Column-added drift (#17) is first
// healed by adding the source column, then the event is retried once; everything
// else dead-letters the event so it never blocks the stream again.
func quarantinePoison(
	ctx context.Context,
	sourceDSN string,
	applier *destination.Applier,
	observer *destination.Observer,
	group *groupState,
	poison *destination.BatchError,
) error {
	reason := poison.Err.Error()
	if destination.IsUndefinedColumn(poison.Err) {
		repaired, repairErr := applier.RepairSchema(ctx, sourceDSN, poison.Event)
		if repairErr != nil {
			log.Printf("warn stage=schema_repair_failed event_id=%s txid=%d reason=%q", poison.Event.ID, group.txid, repairErr)
		} else if repaired {
			applied, applyErr := applier.Apply(ctx, poison.Event)
			if applyErr != nil {
				if !errclass.IsPermanent(applyErr) {
					return applyErr
				}
				reason = applyErr.Error()
			} else {
				if applied {
					observer.RecordApplied()
				} else {
					observer.RecordDuplicate()
				}
				log.Printf("note stage=schema_repaired event_id=%s txid=%d column_drift healed, event applied", poison.Event.ID, group.txid)
				return nil
			}
		}
	}
	if err := applier.RecordDeadLetter(ctx, poison.Event, reason); err != nil {
		return err
	}
	observer.RecordDeadLetter()
	metrics.DeadLetters.Inc()
	log.Printf("warn stage=destination_poison_quarantined event_id=%s txid=%d reason=%q", poison.Event.ID, group.txid, reason)
	return nil
}

// commitKafkaOffsetAndAllowRebalance advances the consumer group past the last
// processed record and only then permits a rebalance, so a crash at any earlier
// point always replays the group. See closeGroup.
func commitKafkaOffsetAndAllowRebalance(ctx context.Context, kafkaClient *kgo.Client, group *groupState) error {
	if group.lastRecord == nil {
		return nil
	}
	if err := retry.Do(ctx, retry.Default, "commit Kafka offset", func() error {
		if err := kafkaClient.CommitRecords(ctx, group.lastRecord); err != nil {
			log.Printf("warn stage=kafka_commit_retry txid=%d partition=%d processed_offset=%d err=%v",
				group.txid, group.lastRecord.Partition, group.lastRecord.Offset, err)
			return err
		}
		return nil
	}); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("commit Kafka offset %d after source transaction %d: %w", group.lastRecord.Offset, group.txid, err)
	}
	kafkaClient.AllowRebalance()
	return nil
}

// shutdown is the graceful-stop path (#23). A fully arrived source transaction is
// committed so it is not replayed pointlessly; an incomplete one is rolled back
// so the destination never holds half a source transaction. Either way the Kafka
// offset stays consistent because every marker is committed with its transaction.
func shutdown(
	ctx context.Context,
	kafkaClient *kgo.Client,
	sourceDSN string,
	applier *destination.Applier,
	observer *destination.Observer,
	group *groupState,
) error {
	if group != nil {
		if group.nextSeq-1 == group.count {
			log.Printf("warn stage=shutdown committing complete source transaction %d (%d events)", group.txid, group.count)
			groupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := closeGroup(groupContext, kafkaClient, sourceDSN, applier, observer, group); err != nil {
				log.Printf("warn stage=shutdown final commit failed err=%v", err)
			}
		} else {
			if group.tx != nil {
				_ = group.tx.Rollback(context.Background())
			}
			log.Printf("warn stage=shutdown discarded incomplete source transaction %d (%d of %d events seen)", group.txid, group.nextSeq-1, group.count)
		}
	}
	return ctx.Err()
}

func loadConfig() config {
	return config{
		destinationDSN: envOrDefault("DESTINATION_DSN", "postgres://postgres:postgres@localhost:5434/destination?sslmode=disable"),
		sourceDSN:      envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		slot:           envOrDefault("PG_REPLICATION_SLOT", "artie_demo_slot"),
		publication:    envOrDefault("PG_PUBLICATION", "artie_demo_pub"),
		kafkaBrokers:   splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		kafkaTopic:     envOrDefault("KAFKA_TOPIC", "artie.users"),
		kafkaGroup:     envOrDefault("KAFKA_CONSUMER_GROUP", "artie-destination-v1"),
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// retentionSeconds returns the source-retention budget the destination writer
// uses to compute cdc_retention_headroom_seconds. The default is 7 days, the
// same Kafka topic retention the source reader enforces.
func retentionSeconds() float64 {
	value := envOrDefault("RETENTION_SECONDS", "604800")
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return seconds
	}
	return 604800
}
