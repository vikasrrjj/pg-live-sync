package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"example.com/pg-live-sync/internal/cdc"
	"example.com/pg-live-sync/internal/metrics"
	"example.com/pg-live-sync/internal/retry"
	"example.com/pg-live-sync/internal/source"
	"example.com/pg-live-sync/internal/transport"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	standbyStatusInterval = 10 * time.Second

	// Kafka ACK durability requires a configuration that can survive broker
	// failures: at least three brokers, a replication factor of three, and at
	// least two in-sync replicas acknowledged before a produce is confirmed.
	// The reader refuses to run against anything weaker.
	minimumKafkaBrokers        = 3
	minimumKafkaReplication    = 3
	minimumKafkaInSyncReplicas = 2

	// Kafka retention: topics created by the reader keep events for 7 days so a
	// slow destination can still catch up within that window. One partition is
	// mandatory: it is what guarantees global event order end to end.
	kafkaTopicRetention = 7 * 24 * time.Hour

	// Kafka produce retries bound the producer's internal re-sends of a single
	// record. publishTransaction additionally wraps each event in an outer
	// backoff loop, so a broker outage is survived until that loop gives up.
	kafkaProduceRetries = 8

	// WAL keeps growing while Kafka is unreachable: the slot cannot advance past
	// the last Kafka-durable LSN. After this long without progress the reader
	// starts warning so an operator knows the disk-filling clock is running.
	walStallWarnInterval = 60 * time.Second

	readerMetricsInterval = 30 * time.Second
)

var postgresIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type config struct {
	sqlDSN         string
	replicationDSN string
	slot           string
	publication    string
	kafkaBrokers   []string
	kafkaTopic     string
}

type publicationResult struct {
	transaction *source.CommittedTransaction
	err         error
}

// publishWork carries one event to the publisher, or the transaction descriptor
// that marks the end of a transaction after every event has been sent.
type publishWork struct {
	event *cdc.Event
	tx    *source.CommittedTransaction
}

type readerMetrics struct {
	eventsPublished    atomic.Uint64
	transactionsAcked  atomic.Uint64
	publishFailures    atomic.Uint64
	replicationRejoins atomic.Uint64
}

// reader owns the mutable replication state. Keeping it in one struct lets
// reconnect, streaming, and the publisher share it without a forest of pointers.
// durableLSN is atomic: the metrics loop reads it while the stream loop writes it.
type reader struct {
	cfg             config
	kafka           *kgo.Client
	topic           string
	metrics         readerMetrics
	connection      *pgconn.PgConn
	decoder         *source.Decoder
	durableLSN      atomic.Uint64
	nextStatus      time.Time
	lastLSNAdvanced time.Time
}

func (r *reader) durable() pglogrepl.LSN {
	return pglogrepl.LSN(r.durableLSN.Load())
}

func (r *reader) advance(lsn pglogrepl.LSN) {
	r.durableLSN.Store(uint64(lsn))
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, loadConfig()); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("source reader stopped: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	if !postgresIdentifier.MatchString(cfg.slot) {
		return fmt.Errorf("invalid replication slot name %q", cfg.slot)
	}
	if !postgresIdentifier.MatchString(cfg.publication) {
		return fmt.Errorf("invalid publication name %q", cfg.publication)
	}

	metrics.StartServer(":" + envOrDefault("METRICS_PORT", "9090"))

	kafkaOptions := []kgo.Opt{
		kgo.SeedBrokers(cfg.kafkaBrokers...),
		kgo.DefaultProduceTopic(cfg.kafkaTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordRetries(kafkaProduceRetries),
		kgo.RetryBackoffFn(kafkaProduceBackoff),
	}
	kafkaOptions = append(kafkaOptions, transport.KafkaOptions()...)
	kafkaClient, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		return fmt.Errorf("create Kafka producer: %w", err)
	}
	defer kafkaClient.Close()
	if err := kafkaClient.Ping(ctx); err != nil {
		return fmt.Errorf("connect to Kafka: %w", err)
	}
	if err := ensureKafkaTopicConfig(ctx, kafkaClient, cfg.kafkaTopic); err != nil {
		return err
	}
	if err := verifyKafkaDurability(ctx, kafkaClient, cfg.kafkaTopic); err != nil {
		return err
	}

	// Connector fencing: only one source reader may own a replication slot. A
	// second instance is refused, so two readers cannot produce the same events
	// in parallel and race each other into Kafka.
	fence, err := acquireFence(ctx, cfg.sqlDSN, cfg.slot)
	if err != nil {
		return err
	}
	defer func() { _ = fence.Close(context.Background()) }()

	// Corrupted-state recovery: a slot whose required WAL has already been
	// recycled cannot be resumed. Comparing the slot's restart point against the
	// known-good prior checkpoint catches that early instead of half-streaming.
	startLSN, slotExists, err := findSlotLSN(ctx, cfg.sqlDSN, cfg.slot)
	if err != nil {
		return err
	}
	if err := verifySlotRecoverable(ctx, cfg.sqlDSN, cfg.slot, slotExists); err != nil {
		return err
	}
	identity, err := discoverIdentity(ctx, cfg)
	if err != nil {
		return err
	}

	reader := &reader{
		cfg:   cfg,
		kafka: kafkaClient,
		topic: cfg.kafkaTopic,
		decoder: source.NewDecoderWithIdentity(func(namespace, relation string) []string {
			return identity[namespace+"."+relation]
		}),
	}
	reader.advance(startLSN)
	reader.nextStatus = time.Now().Add(standbyStatusInterval)

	if !slotExists {
		createdLSN, err := createReplicationSlot(ctx, cfg)
		if err != nil {
			return err
		}
		startLSN = createdLSN
		reader.advance(startLSN)
		log.Printf("created replication slot %s at %s", cfg.slot, startLSN)
	} else {
		log.Printf("resuming replication slot %s at acknowledged LSN %s", cfg.slot, startLSN)
	}

	connection, err := dialReplication(ctx, cfg, startLSN)
	if err != nil {
		return err
	}
	reader.connection = connection
	reader.lastLSNAdvanced = time.Now()

	log.Printf("streaming PostgreSQL publication %s to Kafka topic %s", cfg.publication, cfg.kafkaTopic)
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	return reader.stream(ctx)
}

// ensureKafkaTopicConfig creates the destination topic once with the durability
// and ordering settings this pipeline depends on: one partition preserves source
// order and a replication factor of three protects against single-broker loss. If
// the topic already exists, creation is skipped and verifyKafkaDurability
// validates it.
func ensureKafkaTopicConfig(ctx context.Context, client *kgo.Client, topic string) error {
	admin := kadm.NewClient(client)
	configs := map[string]*string{
		"min.insync.replicas": kadm.StringPtr(strconv.Itoa(minimumKafkaInSyncReplicas)),
		"retention.ms":        kadm.StringPtr(strconv.FormatInt(int64(kafkaTopicRetention/time.Millisecond), 10)),
	}
	responses, err := admin.CreateTopics(ctx, 1, minimumKafkaReplication, configs, topic)
	if err != nil {
		return fmt.Errorf("create Kafka topic %q: %w", topic, err)
	}
	response, err := responses.On(topic, nil)
	if err == nil {
		if response.Err == nil {
			log.Printf("created Kafka topic %s (partitions=1 replication_factor=%d min.insync.replicas=%d retention=%s)",
				topic, minimumKafkaReplication, minimumKafkaInSyncReplicas, kafkaTopicRetention)
			return nil
		}
		if !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create Kafka topic %q: %w", topic, response.Err)
		}
		return nil
	}
	if !errors.Is(err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("create Kafka topic %q: %w", topic, err)
	}
	return nil
}

// verifyKafkaDurability rejects an unsafe Kafka cluster or topic configuration.
// The whole durability contract of this pipeline rests on Kafka never losing an
// acknowledged record, so the reader exits loudly instead of quietly producing
// to a cluster that cannot keep that promise. A topic with more than one
// partition would also break global ordering, so it is rejected too.
func verifyKafkaDurability(ctx context.Context, client *kgo.Client, topic string) error {
	admin := kadm.NewClient(client)

	metadata, err := admin.Metadata(ctx, topic)
	if err != nil {
		return fmt.Errorf("fetch Kafka metadata: %w", err)
	}
	if len(metadata.Brokers) < minimumKafkaBrokers {
		return fmt.Errorf("unsafe Kafka configuration: %d brokers, want at least %d", len(metadata.Brokers), minimumKafkaBrokers)
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
	if replicas := detail.Partitions.NumReplicas(); replicas < minimumKafkaReplication {
		return fmt.Errorf("unsafe Kafka configuration: topic %q has replication factor %d, want at least %d", topic, replicas, minimumKafkaReplication)
	}
	for _, partition := range detail.Partitions.Sorted() {
		if len(partition.ISR) < minimumKafkaInSyncReplicas {
			return fmt.Errorf("unsafe Kafka configuration: topic %q partition %d has %d in-sync replicas, want at least %d", topic, partition.Partition, len(partition.ISR), minimumKafkaInSyncReplicas)
		}
	}

	configs, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return fmt.Errorf("describe Kafka topic config for %q: %w", topic, err)
	}
	resource, err := configs.On(topic, nil)
	if err != nil {
		return fmt.Errorf("read Kafka topic config for %q: %w", topic, err)
	}
	minISR := 0
	for _, config := range resource.Configs {
		if config.Key != "min.insync.replicas" {
			continue
		}
		minISR, err = strconv.Atoi(config.MaybeValue())
		if err != nil {
			return fmt.Errorf("parse Kafka min.insync.replicas %q: %w", config.MaybeValue(), err)
		}
	}
	if minISR < minimumKafkaInSyncReplicas {
		return fmt.Errorf("unsafe Kafka configuration: topic %q min.insync.replicas is %d, want at least %d", topic, minISR, minimumKafkaInSyncReplicas)
	}

	log.Printf("kafka durability verified brokers=%d topic=%s partitions=%d replication_factor=%d min_insync_replicas=%d",
		len(metadata.Brokers), topic, len(detail.Partitions), detail.Partitions.NumReplicas(), minISR)
	return nil
}

// acquireFence takes a PostgreSQL advisory lock keyed by the slot name and holds
// it for the life of the process. pg_try_advisory_lock returns false if another
// process already holds it, which is the fencing guarantee (#24): a zombie reader
// cannot come back and claim the same slot.
func acquireFence(ctx context.Context, dsn, slot string) (*pgx.Conn, error) {
	connection, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL for fencing: %w", err)
	}
	key := fenceKey(slot)

	var acquired bool
	if err := connection.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		_ = connection.Close(context.Background())
		return nil, fmt.Errorf("acquire replication slot fence for %q: %w", slot, err)
	}
	if !acquired {
		_ = connection.Close(context.Background())
		return nil, fmt.Errorf("another source reader already holds the fence for slot %q; refusing to duplicate it", slot)
	}
	log.Printf("acquired replication slot fence for %s", slot)
	return connection, nil
}

// verifySlotRecoverable refuses to resume a slot whose required WAL has already
// been recycled away, which is how a corrupted state looks after the slot's
// retained history was lost. The authoritative check is the WAL file itself: as
// long as the segment holding the slot's restart point still exists, the server
// can resume replay from it regardless of how far ahead the latest checkpoint
// has moved.
func verifySlotRecoverable(ctx context.Context, dsn, slot string, slotExists bool) error {
	if !slotExists {
		return nil
	}
	connection, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL for corruption check: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()

	var restartText string
	if err := connection.QueryRow(ctx,
		`SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&restartText); err != nil {
		return fmt.Errorf("read slot %q restart point: %w", slot, err)
	}
	restartLSN, err := pglogrepl.ParseLSN(restartText)
	if err != nil {
		return fmt.Errorf("parse slot %q restart point %q: %w", slot, restartText, err)
	}
	var retained bool
	if err := connection.QueryRow(ctx,
		`SELECT EXISTS (SELECT 0 FROM pg_ls_waldir() WHERE name = pg_walfile_name($1))`,
		restartText).Scan(&retained); err != nil {
		return fmt.Errorf("verify WAL retention for slot %q: %w", slot, err)
	}
	if !retained {
		return fmt.Errorf(
			"corrupted state: slot %q restart point %s has been recycled away, "+
				"the required WAL no longer exists; restore the slot or reset the pipeline", slot, restartLSN)
	}
	log.Printf("slot %s recoverable: restart point %s is still retained in WAL storage", slot, restartLSN)
	return nil
}

// discoverIdentity refuses published tables whose replica identity cannot
// address rows on the pipeline's keyed destination, and returns for every
// published table the columns that must act as the row key. The pgoutput flags
// are not enough: REPLICA IDENTITY FULL flags every column as a key (fabricating
// a whole-row ON CONFLICT key) and DEFAULT with no primary key flags nothing.
// Resolution: DEFAULT -> primary key, USING INDEX -> the configured index's
// columns, FULL -> the primary key (the real stable key), with a clear refusal
// when no usable key exists. The result is handed to the decoder so Event keys
// and key-change detection always use these discovered columns.
func discoverIdentity(ctx context.Context, cfg config) (map[string][]string, error) {
	connection, err := transport.ConnectPostgres(ctx, cfg.sqlDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to source to discover replica identities: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()

	rows, err := connection.Query(ctx, `
		SELECT
			t.schemaname,
			t.tablename,
			c.relreplident,
			COALESCE(pk.cols, '{}') AS pk,
			COALESCE(ri.cols, '{}') AS ri
		FROM pg_publication_tables t
		JOIN pg_class c ON c.relname = t.tablename
		JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
		LEFT JOIN LATERAL (
			SELECT array_agg(a.attname ORDER BY k.ordinality) AS cols
			FROM pg_index i
			JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
			WHERE i.indrelid = c.oid AND i.indisprimary
		) pk ON true
		LEFT JOIN LATERAL (
			SELECT array_agg(a.attname ORDER BY k.ordinality) AS cols
			FROM pg_index i
			JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
			WHERE i.indexrelid = c.relreplidentindex
		) ri ON true
		WHERE t.pubname = $1
		ORDER BY t.schemaname, t.tablename`, cfg.publication)
	if err != nil {
		return nil, fmt.Errorf("list published tables for replica identity discovery: %w", err)
	}
	defer rows.Close()

	identity := make(map[string][]string)
	for rows.Next() {
		var schema, table, mode string
		var pk, ri []string
		if err := rows.Scan(&schema, &table, &mode, &pk, &ri); err != nil {
			return nil, fmt.Errorf("scan published table for replica identity discovery: %w", err)
		}
		var key []string
		qualified := schema + "." + table
		switch mode {
		case "n":
			return nil, fmt.Errorf(
				"unsafe source configuration: table %s has REPLICA IDENTITY NOTHING, "+
					"so deletes and update keys would be lost; run "+
					"ALTER TABLE %s REPLICA IDENTITY DEFAULT (or FULL / USING INDEX) and restart the reader",
				qualified, qualified)
		case "d":
			key = pk
		case "i":
			key = ri
		case "f":
			// FULL streams the whole old row, but the destination is keyed, so
			// the identity stays the actual primary key, not every column.
			key = pk
		default:
			return nil, fmt.Errorf("table %s has unknown replica identity %q", qualified, mode)
		}
		if len(key) == 0 {
			return nil, fmt.Errorf(
				"unsafe source configuration: table %s has REPLICA IDENTITY %s but no key the "+
					"pipeline can address rows with; add a PRIMARY KEY (FULL/DEFAULT) or set "+
					"ALTER TABLE %s REPLICA IDENTITY USING INDEX to a unique index",
				qualified, mode, qualified)
		}
		identity[qualified] = key
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate published tables for replica identity discovery: %w", err)
	}
	return identity, nil
}

// fenceKey maps a replication slot name to a stable 64-bit advisory-lock key,
// so the reader's fence never collides with unrelated locks on the database.
func fenceKey(slot string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(slot))
	return int64(hash.Sum64())
}

func createReplicationSlot(ctx context.Context, cfg config) (pglogrepl.LSN, error) {
	connection, err := pgconn.Connect(ctx, cfg.replicationDSN)
	if err != nil {
		return 0, fmt.Errorf("connect to PostgreSQL replication protocol: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()

	result, err := pglogrepl.CreateReplicationSlot(
		ctx,
		connection,
		cfg.slot,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: "NOEXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		return 0, fmt.Errorf("create replication slot %q: %w", cfg.slot, err)
	}
	lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("parse slot consistent point %q: %w", result.ConsistentPoint, err)
	}
	return lsn, nil
}

// dialReplication opens a fresh logical-replication connection and starts
// streaming from the given position. It is the single path used at boot and by
// every reconnect, so a dropped connection resumes from the last Kafka-durable
// LSN instead of restarting from slot creation.
func dialReplication(ctx context.Context, cfg config, resumeLSN pglogrepl.LSN) (*pgconn.PgConn, error) {
	connection, err := pgconn.Connect(ctx, cfg.replicationDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL replication protocol: %w", err)
	}
	err = pglogrepl.StartReplication(
		ctx,
		connection,
		cfg.slot,
		resumeLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", cfg.publication),
			},
		},
	)
	if err != nil {
		_ = connection.Close(context.Background())
		return nil, fmt.Errorf("start logical replication: %w", err)
	}
	return connection, nil
}

// stream is the heart of the reader. It preserves the invariant that the WAL
// position reported back to PostgreSQL never exceeds what Kafka has confirmed.
func (reader *reader) stream(ctx context.Context) error {
	publishContext, cancelPublish := context.WithCancel(ctx)
	defer cancelPublish()

	// Events flow one at a time from the decoder to the publisher through an
	// unbuffered channel: no transaction ever sits entirely in Kafka-client
	// memory, and a blocked produce naturally backpressures PostgreSQL.
	work := make(chan publishWork)
	results := make(chan publicationResult, 1)
	go publishTransactions(publishContext, reader.kafka, reader.topic, work, results, &reader.metrics)

	// Periodic observability: how much has been published since last count. The
	// metrics loop must stop whenever stream returns, not just on shutdown, so it
	// observes a cancel derived from the stream's own lifecycle.
	metricsContext, cancelMetrics := context.WithCancel(ctx)
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		ticker := time.NewTicker(readerMetricsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-metricsContext.Done():
				return
			case <-ticker.C:
				log.Printf(
					"trace stage=source_metrics events_published=%d transactions_acked=%d publish_failures=%d replication_rejoins=%d kafka_durable_lsn=%s",
					reader.metrics.eventsPublished.Load(),
					reader.metrics.transactionsAcked.Load(),
					reader.metrics.publishFailures.Load(),
					reader.metrics.replicationRejoins.Load(),
					reader.durable(),
				)
			}
		}
	}()
	defer func() {
		cancelMetrics()
		_ = (<-metricsDone)
	}()

	for {
		advanced, err := drainPublicationResults(results, reader)
		if err != nil {
			return reader.recoverPublication(ctx, err)
		}
		if advanced {
			if err := reader.sendStandby(ctx, "kafka_transaction_acked"); err != nil {
				return err
			}
			reader.nextStatus = time.Now().Add(standbyStatusInterval)
		}

		receiveContext, cancel := context.WithDeadline(ctx, reader.nextStatus)
		rawMessage, err := reader.connection.ReceiveMessage(receiveContext)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				// Graceful shutdown: report the position PostgreSQL may treat as
				// durable (never ahead of Kafka's ack) and bow out. The slot
				// stays; the next reader resumes exactly here.
				return reader.finalStandby()
			}
			if pgconn.Timeout(err) {
				advanced, err := drainPublicationResults(results, reader)
				if err != nil {
					return reader.recoverPublication(ctx, err)
				}
				reader.warnIfWALStalled(ctx)
				reason := "periodic"
				if advanced {
					reason = "kafka_transaction_acked"
				}
				if err := reader.sendStandby(ctx, reason); err != nil {
					return err
				}
				reader.nextStatus = time.Now().Add(standbyStatusInterval)
				continue
			}
			// The PostgreSQL side or the network dropped the logical stream.
			if err := reader.reconnect(ctx, fmt.Sprintf("receive: %v", err)); err != nil {
				return err
			}
			continue
		}

		copyData, ok := rawMessage.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse primary keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				if _, err := drainPublicationResults(results, reader); err != nil {
					return reader.recoverPublication(ctx, err)
				}
				if err := reader.sendStandby(ctx, "primary_keepalive_reply_requested"); err != nil {
					return err
				}
				reader.nextStatus = time.Now().Add(standbyStatusInterval)
			}

		case pglogrepl.XLogDataByteID:
			xlogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse XLogData: %w", err)
			}
			logicalMessage, err := pglogrepl.Parse(xlogData.WALData)
			if err != nil {
				return fmt.Errorf("parse pgoutput message at %s: %w", xlogData.WALStart, err)
			}
			logLogicalMessage(logicalMessage, xlogData.WALStart)
			transaction, err := reader.decoder.Handle(logicalMessage)
			if err != nil {
				metrics.Errors.WithLabelValues("decode").Inc()
				return fmt.Errorf("decode pgoutput message at %s: %w", xlogData.WALStart, err)
			}
			if transaction == nil {
				continue
			}
			if err := reader.queueTransactionEvents(ctx, transaction, work, results); err != nil {
				return err
			}
			log.Printf(
				"trace stage=source_transaction_queued txid=%d commit_lsn=%s end_lsn=%s events=%d",
				transaction.TransactionID,
				transaction.CommitLSN,
				transaction.EndLSN,
				transaction.Count,
			)
		}
	}
}

// queueTransactionEvents drains a committed transaction from the decoder into
// the publisher one event at a time, holding the slot open and answering
// PostgreSQL keepalives the whole way. Memory stays bounded by whichever comes
// first: the spool cap on the reader or the channel hand-off.
func (reader *reader) queueTransactionEvents(
	ctx context.Context,
	transaction *source.CommittedTransaction,
	work chan<- publishWork,
	results <-chan publicationResult,
) error {
	for sequence := 1; sequence <= transaction.Count; sequence++ {
		event, ok, err := reader.decoder.NextEvent()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("transaction %d ended early at sequence %d of %d", transaction.TransactionID, sequence, transaction.Count)
		}
		if err := reader.sendWork(ctx, publishWork{event: &event}, work, results); err != nil {
			return err
		}
	}
	return reader.sendWork(ctx, publishWork{tx: transaction}, work, results)
}

// sendWork hands a message to the publisher while still answering PostgreSQL
// keepalives, so a huge or heavily-contended transaction never starves the slot.
func (reader *reader) sendWork(
	ctx context.Context,
	message publishWork,
	work chan<- publishWork,
	results <-chan publicationResult,
) error {
	for {
		statusTimer := time.NewTimer(timeUntil(reader.nextStatus))
		select {
		case work <- message:
			stopTimer(statusTimer)
			return nil

		case result := <-results:
			// An ack for an earlier transaction arrived while we were blocked on
			// the channel. Note it now so PostgreSQL can release that WAL.
			stopTimer(statusTimer)
			if err := reader.applyPublicationResult(result); err != nil {
				return reader.recoverPublication(ctx, err)
			}
			if err := reader.sendStandby(ctx, "kafka_transaction_acked"); err != nil {
				return err
			}
			reader.nextStatus = time.Now().Add(standbyStatusInterval)

		case <-statusTimer.C:
			if err := reader.sendStandby(ctx, "publisher_backpressure"); err != nil {
				return err
			}
			reader.nextStatus = time.Now().Add(standbyStatusInterval)

		case <-ctx.Done():
			stopTimer(statusTimer)
			return ctx.Err()
		}
	}
}

// reconnect resumes the logical stream after a drop. It reconnects from the last
// Kafka-durable LSN, resets the decoder, and lets PostgreSQL replay anything the
// destination has not confirmed to Kafka yet.
func (reader *reader) reconnect(ctx context.Context, reason string) error {
	reader.metrics.replicationRejoins.Add(1)
	metrics.Errors.WithLabelValues("replication").Inc()
	log.Printf("warn stage=replication_reconnect reason=%q durable_lsn=%s", reason, reader.durable())
	err := retry.Do(ctx, retry.Default, "reconnect source replication", func() error {
		_ = reader.connection.Close(context.Background())
		connection, err := dialReplication(ctx, reader.cfg, reader.durable())
		if err != nil {
			log.Printf("warn stage=replication_reconnect retrying err=%v", err)
			return err
		}
		reader.connection = connection
		reader.decoder.Reset()
		reader.nextStatus = time.Now().Add(standbyStatusInterval)
		log.Printf("resumed logical replication at Kafka-durable LSN %s", reader.durable())
		return nil
	})
	return err
}

func (reader *reader) warnIfWALStalled(ctx context.Context) {
	stalledFor := time.Since(reader.lastLSNAdvanced)
	if stalledFor < walStallWarnInterval {
		return
	}
	// Only warn when WAL is actually piling up. An idle source has nothing new
	// to publish, so the slot legitimately stays put; a real outage shows up as
	// the walsender's flushed_lsn falling far behind the live insertion point.
	ahead := int64(-1)
	connection, err := transport.ConnectPostgres(ctx, reader.cfg.sqlDSN)
	if err == nil {
		err = connection.QueryRow(ctx, `
			SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), flushed_lsn)
			FROM pg_stat_replication
			WHERE slot_name = $1`,
			reader.cfg.slot).Scan(&ahead)
		_ = connection.Close(context.Background())
	}
	if err != nil || ahead <= 64*1024*1024 {
		return
	}
	log.Printf(
		"warn stage=wal_backpressure_publishing stalled_for=%s kafka_durable_lsn=%s wal_ahead_bytes=%d "+
			"WAL is growing because Kafka publication is blocked and the slot cannot advance",
		stalledFor.Round(time.Second),
		reader.durable(),
		ahead,
	)
}

// finalStandby is the graceful-shutdown path (#23): report exactly the
// Kafka-durable position (never anything ahead of it) and stop. PostgreSQL keeps
// every byte after that for the next reader.
func (reader *reader) finalStandby() error {
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := reader.sendStandby(shutdownContext, "shutdown"); err != nil {
		log.Printf("warn stage=shutdown standby status failed err=%v", err)
	}
	return context.Canceled
}

// recoverPublication turns a publication failure into a reconnect at the
// Kafka-durable LSN instead of an exit, so a Kafka outage reads as backoff, not
// a crash: the slot never advances past what Kafka confirmed, and PostgreSQL
// replays the un-published transaction once publishing succeeds again.
func (reader *reader) recoverPublication(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	reader.metrics.publishFailures.Add(1)
	metrics.Errors.WithLabelValues("kafka").Inc()
	return reader.reconnect(ctx, fmt.Sprintf("publish: %v", err))
}

func drainPublicationResults(results <-chan publicationResult, reader *reader) (bool, error) {
	advanced := false
	for {
		select {
		case result := <-results:
			if err := reader.applyPublicationResult(result); err != nil {
				return false, err
			}
			advanced = true
		default:
			return advanced, nil
		}
	}
}

// applyPublicationResult is the single place the reportable PostgreSQL position
// advances. That only happens after every event in the transaction has a
// successful Kafka produce result; an error leaves the LSN untouched so
// PostgreSQL retains and replays the transaction.
func (reader *reader) applyPublicationResult(result publicationResult) error {
	if result.err != nil {
		return result.err
	}
	if result.transaction.EndLSN < reader.durable() {
		return fmt.Errorf(
			"Kafka publication completed out of source order: transaction end LSN %s is behind durable LSN %s",
			result.transaction.EndLSN,
			reader.durable(),
		)
	}
	reader.advance(result.transaction.EndLSN)
	reader.lastLSNAdvanced = time.Now()
	reader.metrics.transactionsAcked.Add(1)
	metrics.TransactionsAcked.Inc()
	metrics.SetLag(time.Since(result.transaction.CommitTime).Seconds(), retentionSeconds())
	log.Printf(
		"trace stage=kafka_transaction_acked txid=%d end_lsn=%s events=%d",
		result.transaction.TransactionID,
		result.transaction.EndLSN,
		result.transaction.Count,
	)
	return nil
}

// publishTransactions consumes events from the reader and produces them to
// Kafka. It replies with one publicationResult per transaction: the real
// descriptor on success, or a failure carrying enough of the transaction for the
// reader to stop cleanly. After a hard failure the publisher exits.
func publishTransactions(
	ctx context.Context,
	client *kgo.Client,
	topic string,
	work <-chan publishWork,
	results chan<- publicationResult,
	rm *readerMetrics,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-work:
			if item.tx != nil {
				select {
				case results <- publicationResult{transaction: item.tx}:
				case <-ctx.Done():
					return
				}
				continue
			}

			event := *item.event
			err := retry.Do(ctx, retry.Default, "publish event to Kafka", func() error {
				err := publishTransactionEvent(ctx, client, topic, event)
				if err != nil {
					log.Printf("warn stage=kafka_publish_retry event_id=%s err=%v", event.ID, err)
				}
				return err
			})
			if err != nil {
				rm.publishFailures.Add(1)
				metrics.Errors.WithLabelValues("kafka_publish").Inc()
				select {
				case results <- publicationResult{transaction: failedTransaction(event), err: err}:
				case <-ctx.Done():
				}
				return
			}
			rm.eventsPublished.Add(1)
			metrics.MessagesProcessed.WithLabelValues(string(event.Operation)).Inc()
			metrics.MessagesPublished.Inc()
			metrics.SetLag(time.Since(event.Source.CommitTime).Seconds(), retentionSeconds())
		}
	}
}

// failedTransaction rebuilds enough of a transaction from a failed event for the
// reader to know which source transaction must be redelivered.
func failedTransaction(event cdc.Event) *source.CommittedTransaction {
	commitLSN, _ := pglogrepl.ParseLSN(event.Source.CommitLSN)
	endLSN, _ := pglogrepl.ParseLSN(event.Source.TransactionEndLSN)
	return &source.CommittedTransaction{
		TransactionID: event.Source.TransactionID,
		CommitLSN:     commitLSN,
		EndLSN:        endLSN,
		CommitTime:    event.Source.CommitTime,
		Count:         event.Source.TransactionEventCount,
	}
}

func timeUntil(deadline time.Time) time.Duration {
	duration := time.Until(deadline)
	if duration < 0 {
		return 0
	}
	return duration
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func logLogicalMessage(message pglogrepl.Message, walStart pglogrepl.LSN) {
	switch message := message.(type) {
	case *pglogrepl.RelationMessage:
		log.Printf(
			"protocol message=Relation wal_start=%s relation_id=%d table=%s.%s replica_identity=%c columns=%d",
			walStart,
			message.RelationID,
			message.Namespace,
			message.RelationName,
			message.ReplicaIdentity,
			len(message.Columns),
		)

	case *pglogrepl.BeginMessage:
		log.Printf(
			"protocol message=Begin wal_start=%s txid=%d",
			walStart,
			message.Xid,
		)

	case *pglogrepl.InsertMessage:
		log.Printf(
			"protocol message=Insert wal_start=%s relation_id=%d tuple_columns=%d",
			walStart,
			message.RelationID,
			tupleColumnCount(message.Tuple),
		)

	case *pglogrepl.UpdateMessage:
		log.Printf(
			"protocol message=Update wal_start=%s relation_id=%d old_tuple=%s new_tuple_columns=%d",
			walStart,
			message.RelationID,
			oldTupleKind(message.OldTupleType),
			tupleColumnCount(message.NewTuple),
		)

	case *pglogrepl.DeleteMessage:
		log.Printf(
			"protocol message=Delete wal_start=%s relation_id=%d old_tuple=%s tuple_columns=%d",
			walStart,
			message.RelationID,
			oldTupleKind(message.OldTupleType),
			tupleColumnCount(message.OldTuple),
		)

	case *pglogrepl.TruncateMessage:
		log.Printf(
			"protocol message=Truncate wal_start=%s relations=%d",
			walStart,
			len(message.RelationIDs),
		)

	case *pglogrepl.CommitMessage:
		log.Printf(
			"protocol message=Commit wal_start=%s commit_lsn=%s end_lsn=%s",
			walStart,
			message.CommitLSN,
			message.TransactionEndLSN,
		)
	}
}

func tupleColumnCount(tuple *pglogrepl.TupleData) int {
	if tuple == nil {
		return 0
	}
	return len(tuple.Columns)
}

func oldTupleKind(tupleType uint8) string {
	switch tupleType {
	case 'K':
		return "key"
	case 'O':
		return "full"
	default:
		return "none"
	}
}

func kafkaProduceBackoff(attempts int) time.Duration {
	return retry.Default.Backoff(attempts + 1)
}

func publishTransactionEvent(ctx context.Context, client *kgo.Client, topic string, event cdc.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", event.ID, err)
	}
	record := &kgo.Record{
		Topic: topic,
		Key:   event.KafkaKey(),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(event.ID)},
			{Key: "operation", Value: []byte(event.Operation)},
		},
	}
	producedRecord, err := client.ProduceSync(ctx, record).First()
	if err != nil {
		return fmt.Errorf("publish event %s to Kafka: %w", event.ID, err)
	}
	log.Printf(
		"trace stage=kafka_record_acked event_id=%s operation=%s source_commit_lsn=%s topic=%s partition=%d offset=%d key=%q",
		event.ID,
		event.Operation,
		event.Source.CommitLSN,
		producedRecord.Topic,
		producedRecord.Partition,
		producedRecord.Offset,
		string(producedRecord.Key),
	)
	return nil
}

func (reader *reader) sendStandby(
	ctx context.Context,
	reason string,
) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, reader.connection, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: reader.durable(),
		WALFlushPosition: reader.durable(),
		WALApplyPosition: reader.durable(),
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("send PostgreSQL standby status at Kafka-durable LSN %s: %w", reader.durable(), err)
	}
	log.Printf(
		"trace stage=postgres_feedback_sent reason=%s kafka_durable_lsn=%s",
		reason,
		reader.durable(),
	)
	return nil
}

func findSlotLSN(ctx context.Context, dsn, slot string) (pglogrepl.LSN, bool, error) {
	connection, err := transport.ConnectPostgres(ctx, dsn)
	if err != nil {
		return 0, false, fmt.Errorf("connect to PostgreSQL SQL endpoint: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()

	var lsnText string
	err = connection.QueryRow(ctx, `
		SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text
		FROM pg_replication_slots
		WHERE slot_name = $1`, slot).Scan(&lsnText)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read replication slot %q: %w", slot, err)
	}

	lsn, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return 0, false, fmt.Errorf("parse acknowledged slot LSN %q: %w", lsnText, err)
	}
	return lsn, true, nil
}

func loadConfig() config {
	return config{
		sqlDSN:         envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"),
		replicationDSN: envOrDefault("SOURCE_REPLICATION_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable&replication=database"),
		slot:           envOrDefault("PG_REPLICATION_SLOT", "live_demo_slot"),
		publication:    envOrDefault("PG_PUBLICATION", "live_demo_pub"),
		kafkaBrokers:   splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		kafkaTopic:     envOrDefault("KAFKA_TOPIC", "live.users"),
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

// retentionSeconds returns the downstream retention budget the source reader
// uses to compute cdc_retention_headroom_seconds. It defaults to the 7-day
// Kafka topic retention the reader enforces.
func retentionSeconds() float64 {
	value := envOrDefault("RETENTION_SECONDS", "604800")
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return seconds
	}
	return 604800
}
