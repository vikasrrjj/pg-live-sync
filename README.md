# PG Live Sync

A deliberately small, real change-data-capture pipeline:

```mermaid
flowchart TB
    source[(Source PostgreSQL)]
    reader[Go source reader]
    kafka[(Kafka topic)]
    writer[Go destination writer]
    destination[(Destination PostgreSQL)]

    source -->|logical WAL / pgoutput| reader
    reader -->|JSON change events| kafka
    kafka -->|consumer records| writer
    writer -->|upsert or delete| destination
    reader -. acknowledge LSN after Kafka .-> source
    writer -. commit offset after SQL commit .-> kafka
```

This is the first vertical slice, not a toy SQL poller. The reader uses a PostgreSQL logical-replication slot, decodes committed `INSERT`, `UPDATE`, and `DELETE` changes, and sends them to Kafka. The writer consumes those records and applies them to a separate PostgreSQL database.

## Run it

You need Docker with the Compose plugin. From this directory:

```bash
docker compose up --build -d
docker compose logs -f source-reader destination-writer
```

Wait until the logs contain both of these messages:

```text
streaming PostgreSQL publication live_demo_pub to Kafka topic live.users
consuming Kafka topic live.users as group live-destination-v1
```

Press `Ctrl+C` to leave the log view; the containers keep running.

## Create source changes

Insert a row:

```bash
docker compose exec source-postgres psql -U postgres -d source -c \
  "INSERT INTO users (name, email) VALUES ('Ada', 'ada@example.com') RETURNING *;"
```

Read the independently stored destination row:

```bash
docker compose exec destination-postgres psql -U postgres -d destination -c \
  "SELECT id, name, email, source_lsn FROM users ORDER BY id;"
```

To see the raw JSON events retained in Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic live.users \
  --from-beginning
```

Press `Ctrl+C` after the events appear.

Now update and delete at the source, checking the destination after each command:

```bash
docker compose exec source-postgres psql -U postgres -d source -c \
  "UPDATE users SET name = 'Ada Lovelace' WHERE email = 'ada@example.com';"

docker compose exec source-postgres psql -U postgres -d source -c \
  "DELETE FROM users WHERE email = 'ada@example.com';"
```

Inspect the event IDs already applied at the destination:

```bash
docker compose exec destination-postgres psql -U postgres -d destination -c \
  "SELECT event_id, source_lsn, applied_at FROM cdc_applied_events ORDER BY applied_at;"
```

## The correctness rules

1. **PostgreSQL is acknowledged only after Kafka acknowledges every event in the source transaction.** If the reader dies earlier, PostgreSQL sends the transaction again. The reader reports an LSN to PostgreSQL that is always at or behind what Kafka confirmed (`kafka_durable_lsn` in the logs).
2. **Kafka is committed only after the destination transaction commits.** If the writer dies earlier, Kafka sends the event again. The `cdc_applied_events` deduplication table makes that replay a no-op.
3. **The destination refuses to be overwritten by older data.** Every write carries the source commit LSN, and an `UPDATE`/`DELETE` only proceeds if its LSN is at least as new as what is stored. This is what lets the initial snapshot hand over to the live stream safely.

That gives **exactly-once on replay**: a crash may cause the same event to be attempted again, but it is recorded once, applied once, and never applied out of order.

## What this version handles

- **Initial snapshot** (#21). The writer waits for the reader's replication slot, copies every published table inside one repeatable-read source transaction and one destination transaction, marks `snapshot_done` in `cdc_state`, and then hands over to the live stream. The slot's restart LSN is stamped on each snapshot row, so live events (always at or after that point) correctly overwrite the copy. If the destination says the snapshot is done but the source slot is gone, the writer refuses to run (corrupted state guard).
- **Huge transactions** (#15). The reader keeps at most `DefaultSpoolCap` (4096) row changes in memory per transaction and spools the rest to a temporary file, then publishes them still in commit order. A 5000-row `INSERT` flows through with bounded memory.
- **Schema evolution with auto-repair** (#17). Publication-level DDL is streamed like any change. When a change references a column the destination does not have, the writer no longer poisons blindly: it reads the missing columns' exact source definitions and adds them to the destination (`ADD COLUMN` with matching type, nullability, and default - and `CREATE TYPE ... AS ENUM` for enum dependencies), then retries the event once. If the drift cannot be repaired (type change of an existing column, a non-enum user type, a NOT NULL column without a default), the event is still quarantined to `cdc_dead_letters` and later events heal automatically once the destination is altered.
- **Primary-key changes** (#19). A key change arrives from pgoutput as a delete + insert pair; the writer applies both in one transaction. At boot the reader refuses published tables with `REPLICA IDENTITY NOTHING`, whose deletes and update keys can never be reconstructed.
- **Unchanged TOAST values** (#7). When a TOASTed column is left untouched by an `UPDATE`, pgoutput may send an "unchanged toast" marker instead of the value. The decoder never guesses: with the default (primary-key) replica identity there is no old image to fill from, so the column is tagged as unchanged and the writer preserves the destination's existing value via a guarded `UPDATE` (no candidate-INSERT that would fabricate NULL for a NOT NULL column). On `REPLICA IDENTITY FULL` tables the full old image is available and the decoder copies it, so rows stay complete. The preserved value is the source's truth, so it is never re-fetched from the source. A key change that would need an unknown unchanged value is refused with a message pointing at `REPLICA IDENTITY FULL`.
- **Poison quarantine** (#16). A change the destination can never accept (missing column, type/cast failure, constraint fault) is written to `cdc_dead_letters` with the reason, marked in `cdc_applied_events`, and never retried. Offsets keep moving; nothing is lost.
- **Kafka outage** (#12). The reader holds the slot at the last Kafka-durable LSN, answers keepalives, and reconnects from that exact LSN when brokers return. The writer waits out poll errors with backoff. The stream silently resumes.
- **PostgreSQL reconnect** (#5). A dropped connection is re-established from the Kafka-durable LSN with backoff.
- **Graceful shutdown** (#23). A reader gets a `SIGTERM` and reports `reason=shutdown` at its Kafka-durable LSN. A writer that is stopped mid-transaction rolls the open destination transaction back and leaves Kafka's offset uncommitted, so the transaction replays cleanly on restart.
- **Fencing and observability** (#24). Only one reader may own a slot (advisory lock; a second reader is refused), topics must have exactly one partition to preserve order, and both sides log periodic metrics (`events_published`, `kafka_durable_lsn`, `applied`, `duplicates`, `dead_letters`, `lag`).
- **Types: arrays, enums, and precision** (#18). Values are typed on the wire: the snapshot reads columns as their own `col::text` representation (the same text format pgoutput emits) and live writes cast every parameter to the destination column's `format_type` via the catalog cache. Arrays (`text[]`), enums, numerics, timestamps, booleans, and `jsonb` round-trip exactly; a new enum on the source is even auto-created on the destination during schema repair.
- **REPLICA IDENTITY FULL** (#26). Tables without a primary key can still be mirrored when `REPLICA IDENTITY FULL` is set: the reader discovers the full old-image columns at boot and uses them as the row key, while tables with no usable identity are refused with a clear message.
- **TRUNCATE** (#27). `TRUNCATE` is decoded as one event per affected relation, guarded by a durable per-table watermark (`cdc_table_watermarks`), and applied only when the event's LSN is newer than the last applied truncate. Stale replays are idempotent no-ops.
- **Apply pipelining** (#28). Each source transaction is applied in one destination transaction using a two-phase `pgx.Batch`: all dedupe markers are inserted first, then mutations are issued only for fresh events. The batch path preserves the exact semantics of the original per-event path while cutting wall-clock latency by roughly 2.7x for a 200-event transaction.
- **Dead-letter replay** (#29). The `cdcctl` CLI lists, replays, and drops quarantined events. Replayed events go through the same `Applier.Apply` path as live events; a permanent failure re-stashes the quarantine marker so restarts never retry the poison. The dead-letter record now stores the full `before` row and `unchanged_columns` for faithful replay.
- **Prometheus metrics** (#30). Both binaries expose `/metrics`, `/healthz`, and `/readyz`. Exported metrics include `cdc_messages_processed_total`, `cdc_lag_seconds`, `cdc_retention_headroom_seconds`, `cdc_dead_letters_total`, `cdc_batch_duration_seconds`, `cdc_batch_size`, and `cdc_ready`.
- **TLS and Kafka SASL/SCRAM** (#31). PostgreSQL connections honor `PGSSLMODE`, `PGSSLROOTCERT`, `PGSSLCERT`, and `PGSSLKEY`. Kafka connections support TLS (`KAFKA_TLS_ENABLED`, `KAFKA_CA_CERT`, `KAFKA_CLIENT_CERT`, `KAFKA_CLIENT_KEY`) and SASL/SCRAM (`KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SASL_MECHANISM`). No secrets live in command-line flags.
- **Retention versus lag alarm** (#22). The writer's metrics line now includes the topic's `retention.ms`, and when the group's committed offset slips behind the retained log's first offset the writer logs a loud `retention_outran_destination` error, because Kafka has already evicted records the destination never consumed.

## What it still does not solve

- Exactly one partition, one reader, one writer, no scale-out. Order is the product's invariant; parallelism comes later.
- Exotic types beyond the covered set (`bytea`, ranges, geometric types, custom domains/composites) have no special handling and may fall to the dead-letter queue.

## Destination table contract

Every destination table mirrored from the source must have:

- a **primary key** matching the source's (required for the guarded upsert and for snapshot conflict detection), and
- three metadata columns added by the pipeline:

```sql
ALTER TABLE users
  ADD COLUMN source_lsn         pg_lsn    NOT NULL DEFAULT '0/0',
  ADD COLUMN source_commit_time timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN replicated_at      timestamptz NOT NULL DEFAULT now();
```

The pipeline also owns four bookkeeping tables (`CREATE TABLE IF NOT EXISTS` on boot): `cdc_applied_events` (every event ever applied, idempotently), `cdc_state` (initial-snapshot completion), `cdc_dead_letters` (poisoned events with their reason, `before` row, and `unchanged_columns`), and `cdc_table_watermarks` (durable truncate LSNs). Do not modify them or the metadata columns by hand.

On a fresh environment the `sql/destination-init.sql` demo table already ships with the metadata columns.

## Configuration and secrets

Both binaries read connection settings from environment variables and expose metrics ports via `METRICS_PORT`. Examples:

| Variable | Default | Purpose |
|---|---|---|
| `DESTINATION_DSN` | `postgres://postgres:postgres@localhost:5434/destination?sslmode=disable` | destination PostgreSQL DSN |
| `SOURCE_SQL_DSN` | `postgres://postgres:postgres@localhost:5433/source?sslmode=disable` | source PostgreSQL SQL DSN |
| `SOURCE_REPLICATION_DSN` | `SOURCE_SQL_DSN` with `&replication=database` appended | source replication protocol DSN |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated Kafka seed brokers |
| `KAFKA_TOPIC` | `live.users` | topic name |
| `KAFKA_CONSUMER_GROUP` | `live-destination-v1` | consumer group for the writer |
| `PG_REPLICATION_SLOT` | `live_demo_slot` | logical replication slot name |
| `PG_PUBLICATION` | `live_demo_pub` | publication name |
| `METRICS_PORT` | `9090` (reader), `9091` (writer) | Prometheus/health port |
| `RETENTION_SECONDS` | `604800` (7 days) | budget used for `cdc_retention_headroom_seconds` |

TLS and SASL are configured via additional environment variables; no secrets belong in command-line flags.

PostgreSQL TLS:

```text
PGSSLMODE=require|verify-ca|verify-full|disable
PGSSLROOTCERT=/run/secrets/pg-root.crt
PGSSLCERT=/run/secrets/pg-client.crt
PGSSLKEY=/run/secrets/pg-client.key
```

Kafka TLS and SASL/SCRAM:

```text
KAFKA_TLS_ENABLED=true
KAFKA_CA_CERT=/run/secrets/kafka-ca.crt
KAFKA_CLIENT_CERT=/run/secrets/kafka-client.crt
KAFKA_CLIENT_KEY=/run/secrets/kafka-client.key
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_SASL_USERNAME=cdc-user
KAFKA_SASL_PASSWORD=...
```

## Dead-letter replay with `cdcctl`

Quarantined events live in `public.cdc_dead_letters`. After fixing the destination schema, replay them:

```bash
# list all dead letters
./cdcctl -dsn "$DESTINATION_DSN" list

# replay one event
./cdcctl -dsn "$DESTINATION_DSN" replay -event 0/16B6A30:1

# replay every dead letter for a table
./cdcctl -dsn "$DESTINATION_DSN" replay -table public.users

# discard an event without applying it
./cdcctl -dsn "$DESTINATION_DSN" drop -event 0/16B6A30:1
```

A replayed event runs through the same apply path as a live event, so dedupe markers and LSN/watermark guards still protect against stale writes.

## Stop or reset

Stop while preserving local demo data:

```bash
docker compose down
```

To completely reset this demo, including both databases, Kafka records, offsets, and the replication slot:

```bash
docker compose down -v
```

The second command deletes this project's local Docker volumes.

## Testing

Two layers of tests, gated so the default `go test ./...` never needs Docker:

```sh
make test                 # unit tests only
make integration          # integration tests against `docker compose up -d`
make integration-verbose  # same, with -v
```

- **Unit tests** (`internal/destination/*_test.go`, `internal/source/decoder_test.go`,
  `cmd/source-reader/main_test.go`)
  pin the SQL the applier produces (typed casts, table-qualified LSN guards,
  placeholder parity, the guarded UPDATE/INSERT preservation path), the row
  transcoder, the poison/undefined-column error classification, the decoder's
  unchanged-TOAST contract (filled from a full old image, preserved as a hint
  without one), the retention/lag helpers, and the slot fencing key.
- **Integration tests** (`*integration_test.go`, behind the
  `integration` build tag and the `PGCDC_INTEGRATION=1` env gate) run against
  the demo's real Kafka and PostgreSQL but write to an isolated `dest_test`
  database, never the demo's `cdc_*` tables:
  - deduplication markers make a replay an idempotent no-op (the crash window
    *after the destination commit, before the Kafka offset commit*),
  - the LSN guard refuses stale out-of-order changes,
  - dead-lettering is atomic with its marker,
  - the initial snapshot round-trips enums, arrays, numeric, jsonb and
    timestamps bit-for-bit and never stamps the empty LSN,
  - column-added drift is repaired (columns + enum created), and existing
    columns are never silently rewritten,
  - over a live replication slot a real 8 KB `SET STORAGE EXTERNAL` TOAST column
    is confirmed to arrive as pgoutput's unchanged marker (default replica
    identity), decode to a preservation hint, and read back untouched at the
    destination while a sibling column updates; stale replays and never-seen
    keys behave honestly,
  - `TRUNCATE` is decoded and applied per-table with watermark guards, and
    multi-table truncates within one source transaction remain atomic,
  - the batch apply path matches the per-event path byte-for-byte on state,
    markers, and watermarks, and a poison event in the middle rolls the whole
    batch back,
  - dead-letter records round-trip `before` and `unchanged_columns`; replays
    re-apply once the schema is fixed, and permanent replay failures keep the
    quarantine marker intact,
  - Prometheus `/metrics`, `/healthz`, and `/readyz` endpoints return the
    expected statuses and metric families,
  - against real Kafka, the two deterministic crash windows end with exactly
    the right rows, markers, and committed consumer-group offset, and the
    offset seam itself advances the group to `record.Offset + 1`.

## Event shape

Kafka values are JSON. A typical insert looks like this:

```json
{
  "version": 1,
  "id": "0/16B6A30:1",
  "schema": "public",
  "table": "users",
  "operation": "insert",
  "key": {"id": "1"},
  "after": {"id": "1", "name": "Ada", "email": "ada@example.com"},
  "source": {
    "transaction_id": 741,
    "commit_lsn": "0/16B6A30",
    "transaction_end_lsn": "0/16B6A60",
    "commit_time": "2026-09-18T12:00:00Z",
    "sequence": 1,
    "transaction_event_count": 1
  }
}
```

The exact transaction ID, LSNs, and time will differ.
