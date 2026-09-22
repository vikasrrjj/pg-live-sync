//go:build integration

package destination

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"example.com/artie-mini-cdc/internal/cdc"
	"github.com/jackc/pgx/v5"
)

const metaColumnsDDL = "source_lsn PG_LSN NOT NULL DEFAULT '0/0', source_commit_time TIMESTAMPTZ NOT NULL DEFAULT now(), replicated_at TIMESTAMPTZ NOT NULL DEFAULT now()"

// Integration tests run in the same dest_test database as the destination-writer
// package's tests (go test executes the two package test binaries in parallel),
// so every table gets a unique name instead of a shared well-known one.
var integrationTableSequence uint64

func uniqueTable(prefix string) string {
	sequence := atomic.AddUint64(&integrationTableSequence, 1)
	return fmt.Sprintf("%s_%03d_%x", prefix, sequence, time.Now().UnixNano()&0xffffff)
}

func integrationGate(t *testing.T) {
	t.Helper()
	if os.Getenv("ARTIE_INTEGRATION") != "1" {
		t.Skip("set ARTIE_INTEGRATION=1 to run the docker integration tests (make integration)")
	}
}

func sourceIntegrationDSN() string {
	if dsn := os.Getenv("SOURCE_SQL_DSN_INT"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"
}

func destinationIntegrationDSN(database string) string {
	if dsn := os.Getenv("DESTINATION_DSN_INT"); dsn != "" {
		return dsn
	}
	return fmt.Sprintf("postgres://postgres:postgres@localhost:5434/%s?sslmode=disable", database)
}

// ensureDestTestDatabase creates the isolated dest_test database used by every
// integration test, so the demo's cdc_applied_events / cdc_state tables are never
// touched.
func ensureDestTestDatabase(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, destinationIntegrationDSN("postgres"))
	if err != nil {
		t.Fatalf("connect to destination postgres database: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

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
	return admin
}

func openDestTest(t *testing.T) (*pgx.Conn, *Applier) {
	t.Helper()
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, destinationIntegrationDSN("dest_test"))
	if err != nil {
		t.Fatalf("connect to dest_test: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	applier := NewApplier(connection)
	if err := applier.EnsureMetaSchema(ctx); err != nil {
		t.Fatalf("ensure meta schema in dest_test: %v", err)
	}
	return connection, applier
}

func str(value string) *string {
	return &value
}

// eventWith builds a valid pipeline event whose payload drives exactly the rows
// the test expects. commitTime is kept current so the pg_lsn and clock guards
// behave like the live stream.
func eventWith(id, table string, txid uint32, sequence, count int, key, after cdc.Row, lsn string) cdc.Event {
	return cdc.Event{
		Version:   cdc.EventVersion,
		ID:        id,
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationInsert,
		Key:       key,
		After:     after,
		Source: cdc.SourceMetadata{
			TransactionID:         txid,
			CommitLSN:             lsn,
			TransactionEndLSN:     lsn,
			CommitTime:            time.Now(),
			Sequence:              sequence,
			TransactionEventCount: count,
		},
	}
}

func dropDestTables(t *testing.T, connection *pgx.Conn, tables ...string) {
	t.Helper()
	ctx := context.Background()
	for _, table := range tables {
		if _, err := connection.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS public.%s CASCADE`, table)); err != nil {
			t.Fatalf("drop destination table %s: %v", table, err)
		}
	}
}

func clearMetadata(t *testing.T, connection *pgx.Conn, eventIDs []string) {
	t.Helper()
	ctx := context.Background()
	if len(eventIDs) == 0 {
		return
	}
	if _, err := connection.Exec(ctx,
		`DELETE FROM public.cdc_applied_events WHERE event_id = ANY($1::text[])`, eventIDs); err != nil {
		t.Fatalf("clear applied markers: %v", err)
	}
	if _, err := connection.Exec(ctx,
		`DELETE FROM public.cdc_dead_letters WHERE event_id = ANY($1::text[])`, eventIDs); err != nil {
		t.Fatalf("clear dead letters: %v", err)
	}
}

// The crash window "after the destination transaction committed, before the Kafka
// offset was committed". The whole-source-transaction effect exists on the
// destination; replaying the same Kafka records must be an idempotent no-op.
func TestApplyInTxCrashWindowReplay(t *testing.T) {
	integrationGate(t)
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)

	const eventIDOne = "it-crash-0000000000000001"
	const eventIDTwo = "it-crash-0000000000000002"
	table := uniqueTable("it_crash")
	t.Cleanup(func() { clearMetadata(t, connection, []string{eventIDOne, eventIDTwo}) })

	ctx := context.Background()
	dropDestTables(t, connection, table)
	t.Cleanup(func() { dropDestTables(t, connection, table) })
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (id integer PRIMARY KEY, name text NOT NULL, %s)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	first := eventWith(eventIDOne, table, 777, 1, 2,
		cdc.Row{"id": str("42")}, cdc.Row{"id": str("42"), "name": str("alice")}, "0/1F00000")
	second := eventWith(eventIDTwo, table, 777, 2, 2,
		cdc.Row{"id": str("43")}, cdc.Row{"id": str("43"), "name": str("bob")}, "0/1F00000")

	// First pass draws the group transaction exactly like the writer does, then
	// the destination side is committed without touching the Kafka offset.
	transaction, err := connection.Begin(ctx)
	if err != nil {
		t.Fatalf("begin group transaction: %v", err)
	}
	for _, event := range []cdc.Event{first, second} {
		applied, err := applier.ApplyInTx(ctx, transaction, event)
		if err != nil {
			t.Fatalf("apply %s in group: %v", event.ID, err)
		}
		if !applied {
			t.Fatalf("first application of %s must apply", event.ID)
		}
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatalf("commit group transaction: %v", err)
	}

	var preReplayCount int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.`+table).Scan(&preReplayCount); err != nil {
		t.Fatalf("count rows after commit: %v", err)
	}
	if preReplayCount != 2 {
		t.Fatalf("want 2 rows after the committed crash window, got %d", preReplayCount)
	}

	// The crash window: the same records arrive again (the consumer group offset
	// was never advanced). Every marker conflicts, so every replay is a no-op.
	for _, event := range []cdc.Event{first, second} {
		applied, err := applier.Apply(ctx, event)
		if err != nil {
			t.Fatalf("replay %s: %v", event.ID, err)
		}
		if applied {
			t.Fatalf("replay of %s must not re-apply", event.ID)
		}
	}

	var rowCount int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.`+table).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 2 {
		t.Fatalf("want exactly 2 rows after replay, got %d", rowCount)
	}

	var markerCount int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_applied_events WHERE event_id = ANY($1::text[])`,
		[]string{eventIDOne, eventIDTwo}).Scan(&markerCount); err != nil {
		t.Fatalf("count markers: %v", err)
	}
	if markerCount != 2 {
		t.Fatalf("want exactly 2 markers, got %d", markerCount)
	}
}

// A TOAST-unchanged column from an UPDATE is preserved on the destination: the
// decoder marked the column as unchanged (absent from After), so the upsert must
// leave the destination's value untouched while still applying the real changes.
func TestUnchangedColumnsPreserveDestinationValue(t *testing.T) {
	integrationGate(t)
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	ctx := context.Background()

	table := uniqueTable("it_toast")
	dropDestTables(t, connection, table)
	t.Cleanup(func() { dropDestTables(t, connection, table) })
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (id integer PRIMARY KEY, name text NOT NULL, bio text NOT NULL, %s)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	bigBio := strings.Repeat("x", 12_000)
	runID := fmt.Sprintf("it-toast-%d", time.Now().UnixNano())
	insertID := runID + "-insert"
	applied, err := applier.Apply(ctx, cdc.Event{
		Version:   cdc.EventVersion,
		ID:        insertID,
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationInsert,
		Key:       cdc.Row{"id": str("1")},
		After:     cdc.Row{"id": str("1"), "name": str("original"), "bio": str(bigBio)},
		Source: cdc.SourceMetadata{
			TransactionID:         950,
			CommitLSN:             "0/2400000",
			TransactionEndLSN:     "0/2400000",
			CommitTime:            time.Now(),
			Sequence:              1,
			TransactionEventCount: 1,
		},
	})
	if err != nil || !applied {
		t.Fatalf("seed insert: applied=%v err=%v", applied, err)
	}

	// The TOASTed bio is unchanged: it is missing from After and listed in
	// UnchangedColumns. The name update must land, the bio must stay exact.
	applied, err = applier.Apply(ctx, cdc.Event{
		Version:   cdc.EventVersion,
		ID:        runID + "-update",
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationUpdate,
		Key:       cdc.Row{"id": str("1")},
		After:     cdc.Row{"id": str("1"), "name": str("renamed")},
		Source: cdc.SourceMetadata{
			TransactionID:         951,
			CommitLSN:             "0/2400100",
			TransactionEndLSN:     "0/2400100",
			CommitTime:            time.Now(),
			Sequence:              1,
			TransactionEventCount: 1,
		},
		UnchangedColumns: []string{"bio"},
	})
	if err != nil || !applied {
		t.Fatalf("update with unchanged column: applied=%v err=%v", applied, err)
	}

	var name string
	var bio string
	if err := connection.QueryRow(ctx,
		`SELECT name, bio FROM public.`+table+` WHERE id = 1`).Scan(&name, &bio); err != nil {
		t.Fatalf("read toasted row: %v", err)
	}
	if name != "renamed" {
		t.Fatalf("changed column must update, got %q", name)
	}
	if bio != bigBio {
		t.Fatalf("unchanged toast column was overwritten: len(bio)=%d want %d", len(bio), len(bigBio))
	}

	// An unchanged primary-key column is a contract violation and must be refused.
	_, err = applier.Apply(ctx, cdc.Event{
		Version:   cdc.EventVersion,
		ID:        runID + "-badkey",
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationUpdate,
		Key:       cdc.Row{"id": str("1")},
		After:     cdc.Row{"name": str("x")},
		Source: cdc.SourceMetadata{
			TransactionID:         952,
			CommitLSN:             "0/2400200",
			TransactionEndLSN:     "0/2400200",
			CommitTime:            time.Now(),
			Sequence:              1,
			TransactionEventCount: 1,
		},
		UnchangedColumns: []string{"id"},
	})
	if err == nil {
		t.Fatal("unchanged primary-key column must be rejected")
	}
	var markerCount int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_applied_events WHERE event_id = '`+runID+`-badkey'`).Scan(&markerCount); err != nil {
		t.Fatalf("count bad marker: %v", err)
	}
	if markerCount != 0 {
		t.Fatalf("rejected event must not leave a marker, found %d", markerCount)
	}

	// Stale replay: the row already holds 0/2400100; an older event (0/2400050)
	// with an unchanged toast column must be a clean no-op that preserves the
	// row exactly, not a candidate-INSERT collision against NOT NULL bio.
	applied, err = applier.Apply(ctx, cdc.Event{
		Version:   cdc.EventVersion,
		ID:        runID + "-stale",
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationUpdate,
		Key:       cdc.Row{"id": str("1")},
		After:     cdc.Row{"id": str("1"), "name": str("stale-name")},
		Source: cdc.SourceMetadata{
			TransactionID:         953,
			CommitLSN:             "0/2400050",
			TransactionEndLSN:     "0/2400050",
			CommitTime:            time.Now(),
			Sequence:              1,
			TransactionEventCount: 1,
		},
		UnchangedColumns: []string{"bio"},
	})
	if err != nil {
		t.Fatalf("stale replay with unchanged column: %v", err)
	}
	var lsn string
	if err := connection.QueryRow(ctx,
		`SELECT name, bio, source_lsn FROM public.`+table+` WHERE id = 1`).Scan(&name, &bio, &lsn); err != nil {
		t.Fatalf("read row after stale replay: %v", err)
	}
	if name != "renamed" || bio != bigBio || lsn != "0/2400100" {
		t.Fatalf("stale replay changed the row: name=%q bio_len=%d lsn=%q", name, len(bio), lsn)
	}

	// A key the destination has NEVER seen with an unchanged NOT NULL column is
	// unknowable: the honest outcome is a poisonable error, not a NULL write.
	_, err = applier.Apply(ctx, cdc.Event{
		Version:   cdc.EventVersion,
		ID:        runID + "-gone",
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationUpdate,
		Key:       cdc.Row{"id": str("77")},
		After:     cdc.Row{"id": str("77"), "name": str("newcomer")},
		Source: cdc.SourceMetadata{
			TransactionID:         954,
			CommitLSN:             "0/2400300",
			TransactionEndLSN:     "0/2400300",
			CommitTime:            time.Now(),
			Sequence:              1,
			TransactionEventCount: 1,
		},
		UnchangedColumns: []string{"bio"},
	})
	if err == nil {
		t.Fatal("update of a never-seen key with unchanged NOT NULL column must fail honestly")
	}
}

// The LSN guard is the defence against out-of-order replay: an event older than
// the row it would overwrite is recorded as applied (so its marker stops a later
// retry) but must not change the row.
func TestLSNGuardRejectsStaleChanges(t *testing.T) {
	integrationGate(t)
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	ctx := context.Background()

	table := uniqueTable("it_guard")
	dropDestTables(t, connection, table)
	t.Cleanup(func() { dropDestTables(t, connection, table) })
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (id integer PRIMARY KEY, name text NOT NULL, %s)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	insert := func(id, name, lsn string) (bool, error) {
		return applier.Apply(ctx, cdc.Event{
			Version:   cdc.EventVersion,
			ID:        fmt.Sprintf("it-guard-%s", lsn),
			Schema:    "public",
			Table:     table,
			Operation: cdc.OperationInsert,
			Key:       cdc.Row{"id": str(id)},
			After:     cdc.Row{"id": str(id), "name": str(name)},
			Source: cdc.SourceMetadata{
				TransactionID:         900,
				CommitLSN:             lsn,
				TransactionEndLSN:     lsn,
				CommitTime:            time.Now(),
				Sequence:              1,
				TransactionEventCount: 1,
			},
		})
	}

	if applied, err := insert("1", "first", "0/100"); err != nil || !applied {
		t.Fatalf("seed insert: applied=%v err=%v", applied, err)
	}
	if applied, err := insert("1", "second", "0/200"); err != nil || !applied {
		t.Fatalf("newer update: applied=%v err=%v", applied, err)
	}

	// A stale update must not overwrite the row held at LSN 0/200.
	if applied, err := insert("1", "stale", "0/150"); err != nil || !applied {
		t.Fatalf("stale update marker: applied=%v err=%v", applied, err)
	}

	var name string
	var lsn string
	if err := connection.QueryRow(ctx,
		`SELECT name, source_lsn::text FROM public.`+table+` WHERE id = 1`).Scan(&name, &lsn); err != nil {
		t.Fatalf("read guarded row: %v", err)
	}
	if name != "second" || lsn != "0/200" {
		t.Fatalf("stale update overwrote the row: name=%q lsn=%s", name, lsn)
	}

	if _, err := connection.Exec(ctx,
		`DELETE FROM public.cdc_applied_events WHERE event_id LIKE 'it-guard-%'`); err != nil {
		t.Fatalf("cleanup guard markers: %v", err)
	}
}

// Quarantining a poison event records the marker and the dead letter atomically,
// and the marker alone makes every later application a no-op.
func TestRecordDeadLetterAtomicity(t *testing.T) {
	integrationGate(t)
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	ctx := context.Background()

	const eventID = "it-dlq-0000000000000001"
	table := uniqueTable("it_dlq")
	t.Cleanup(func() { clearMetadata(t, connection, []string{eventID}) })
	t.Cleanup(func() { dropDestTables(t, connection, table) })
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (id integer PRIMARY KEY, name text NOT NULL, %s)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	event := eventWith(eventID, table, 800, 1, 1,
		cdc.Row{"id": str("1")}, cdc.Row{"id": str("1"), "name": str("poison")}, "0/2100000")
	if err := applier.RecordDeadLetter(ctx, event, "deliberate integration poison"); err != nil {
		t.Fatalf("record dead letter: %v", err)
	}

	var rowCount int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.`+table).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 0 {
		t.Fatalf("poisoned event must not create a row, found %d", rowCount)
	}

	var reason string
	err := connection.QueryRow(ctx,
		`SELECT reason FROM public.cdc_dead_letters WHERE event_id = $1`, eventID).Scan(&reason)
	if err != nil {
		t.Fatalf("read dead letter: %v", err)
	}
	if reason != "deliberate integration poison" {
		t.Fatalf("unexpected dead-letter reason %q", reason)
	}

	applied, err := applier.Apply(ctx, event)
	if err != nil {
		t.Fatalf("post-quarantine apply: %v", err)
	}
	if applied {
		t.Fatal("quarantined event must never apply")
	}
}
