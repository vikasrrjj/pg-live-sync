//go:build integration

package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"example.com/pg-live-sync/internal/cdc"
	"example.com/pg-live-sync/internal/errclass"
	"github.com/jackc/pgx/v5"
)

// batchScenario is the event lifetime of one source table covering every
// behaviour the batch path must reproduce exactly: fresh inserts, an exact group
// replay defeated by dedupe markers, a guarded + preserved-column + older-LSN
// update, a delete, a truncate, and a stale replayed truncate.
func batchScenario(table string) [][]cdc.Event {
	row := func(id string) cdc.Row {
		return cdc.Row{"id": stringPtr(id)}
	}
	after := func(id, name string) cdc.Row {
		return cdc.Row{"id": stringPtr(id), "name": stringPtr(name)}
	}
	event := func(id string, seq, count int, operation cdc.Operation, key, afterRow cdc.Row, lsn string, unchanged ...string) cdc.Event {
		return cdc.Event{
			Version:   cdc.EventVersion,
			ID:        id,
			Schema:    "public",
			Table:     table,
			Operation: operation,
			Key:       key,
			After:     afterRow,
			Source: cdc.SourceMetadata{
				TransactionID:         5000,
				CommitLSN:             lsn,
				TransactionEndLSN:     lsn,
				CommitTime:            time.Now(),
				Sequence:              seq,
				TransactionEventCount: count,
			},
			UnchangedColumns: unchanged,
		}
	}

	return [][]cdc.Event{
		{ // group 1: fresh rows.
			event(table+"-g1-1", 1, 3, cdc.OperationInsert, row("1"), after("1", "one"), "0/1000"),
			event(table+"-g1-2", 2, 3, cdc.OperationInsert, row("2"), after("2", "two"), "0/1000"),
			event(table+"-g1-3", 3, 3, cdc.OperationInsert, row("3"), after("3", "three"), "0/1000"),
		},
		{ // group 2: exact replay of group 1; markers conflict, nothing moves.
			event(table+"-g1-1", 1, 3, cdc.OperationInsert, row("1"), after("1", "one"), "0/1000"),
			event(table+"-g1-2", 2, 3, cdc.OperationInsert, row("2"), after("2", "two"), "0/1000"),
			event(table+"-g1-3", 3, 3, cdc.OperationInsert, row("3"), after("3", "three"), "0/1000"),
		},
		{ // group 3: preserved-column update, guarded update, older-LSN update, delete.
			event(table+"-g3-1", 1, 4, cdc.OperationUpdate, row("1"), after("1", "updated"), "0/2000", "bio"),
			event(table+"-g3-2", 2, 4, cdc.OperationUpdate, row("2"), after("2", "two-v2"), "0/2000"),
			event(table+"-g3-3", 3, 4, cdc.OperationUpdate, row("3"), after("3", "stale"), "0/0500"),
			event(table+"-g3-4", 4, 4, cdc.OperationDelete, row("3"), nil, "0/2000"),
		},
		{ // group 4: the truncate applies and is followed by a fresh insert.
			event(table+"-g4-1", 1, 2, cdc.OperationTruncate, nil, nil, "0/3000"),
			event(table+"-g4-2", 2, 2, cdc.OperationInsert, row("11"), after("11", "eleven"), "0/3000"),
		},
		{ // group 5: an older replayed truncate is stale; the newer insert survives.
			event(table+"-g5-1", 1, 2, cdc.OperationTruncate, nil, nil, "0/2500"),
			event(table+"-g5-2", 2, 2, cdc.OperationInsert, row("12"), after("12", "twelve"), "0/4000"),
		},
	}
}

func tableState(t *testing.T, ctx context.Context, connection *pgx.Conn, table string) []string {
	t.Helper()
	rows, err := connection.Query(ctx, fmt.Sprintf(
		`SELECT id, name, source_lsn::text FROM public.%s ORDER BY id`, table))
	if err != nil {
		t.Fatalf("query %s state: %v", table, err)
	}
	defer rows.Close()
	var state []string
	for rows.Next() {
		var id, name, lsn string
		if err := rows.Scan(&id, &name, &lsn); err != nil {
			t.Fatalf("scan %s state: %v", table, err)
		}
		state = append(state, fmt.Sprintf("%s|%s|%s", id, name, lsn))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s state: %v", table, err)
	}
	return state
}

// TestApplyBatchMatchesSequentialSemantics runs the exact same event history
// through the sequential per-event path and through ApplyBatch and proves the
// destination state, markers, and truncate watermark end up identical: the
// batch changes the transport, never the outcome.
func TestApplyBatchMatchesSequentialSemantics(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itbatcha")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id TEXT PRIMARY KEY,
			name TEXT,
			bio TEXT,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })
	groups := batchScenario(table)

	// Baseline: sequential per-event apply of every group, one destination
	// transaction per source transaction.
	for _, group := range groups {
		transaction, err := connection.Begin(ctx)
		if err != nil {
			t.Fatalf("begin sequential group: %v", err)
		}
		for _, event := range group {
			if _, err := applier.ApplyInTx(ctx, transaction, event); err != nil {
				t.Fatalf("sequential apply %s: %v", event.ID, err)
			}
		}
		if err := transaction.Commit(ctx); err != nil {
			t.Fatalf("commit sequential group: %v", err)
		}
	}
	expectedState := tableState(t, ctx, connection, table)
	expectedMarkers := markerIDSet(t, ctx, connection, groups)

	// Reset the destination rows, markers, and truncate watermark so the batch
	// run starts from the same empty base as the sequential run.
	if _, err := connection.Exec(ctx, fmt.Sprintf(`TRUNCATE public.%s RESTART IDENTITY CASCADE`, table)); err != nil {
		t.Fatalf("reset rows: %v", err)
	}
	rewindMarkers(t, ctx, connection, groups)
	if _, err := connection.Exec(ctx,
		`DELETE FROM public.cdc_table_watermarks WHERE table_name = $1`, quoteIdent("public")+"."+quoteIdent(table)); err != nil {
		t.Fatalf("reset watermark: %v", err)
	}

	// Batched: same groups through ApplyBatch, still one tx per source tx.
	for _, group := range groups {
		transaction, err := connection.Begin(ctx)
		if err != nil {
			t.Fatalf("begin batch group: %v", err)
		}
		if err := applier.ApplyBatch(ctx, transaction, group); err != nil {
			t.Fatalf("batch apply group: %v", err)
		}
		if err := transaction.Commit(ctx); err != nil {
			t.Fatalf("commit batch group: %v", err)
		}
	}

	a, b := tableState(t, ctx, connection, table), expectedState
	if len(a) != len(b) {
		t.Fatalf("batch state row count %d != sequential %d: %v vs %v", len(a), len(b), a, b)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("row %d differs: batched %q, sequential %q\nall batched=%v\nall sequential=%v", i, a[i], b[i], a, b)
		}
	}
	if got := markerIDSet(t, ctx, connection, groups); !equalStringSet(got, expectedMarkers) {
		t.Fatalf("marker sets differ: sequential=%v batched=%v", expectedMarkers, got)
	}
	if got := watermarkFor(t, ctx, connection, table); got != "0/3000" {
		t.Fatalf("watermark after batch truncate = %s, want 0/3000 (stale 0/2500 must not move it)", got)
	}
	// Only the inserted-after-truncate rows survive: ids 11 and 12.
	if len(a) != 2 || a[0][0:2] != "11" || a[1][0:2] != "12" {
		t.Fatalf("batch end state = %v, want exactly 11 and 12", a)
	}
}

// A poison event mid-batch surfaces as a permanent *BatchError naming exactly
// the poisoned event; nothing before it half-applies, so a rollback followed by
// individual replay (the writer's poison path) recovers every healthy event and
// quarantines the poison.
func TestApplyBatchPoisonFailsLoudAndRollsBackEverything(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itbatchpoison")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id TEXT PRIMARY KEY,
			age BIGINT NOT NULL,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	mkEvent := func(id string, seq int, age string, lsn string) cdc.Event {
		return cdc.Event{
			Version: cdc.EventVersion, ID: id, Schema: "public", Table: table,
			Operation: cdc.OperationInsert, Key: cdc.Row{"id": stringPtr(id)},
			After: cdc.Row{"id": stringPtr(id), "age": stringPtr(age)},
			Source: cdc.SourceMetadata{TransactionID: 6000, CommitLSN: lsn, TransactionEndLSN: lsn,
				CommitTime: time.Now(), Sequence: seq, TransactionEventCount: 3},
		}
	}
	poison := mkEvent(table+"-poison", 2, "not-a-bigint", "0/6000")

	transaction, err := connection.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	err = applier.ApplyBatch(ctx, transaction, []cdc.Event{
		mkEvent(table+"-healthy-1", 1, "10", "0/6000"),
		poison,
		mkEvent(table+"-healthy-3", 3, "30", "0/6000"),
	})
	var batchErr *BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("ApplyBatch must return a *BatchError, got %T: %v", err, err)
	}
	if batchErr.Event.ID != poison.ID {
		t.Fatalf("poison event reported = %s, want %s", batchErr.Event.ID, poison.ID)
	}
	if !errclass.IsPermanent(batchErr) {
		t.Fatalf("poison must classify as permanent: %v", err)
	}
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	var count int
	if err := connection.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s`, table)).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("poisoned batch half-applied %d rows, want 0", count)
	}
	var markers int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_applied_events WHERE event_id LIKE $1`, table+"%").Scan(&markers); err != nil {
		t.Fatalf("count markers: %v", err)
	}
	if markers != 0 {
		t.Fatalf("poisoned batch leaked %d markers, want 0", markers)
	}

	// Healthy events recover individually; the poison stays poisoned and lands
	// in the dead-letter table.
	for _, event := range []cdc.Event{
		mkEvent(table+"-healthy-1", 1, "10", "0/6000"),
		mkEvent(table+"-healthy-3", 3, "30", "0/6000"),
	} {
		if applied, applyErr := applier.Apply(ctx, event); applyErr != nil || !applied {
			t.Fatalf("replay healthy %s: applied=%v err=%v", event.ID, applied, applyErr)
		}
	}
	if _, err := applier.Apply(ctx, poison); err == nil {
		t.Fatal("replay of the poison succeeded, want a permanent error")
	} else if !errclass.IsPermanent(err) {
		t.Fatalf("replay of poison must stay permanent: %v", err)
	}
	if err := applier.RecordDeadLetter(ctx, poison, "poison replay must dead letter"); err != nil {
		t.Fatalf("dead letter poison: %v", err)
	}
	var dead int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_dead_letters WHERE event_id = $1`, poison.ID).Scan(&dead); err != nil {
		t.Fatalf("count dead letters: %v", err)
	}
	if dead != 1 {
		t.Fatalf("want 1 dead letter for the poison, got %d", dead)
	}
}

func markerIDSet(t *testing.T, ctx context.Context, connection *pgx.Conn, groups [][]cdc.Event) map[string]bool {
	t.Helper()
	set := make(map[string]bool)
	for _, group := range groups {
		for _, event := range group {
			var present int
			if err := connection.QueryRow(ctx,
				`SELECT count(*) FROM public.cdc_applied_events WHERE event_id = $1`, event.ID).Scan(&present); err != nil {
				t.Fatalf("marker %s: %v", event.ID, err)
			}
			if present > 0 {
				set[event.ID] = true
			}
		}
	}
	return set
}

func rewindMarkers(t *testing.T, ctx context.Context, connection *pgx.Conn, groups [][]cdc.Event) {
	t.Helper()
	for _, group := range groups {
		for _, event := range group {
			if _, err := connection.Exec(ctx,
				`DELETE FROM public.cdc_applied_events WHERE event_id = $1`, event.ID); err != nil {
				t.Fatalf("rewind marker %s: %v", event.ID, err)
			}
		}
	}
}

func equalStringSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for key := range a {
		if !b[key] {
			return false
		}
	}
	return true
}

func stringPtr(value string) *string { return &value }

// integrationGateForBenchmark skips a benchmark unless the integration stack is
// up, mirroring integrationGate for *testing.T.
func integrationGateForBenchmark(b *testing.B) {
	b.Helper()
	if os.Getenv("PGCDC_INTEGRATION") != "1" {
		b.Skip("set PGCDC_INTEGRATION=1 to run the docker integration benchmarks (make integration)")
	}
}

// Benchmark helpers: the destination helpers are typed on *testing.T, so the
// benchmarks use small equivalents pointed at the same dest_test database.
func destinationDSNForBenchmark() string {
	if dsn := os.Getenv("DESTINATION_DSN_INT"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5434/dest_test?sslmode=disable"
}

func openDestTestForBenchmark(b *testing.B) (*pgx.Conn, *Applier) {
	b.Helper()
	ctx := context.Background()
	ensureDestTestDatabaseForBenchmark(b)
	connection, err := pgx.Connect(ctx, destinationDSNForBenchmark())
	if err != nil {
		b.Fatalf("connect to dest_test: %v", err)
	}
	b.Cleanup(func() { _ = connection.Close(context.Background()) })
	applier := NewApplier(connection)
	if err := applier.EnsureMetaSchema(ctx); err != nil {
		b.Fatalf("ensure meta schema: %v", err)
	}
	return connection, applier
}

func ensureDestTestDatabaseForBenchmark(b *testing.B) {
	b.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5434/postgres?sslmode=disable")
	if err != nil {
		b.Fatalf("connect to destination postgres: %v", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dest_test')`).Scan(&exists); err != nil {
		b.Fatalf("check dest_test: %v", err)
	}
	if !exists {
		if _, err := admin.Exec(ctx, `CREATE DATABASE dest_test`); err != nil {
			b.Fatalf("create dest_test: %v", err)
		}
	}
}

func uniqueTableForBenchmark(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()&0x7fffffffffffffff)
}

// BenchmarkApplyBatchProbe reports the per-source-transaction cost of the
// batched path against the live destination (PGCDC_INTEGRATION=1).
func BenchmarkApplyBatchProbe(b *testing.B) {
	integrationGateForBenchmark(b)
	ctx := context.Background()
	ensureDestTestDatabaseForBenchmark(b)
	connection, applier := openDestTestForBenchmark(b)
	table := uniqueTableForBenchmark("itbenchb")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id BIGINT PRIMARY KEY,
			payload TEXT,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		b.Fatalf("create table: %v", err)
	}
	b.Cleanup(func() { _, _ = connection.Exec(context.Background(), "DROP TABLE IF EXISTS public."+table+" CASCADE") })

	const eventsPerGroup = 200
	b.ResetTimer()
	for cycle := 0; cycle < b.N; cycle++ {
		group := make([]cdc.Event, 0, eventsPerGroup)
		for i := 0; i < eventsPerGroup; i++ {
			eventID := fmt.Sprintf("%s-b-%d-%d", table, cycle, i)
			id := fmt.Sprintf("%d", i)
			payload := fmt.Sprintf("row-%d", i)
			group = append(group, cdc.Event{
				Version: cdc.EventVersion, ID: eventID, Schema: "public", Table: table,
				Operation: cdc.OperationInsert,
				Key:       cdc.Row{"id": &id},
				After:     cdc.Row{"id": &id, "payload": &payload},
				Source: cdc.SourceMetadata{TransactionID: uint32(cycle), CommitLSN: "0/1FFFFFF",
					TransactionEndLSN: "0/1FFFFFF", CommitTime: time.Now(),
					Sequence: i + 1, TransactionEventCount: eventsPerGroup},
			})
		}
		transaction, err := connection.Begin(ctx)
		if err != nil {
			b.Fatalf("begin: %v", err)
		}
		if err := applier.ApplyBatch(ctx, transaction, group); err != nil {
			b.Fatalf("batch apply: %v", err)
		}
		if err := transaction.Commit(ctx); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

// BenchmarkApplyInTxProbe is the pre-batching baseline: per-event statements
// inside a single destination transaction (PGCDC_INTEGRATION=1).
func BenchmarkApplyInTxProbe(b *testing.B) {
	integrationGateForBenchmark(b)
	ctx := context.Background()
	ensureDestTestDatabaseForBenchmark(b)
	connection, applier := openDestTestForBenchmark(b)
	table := uniqueTableForBenchmark("itbenchi")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id BIGINT PRIMARY KEY,
			payload TEXT,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		b.Fatalf("create table: %v", err)
	}
	b.Cleanup(func() { _, _ = connection.Exec(context.Background(), "DROP TABLE IF EXISTS public."+table+" CASCADE") })

	const eventsPerGroup = 200
	b.ResetTimer()
	for cycle := 0; cycle < b.N; cycle++ {
		transaction, err := connection.Begin(ctx)
		if err != nil {
			b.Fatalf("begin: %v", err)
		}
		for i := 0; i < eventsPerGroup; i++ {
			eventID := fmt.Sprintf("%s-inline-%d-%d", table, cycle, i)
			id := fmt.Sprintf("%d", i)
			payload := fmt.Sprintf("row-%d", i)
			applied, err := applier.ApplyInTx(ctx, transaction, cdc.Event{
				Version: cdc.EventVersion, ID: eventID, Schema: "public", Table: table,
				Operation: cdc.OperationInsert,
				Key:       cdc.Row{"id": &id},
				After:     cdc.Row{"id": &id, "payload": &payload},
				Source: cdc.SourceMetadata{TransactionID: uint32(cycle), CommitLSN: "0/1FFFFFF",
					TransactionEndLSN: "0/1FFFFFF", CommitTime: time.Now(),
					Sequence: i + 1, TransactionEventCount: eventsPerGroup},
			})
			if err != nil || !applied {
				b.Fatalf("inline apply: applied=%v err=%v", applied, err)
			}
		}
		if err := transaction.Commit(ctx); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}
