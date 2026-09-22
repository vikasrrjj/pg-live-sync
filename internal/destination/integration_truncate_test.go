//go:build integration

package destination

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"example.com/artie-mini-cdc/internal/cdc"
	"example.com/artie-mini-cdc/internal/errclass"
	"github.com/jackc/pgx/v5"
)

func truncateEvent(id, table string, txid uint32, sequence, count int, lsn string) cdc.Event {
	return cdc.Event{
		Version:   cdc.EventVersion,
		ID:        id,
		Schema:    "public",
		Table:     table,
		Operation: cdc.OperationTruncate,
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

func seedRows(t *testing.T, ctx context.Context, connection *pgx.Conn, table string, ids ...int64) {
	t.Helper()
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, name, source_lsn) SELECT g, 'row-' || g, '0/999' FROM generate_series(1, $1) AS g`, table),
		len(ids)); err != nil {
		t.Fatalf("seed %s rows: %v", table, err)
	}
}

func countRows(t *testing.T, ctx context.Context, connection *pgx.Conn, table string) int {
	t.Helper()
	var count int
	if err := connection.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM public.%s`, table)).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func watermarkFor(t *testing.T, ctx context.Context, connection *pgx.Conn, table string) string {
	t.Helper()
	var watermark string
	tableName := quoteIdent("public") + "." + quoteIdent(table)
	err := connection.QueryRow(ctx,
		`SELECT watermark_lsn::text FROM public.cdc_table_watermarks WHERE table_name = $1`,
		tableName).Scan(&watermark)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatalf("read watermark for %s: %v", table, err)
	}
	return watermark
}

// The truncate contract: guards, durability, stale replay, and newer inserts.
// A truncate clears the table and advances a per-table watermark atomically with
// its applied-event marker; replayed or older truncates are safe no-ops that can
// never destroy rows written after the watermark.
func TestTruncateApplyGuardStaleReplayAndNewerInserts(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("ittrunc")

	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create truncate table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })
	seedRows(t, ctx, connection, table, 1, 2)

	// First truncate: applies, clears everything, watermark advances.
	first := truncateEvent(table+"-trunc-first", table, 901, 1, 1, "0/5000")
	applied, err := applier.Apply(ctx, first)
	if err != nil {
		t.Fatalf("apply first truncate: %v", err)
	}
	if !applied {
		t.Fatal("first truncate reported not applied")
	}
	if countRows(t, ctx, connection, table) != 0 {
		t.Fatal("first truncate did not clear the table")
	}
	if got := watermarkFor(t, ctx, connection, table); got != "0/5000" {
		t.Fatalf("watermark after first truncate = %s, want 0/5000", got)
	}

	// Newer inserts, then the same (replayed) truncate must not destroy them.
	insertID := table + "-trunc-newer-row"
	applied, err = applier.Apply(ctx, eventWith(insertID, table, 902, 1, 1,
		cdc.Row{"id": str("9")}, cdc.Row{"id": str("9"), "name": str("newer")}, "0/6000"))
	if err != nil {
		t.Fatalf("apply newer insert: %v", err)
	}
	if !applied {
		t.Fatal("insert after truncate reported not applied")
	}
	if countRows(t, ctx, connection, table) != 1 {
		t.Fatalf("expected 1 newer row after truncate, got %d", countRows(t, ctx, connection, table))
	}
	if applied, err = applier.Apply(ctx, first); err != nil {
		t.Fatalf("replay first truncate: %v", err)
	} else if applied {
		t.Fatal("replayed truncate reported applied; replay must be a no-op")
	}
	if countRows(t, ctx, connection, table) != 1 {
		t.Fatal("replayed truncate destroyed a newer insert")
	}

	// An older stray truncate, even one never seen before, is stale vs the
	// watermark and must not clear the table.
	stale := truncateEvent(table+"-trunc-stale", table, 903, 1, 1, "0/4000")
	if applied, err = applier.Apply(ctx, stale); err != nil {
		t.Fatalf("apply stale truncate: %v", err)
	} else if applied {
		t.Fatal("truncate older than the watermark reported applied")
	}
	if countRows(t, ctx, connection, table) != 1 {
		t.Fatal("stale truncate destroyed the newer insert")
	}

	// A later truncate still wins, and its own replay is a duplicate no-op.
	later := truncateEvent(table+"-trunc-later", table, 904, 1, 1, "0/7000")
	if applied, err = applier.Apply(ctx, later); err != nil {
		t.Fatalf("apply later truncate: %v", err)
	} else if !applied {
		t.Fatal("later truncate reported not applied")
	}
	if countRows(t, ctx, connection, table) != 0 {
		t.Fatal("later truncate did not clear the table")
	}
	if applied, err = applier.Apply(ctx, later); err != nil {
		t.Fatalf("replay later truncate: %v", err)
	} else if applied {
		t.Fatal("replay of the latest truncate reported applied")
	}
	if got := watermarkFor(t, ctx, connection, table); got != "0/7000" {
		t.Fatalf("watermark after later truncate = %s, want 0/7000", got)
	}
}

// Multi-relation truncates share the source transaction and one destination
// transaction: either every table is cleared or none is, and a malformed
// relation fails loudly (poison) instead of half-applying.
func TestTruncateMultiTableAtomicityAndLoudFailure(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	tableA := uniqueTable("ittrunc_a")
	tableB := uniqueTable("ittrunc_b")

	for _, table := range []string{tableA, tableB} {
		if _, err := connection.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE public.%s (
				id BIGINT PRIMARY KEY,
				name TEXT NOT NULL,
				%s
			)`, table, metaColumnsDDL)); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
		t.Cleanup(func() { dropDestTables(t, connection, table) })
		seedRows(t, ctx, connection, table, 1, 2, 3)
	}

	// One source transaction truncates tableA and tableB; a sibling truncate of
	// a table the destination does not have makes the whole group fail.
	missing := uniqueTable("ittrunc_missing")
	transaction, err := connection.Begin(ctx)
	if err != nil {
		t.Fatalf("begin destination transaction: %v", err)
	}
	_, err = applier.ApplyInTx(ctx, transaction, truncateEvent(missing+"-trunc-bad", missing, 910, 1, 3, "0/8000"))
	if err == nil {
		t.Fatal("truncate of a missing destination table applied silently")
	}
	if !errclass.IsPermanent(err) {
		t.Fatalf("missing-table truncate must be permanent, got %v", err)
	}
	if IsUndefinedColumn(err) {
		t.Fatal("missing-table truncate must not be treated as healable column drift")
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback failed group: %v", err)
	}

	// Nothing was half-applied: both tables keep their rows, and no markers or
	// watermarks leaked from the rolled-back transaction.
	if countRows(t, ctx, connection, tableA) != 3 || countRows(t, ctx, connection, tableB) != 3 {
		t.Fatalf("poisoned group half-applied: A=%d B=%d", countRows(t, ctx, connection, tableA), countRows(t, ctx, connection, tableB))
	}
	if watermarkFor(t, ctx, connection, tableA) != "" || watermarkFor(t, ctx, connection, tableB) != "" {
		t.Fatal("rolled-back group leaked a truncate watermark")
	}

	// Now the same two truncates apply atomically in one transaction.
	transaction, err = connection.Begin(ctx)
	if err != nil {
		t.Fatalf("begin destination transaction: %v", err)
	}
	if applied, applyErr := applier.ApplyInTx(ctx, transaction,
		truncateEvent(tableA+"-trunc-a", tableA, 911, 1, 2, "0/8100")); applyErr != nil || !applied {
		t.Fatalf("apply truncate A: applied=%v err=%v", applied, applyErr)
	}
	if applied, applyErr := applier.ApplyInTx(ctx, transaction,
		truncateEvent(tableB+"-trunc-b", tableB, 911, 2, 2, "0/8100")); applyErr != nil || !applied {
		t.Fatalf("apply truncate B: applied=%v err=%v", applied, applyErr)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatalf("commit truncate group: %v", err)
	}
	if countRows(t, ctx, connection, tableA) != 0 || countRows(t, ctx, connection, tableB) != 0 {
		t.Fatalf("atomic truncate did not clear both tables: A=%d B=%d", countRows(t, ctx, connection, tableA), countRows(t, ctx, connection, tableB))
	}
	if watermarkFor(t, ctx, connection, tableA) != "0/8100" || watermarkFor(t, ctx, connection, tableB) != "0/8100" {
		t.Fatalf("watermarks not advanced atomically: A=%s B=%s",
			watermarkFor(t, ctx, connection, tableA), watermarkFor(t, ctx, connection, tableB))
	}
}
