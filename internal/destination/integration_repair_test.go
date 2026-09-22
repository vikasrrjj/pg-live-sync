//go:build integration

package destination

import (
	"context"
	"fmt"
	"testing"
	"time"

	"example.com/artie-mini-cdc/internal/cdc"
	"github.com/jackc/pgx/v5"
)

// Column-added drift is the one schema change the pipeline can heal in place:
// the event fails with SQLSTATE 42703, RepairSchema clones the missing columns
// from the source, and the retried event applies. An enum type the destination
// lacks is created as part of the repair.
func TestRepairSchemaHealsColumnDrift(t *testing.T) {
	integrationGate(t)
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	ctx := context.Background()

	source, err := pgx.Connect(ctx, sourceIntegrationDSN())
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close(context.Background()) })

	// The source scratch table uses the live public.mood enum plus a text[] array.
	if _, err := source.Exec(ctx, `DROP TABLE IF EXISTS public.cdc_it_repair CASCADE`); err != nil {
		t.Fatalf("drop source scratch table: %v", err)
	}
	t.Cleanup(func() { _, _ = source.Exec(context.Background(), `DROP TABLE IF EXISTS public.cdc_it_repair CASCADE`) })
	if _, err := source.Exec(ctx, `
		CREATE TABLE public.cdc_it_repair (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			mood public.mood,
			tags TEXT[]
		)`); err != nil {
		t.Fatalf("create source scratch table: %v", err)
	}
	if _, err := source.Exec(ctx,
		`INSERT INTO public.cdc_it_repair (id, name, mood, tags) VALUES (1, 'zed', 'sad', ARRAY['z']::text[])`); err != nil {
		t.Fatalf("seed source scratch table: %v", err)
	}

	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.cdc_it_repair (id BIGINT PRIMARY KEY, name TEXT NOT NULL, %s)`, metaColumnsDDL)); err != nil {
		t.Fatalf("create dest_test scratch table: %v", err)
	}
	t.Cleanup(func() {
		dropDestTables(t, connection, "cdc_it_repair")
		if _, err := connection.Exec(context.Background(),
			`DROP TYPE IF EXISTS public.mood`); err != nil {
			t.Errorf("drop repair-created mood type: %v", err)
		}
	})

	event := cdc.Event{
		Version:   cdc.EventVersion,
		ID:        fmt.Sprintf("it-repair-%d", time.Now().UnixNano()),
		Schema:    "public",
		Table:     "cdc_it_repair",
		Operation: cdc.OperationInsert,
		Key:       cdc.Row{"id": str("1")},
		After:     cdc.Row{"id": str("1"), "name": str("zed"), "mood": str("sad"), "tags": str("{z}")},
		Source: cdc.SourceMetadata{
			TransactionID:         900,
			CommitLSN:             "0/2200000",
			TransactionEndLSN:     "0/2200000",
			CommitTime:            time.Now(),
			Sequence:              1,
			TransactionEventCount: 1,
		},
	}
	t.Cleanup(func() { clearMetadata(t, connection, []string{event.ID}) })

	// First the failure the writer classifies as repairable drift.
	applied, err := applier.Apply(ctx, event)
	if err == nil {
		t.Fatalf("expected 42703 for missing columns, applied=%v", applied)
	}
	if !IsUndefinedColumn(err) {
		t.Fatalf("expected an undefined-column failure, got: %v", err)
	}
	var atGuard int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_it_repair`).Scan(&atGuard); err != nil {
		t.Fatalf("count before repair: %v", err)
	}
	if atGuard != 0 {
		t.Fatalf("failed event must not leave rows, found %d", atGuard)
	}

	// Repair clones mood + tags from the source and creates the enum type.
	repaired, err := applier.RepairSchema(ctx, sourceIntegrationDSN(), event)
	if err != nil {
		t.Fatalf("repair schema: %v", err)
	}
	if !repaired {
		t.Fatal("RepairSchema must report it added columns")
	}

	rows, err := connection.Query(ctx, `
		SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = 'public.cdc_it_repair'::regclass
		  AND a.attnum > 0 AND NOT a.attisdropped AND a.attname IN ('mood', 'tags')
		ORDER BY a.attname`)
	if err != nil {
		t.Fatalf("inspect repaired columns: %v", err)
	}
	defer rows.Close()
	types := map[string]string{}
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			t.Fatalf("scan repaired column: %v", err)
		}
		types[name] = dataType
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate repaired columns: %v", err)
	}
	if types["tags"] != "text[]" {
		t.Fatalf("tags column not repaired as text[]: %v", types)
	}
	var enumExists bool
	if err := connection.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'mood' AND typnamespace = 'public'::regnamespace)`).
		Scan(&enumExists); err != nil {
		t.Fatalf("check mood type: %v", err)
	}
	var isEnum bool
	if err := connection.QueryRow(ctx,
		`SELECT t.typtype = 'e' FROM pg_type t WHERE t.typname = 'mood'`).Scan(&isEnum); err != nil {
		t.Fatalf("check mood is enum: %v", err)
	}
	if !enumExists || !isEnum {
		t.Fatalf("mood type not created as an enum: exists=%v isEnum=%v", enumExists, isEnum)
	}

	// The retried event applies with the typed values intact.
	applied, err = applier.Apply(ctx, event)
	if err != nil {
		t.Fatalf("apply after repair: %v", err)
	}
	if !applied {
		t.Fatal("retried event must apply")
	}
	var mood string
	var tags string
	if err := connection.QueryRow(ctx,
		`SELECT mood::text, tags::text FROM public.cdc_it_repair WHERE id = 1`).Scan(&mood, &tags); err != nil {
		t.Fatalf("read repaired row: %v", err)
	}
	if mood != "sad" || tags != "{z}" {
		t.Fatalf("repaired row mismatch: mood=%q tags=%q", mood, tags)
	}

	// A fully aligned table must not be reported as needing repair.
	repaired, err = applier.RepairSchema(ctx, sourceIntegrationDSN(), event)
	if err != nil {
		t.Fatalf("re-check repair: %v", err)
	}
	if repaired {
		t.Fatal("RepairSchema must not report changes when nothing is missing")
	}
}

// A type that exists on the source and destination cannot be silently changed; a
// column that already exists must be reported as "nothing to repair", because
// silently rewriting an existing column's type is how real pipelines drop data.
func TestRepairSchemaDoesNotRewriteExistingColumns(t *testing.T) {
	integrationGate(t)
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	ctx := context.Background()

	dropDestTables(t, connection, "it_reptype")
	t.Cleanup(func() { dropDestTables(t, connection, "it_reptype") })
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.it_reptype (id BIGINT PRIMARY KEY, name TEXT NOT NULL, %s)`, metaColumnsDDL)); err != nil {
		t.Fatalf("create it_reptype: %v", err)
	}

	event := cdc.Event{
		Version:   cdc.EventVersion,
		ID:        "it-reptype-0000000000000001",
		Schema:    "public",
		Table:     "it_reptype",
		Operation: cdc.OperationInsert,
		Key:       cdc.Row{"id": str("1")},
		After:     cdc.Row{"id": str("1"), "name": str("x")},
		Source: cdc.SourceMetadata{
			TransactionID:         901,
			CommitLSN:             "0/2300000",
			TransactionEndLSN:     "0/2300000",
			CommitTime:            time.Now(),
			Sequence:              1,
			TransactionEventCount: 1,
		},
	}

	repaired, err := applier.RepairSchema(ctx, sourceIntegrationDSN(), event)
	if err != nil {
		t.Fatalf("repair existing columns: %v", err)
	}
	if repaired {
		t.Fatal("RepairSchema must not touch a table that already has all the columns")
	}
}
