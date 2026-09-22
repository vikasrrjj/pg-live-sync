//go:build integration

package destination

import (
	"context"
	"fmt"
	"testing"

	"example.com/artie-mini-cdc/internal/cdc"
	"example.com/artie-mini-cdc/internal/errclass"
)

// Schema drift that cannot be repaired must fail loudly: the error is surfaced
// as permanent (never silently retried or fabricated), lands in the dead-letter
// queue with a reason, and leaves the destination with zero partial rows.
func TestApplyIncompatibleTypeChangeFailsLoudAndDeadLetters(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itddltype")

	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id BIGINT PRIMARY KEY,
			score NUMERIC(10,2),
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create type-drift destination table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	// The source column was altered to a shape the destination column cannot
	// take (e.g. NUMERIC -> TEXT with non-numeric content). The live event
	// carries the new text value and the destination cast must fail.
	event := eventWith(
		"ddl-type-cast-"+table, table, 910, 1, 1,
		cdc.Row{"id": str("5")},
		cdc.Row{"id": str("5"), "score": str("not-a-number")},
		"0/410A000",
	)

	_, err := applier.Apply(ctx, event)
	if err == nil {
		t.Fatal("incompatible type change applied silently; want a loud permanent failure")
	}
	if !errclass.IsPermanent(err) {
		t.Fatalf("incompatible type change error must be permanent, got %T: %v", err, err)
	}
	if IsUndefinedColumn(err) {
		t.Fatalf("type cast failure must not be classified as healable column drift: %v", err)
	}

	if err := applier.RecordDeadLetter(ctx, event, err.Error()); err != nil {
		t.Fatalf("record dead letter: %v", err)
	}
	var reason string
	if err := connection.QueryRow(ctx,
		`SELECT reason FROM public.cdc_dead_letters WHERE event_id = $1`, event.ID).Scan(&reason); err != nil {
		t.Fatalf("read dead letter for %s: %v", event.ID, err)
	}
	if reason == "" {
		t.Fatalf("dead letter reason is empty")
	}

	var count int
	if err := connection.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM public.%s WHERE id = 5`, table)).Scan(&count); err != nil {
		t.Fatalf("count partial row: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed event left %d partial rows on the destination", count)
	}
}

// A column dropped on the source but still NOT NULL on the destination cannot
// be applied; it must fail loudly and be quarantined, never silently NULLed or
// dropped below the destination's contract.
func TestApplyDroppedNotNullColumnFailsLoudAndDeadLetters(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itddldrop")

	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id BIGINT PRIMARY KEY,
			bio TEXT NOT NULL,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create drop-drift destination table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	// The source dropped its bio column, so live events no longer carry it.
	event := eventWith(
		"ddl-drop-notnull-"+table, table, 911, 1, 1,
		cdc.Row{"id": str("6")},
		cdc.Row{"id": str("6")},
		"0/411A000",
	)

	_, err := applier.Apply(ctx, event)
	if err == nil {
		t.Fatal("dropped NOT NULL column applied silently; want a loud permanent failure")
	}
	if !errclass.IsPermanent(err) {
		t.Fatalf("dropped NOT NULL column error must be permanent, got %v", err)
	}
	if IsUndefinedColumn(err) {
		t.Fatalf("dropped-column NOT NULL failure must not be treated as healable column drift: %v", err)
	}

	if err := applier.RecordDeadLetter(ctx, event, err.Error()); err != nil {
		t.Fatalf("record dead letter: %v", err)
	}
	var reason string
	if err := connection.QueryRow(ctx,
		`SELECT reason FROM public.cdc_dead_letters WHERE event_id = $1`, event.ID).Scan(&reason); err != nil {
		t.Fatalf("read dead letter for %s: %v", event.ID, err)
	}
	if reason == "" {
		t.Fatalf("dead letter reason is empty")
	}

	var count int
	if err := connection.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM public.%s WHERE id = 6`, table)).Scan(&count); err != nil {
		t.Fatalf("count partial row: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed dropped-column event left %d partial rows", count)
	}
}
