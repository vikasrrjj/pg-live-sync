//go:build integration

package destination

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"example.com/pg-live-sync/internal/cdc"
)

// A quarantined update must round-trip: the preserved TOAST columns and the
// before row survive the dead-letter table, and ReplayDeadLetter re-applies
// through the same path the live writer uses, then clears the record.
func TestDeadLetterReplayRoundTrip(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itdlqround")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id TEXT PRIMARY KEY,
			value TEXT,
			bio TEXT,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	beforeRow := cdc.Row{"id": stringPtr("1"), "value": stringPtr("old"), "bio": stringPtr("bio")}
	afterRow := cdc.Row{"id": stringPtr("1"), "value": stringPtr("new")}
	event := cdc.Event{
		Version: cdc.EventVersion, ID: table + "-update", Schema: "public", Table: table,
		Operation: cdc.OperationUpdate, Key: cdc.Row{"id": stringPtr("1")},
		Before: beforeRow, After: afterRow, UnchangedColumns: []string{"bio"},
		Source: cdc.SourceMetadata{TransactionID: 7, CommitLSN: "0/8000", TransactionEndLSN: "0/8000",
			CommitTime: time.Now(), Sequence: 1, TransactionEventCount: 1},
	}
	if err := applier.RecordDeadLetter(ctx, event, "test quarantine"); err != nil {
		t.Fatalf("record dead letter: %v", err)
	}

	loaded, err := applier.GetDeadLetter(ctx, event.ID)
	if err != nil {
		t.Fatalf("get dead letter: %v", err)
	}
	if loaded.Operation != cdc.OperationUpdate || loaded.SourceLSN != "0/8000" {
		t.Fatalf("loaded operation/lsn = %s/%s, want update/0/8000", loaded.Operation, loaded.SourceLSN)
	}
	if len(loaded.UnchangedColumns) != 1 || loaded.UnchangedColumns[0] != "bio" {
		t.Fatalf("loaded unchanged columns = %v, want [bio]", loaded.UnchangedColumns)
	}
	if loaded.Before["value"] == nil || *loaded.Before["value"] != "old" {
		t.Fatalf("loaded before row = %v, want value=old", loaded.Before)
	}
	if loaded.After["value"] == nil || *loaded.After["value"] != "new" {
		t.Fatalf("loaded after row = %v, want value=new", loaded.After)
	}

	listed, err := applier.ListDeadLetters(ctx, "", "")
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if !containsEventID(listed, event.ID) {
		t.Fatalf("list without filter misses %s", event.ID)
	}
	listed, err = applier.ListDeadLetters(ctx, "public", table)
	if err != nil {
		t.Fatalf("list dead letters by table: %v", err)
	}
	if len(listed) != 1 || listed[0].EventID != event.ID {
		t.Fatalf("list by table = %+v, want exactly %s", listed, event.ID)
	}

	applied, err := applier.ReplayDeadLetter(ctx, event.ID)
	if err != nil {
		t.Fatalf("replay dead letter: %v", err)
	}
	if !applied {
		t.Fatal("replay reported the event was not applied")
	}
	var value, bio any
	if err := connection.QueryRow(ctx,
		`SELECT value, bio FROM public.`+table+` WHERE id = '1'`).Scan(&value, &bio); err != nil {
		t.Fatalf("row after replay: %v", err)
	}
	if value != "new" {
		t.Fatalf("value after replay = %v, want new", value)
	}
	if bio != nil {
		t.Fatalf("bio after replay = %v, want NULL (never written)", bio)
	}

	listed, err = applier.ListDeadLetters(ctx, "public", table)
	if err != nil {
		t.Fatalf("list after replay: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("dead letter still present after successful replay: %+v", listed)
	}
	if _, err := applier.GetDeadLetter(ctx, event.ID); err == nil {
		t.Fatal("get after replay must report the dead letter is gone")
	}

	if err := applier.DropDeadLetter(ctx, event.ID); err != nil {
		t.Fatalf("dropping an already-replayed dead letter must be a no-op, got: %v", err)
	}
}

// A dead letter whose apply still fails permanently must keep both its record
// and its quarantine marker (so a writer restart does not retry it), then
// replay cleanly once the destination schema accepts the value.
func TestDeadLetterPermanentFailureKeepsQuarantineUntilFixed(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itdlqpoison")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id TEXT PRIMARY KEY,
			age BIGINT NOT NULL,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	event := cdc.Event{
		Version: cdc.EventVersion, ID: table + "-poison", Schema: "public", Table: table,
		Operation: cdc.OperationInsert, Key: cdc.Row{"id": stringPtr("5")},
		After: cdc.Row{"id": stringPtr("5"), "age": stringPtr("not-a-bigint")},
		Source: cdc.SourceMetadata{TransactionID: 8, CommitLSN: "0/9000", TransactionEndLSN: "0/9000",
			CommitTime: time.Now(), Sequence: 1, TransactionEventCount: 1},
	}
	if err := applier.RecordDeadLetter(ctx, event, "bad value cast"); err != nil {
		t.Fatalf("record dead letter: %v", err)
	}

	if _, err := applier.ReplayDeadLetter(ctx, event.ID); err == nil {
		t.Fatal("replay of the poison must fail permanently")
	}
	if _, err := applier.GetDeadLetter(ctx, event.ID); err != nil {
		t.Fatalf("record must survive a failed replay: %v", err)
	}
	var markers int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_applied_events WHERE event_id = $1`, event.ID).Scan(&markers); err != nil {
		t.Fatalf("count quarantine markers: %v", err)
	}
	if markers != 1 {
		t.Fatalf("re-stashed quarantine marker count = %d, want 1 (restart must not retry)", markers)
	}

	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE public.%s ALTER COLUMN age TYPE TEXT`, table)); err != nil {
		t.Fatalf("fix schema: %v", err)
	}
	applier.invalidateColumns("public", table)

	applied, err := applier.ReplayDeadLetter(ctx, event.ID)
	if err != nil {
		t.Fatalf("replay after fix: %v", err)
	}
	if !applied {
		t.Fatal("replay after fix reported not applied")
	}
	var age string
	if err := connection.QueryRow(ctx,
		`SELECT age FROM public.`+table+` WHERE id = '5'`).Scan(&age); err != nil {
		t.Fatalf("row after fixed replay: %v", err)
	}
	if age != "not-a-bigint" {
		t.Fatalf("age after fixed replay = %q, want not-a-bigint", age)
	}
	if _, err := applier.GetDeadLetter(ctx, event.ID); err == nil {
		t.Fatal("dead letter must be cleared by the successful replay")
	}
}

// DropDeadLetter removes the record and its quarantine marker in one
// transaction, so the event can neither be replayed nor retried by a restart.
func TestDeadLetterDropClearsRecordAndMarker(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itdlqdrop")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id TEXT PRIMARY KEY,
			value TEXT,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	event := cdc.Event{
		Version: cdc.EventVersion, ID: table + "-dropme", Schema: "public", Table: table,
		Operation: cdc.OperationInsert, Key: cdc.Row{"id": stringPtr("9")},
		After: cdc.Row{"id": stringPtr("9"), "value": stringPtr("v")},
		Source: cdc.SourceMetadata{TransactionID: 9, CommitLSN: "0/A000", TransactionEndLSN: "0/A000",
			CommitTime: time.Now(), Sequence: 1, TransactionEventCount: 1},
	}
	if err := applier.RecordDeadLetter(ctx, event, "investigate"); err != nil {
		t.Fatalf("record dead letter: %v", err)
	}
	if err := applier.DropDeadLetter(ctx, event.ID); err != nil {
		t.Fatalf("drop dead letter: %v", err)
	}
	var markers int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_applied_events WHERE event_id = $1`, event.ID).Scan(&markers); err != nil {
		t.Fatalf("count markers: %v", err)
	}
	if markers != 0 {
		t.Fatalf("marker survived drop: %d", markers)
	}
	if _, err := applier.GetDeadLetter(ctx, event.ID); err == nil {
		t.Fatal("record survived drop")
	}
	var records int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.cdc_dead_letters WHERE event_id = $1`, event.ID).Scan(&records); err != nil {
		t.Fatalf("count records: %v", err)
	}
	if records != 0 {
		t.Fatalf("dead-letter record survived drop: %d", records)
	}
}

func containsEventID(records []DeadLetter, eventID string) bool {
	for _, record := range records {
		if record.EventID == eventID {
			return true
		}
	}
	return false
}

// The live writer quarantines a poison event with its full replay fields; prove
// the columns survive the actual application path end to end.
func TestLiveQuarantineStoresReplayFields(t *testing.T) {
	integrationGate(t)
	ctx := context.Background()
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	table := uniqueTable("itdlqlive")
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id TEXT PRIMARY KEY,
			age BIGINT NOT NULL,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	poison := cdc.Event{
		Version: cdc.EventVersion, ID: table + "-live-poison", Schema: "public", Table: table,
		Operation: cdc.OperationInsert, Key: cdc.Row{"id": stringPtr("42")},
		After: cdc.Row{"id": stringPtr("42"), "age": stringPtr("oops")},
		Source: cdc.SourceMetadata{TransactionID: 10, CommitLSN: "0/B000", TransactionEndLSN: "0/B000",
			CommitTime: time.Now(), Sequence: 1, TransactionEventCount: 1},
	}

	transaction, err := connection.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var batchErr *BatchError
	err = applier.ApplyBatch(ctx, transaction, []cdc.Event{poison})
	if !errors.As(err, &batchErr) {
		t.Fatalf("ApplyBatch must surface a BatchError for the poison, got: %v", err)
	}
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	// The writer's quarantine path: once healthy events are re-applied, the
	// poison itself is recorded as a dead letter with its full replay payload.
	if err := applier.RecordDeadLetter(ctx, batchErr.Event, batchErr.Error()); err != nil {
		t.Fatalf("quarantine poison: %v", err)
	}

	healthy, err := applier.ReplayDeadLetter(ctx, poison.ID)
	if err == nil {
		t.Fatalf("replay of poison: applied=%v, want failure before the schema is fixed", healthy)
	}
	// Re-stash path: the record stays and the marker is back so nothing
	// retries it behind our back; the writer's real poison flow is unaffected.
	record, err := applier.GetDeadLetter(ctx, poison.ID)
	if err != nil {
		t.Fatalf("poison record missing after batch quarantine: %v", err)
	}
	if record.After["age"] == nil || *record.After["age"] != "oops" {
		t.Fatalf("poison after row = %v, want age=oops", record.After)
	}
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE public.%s ALTER COLUMN age TYPE TEXT`, table)); err != nil {
		t.Fatalf("fix schema: %v", err)
	}
	applier.invalidateColumns("public", table)
	applied, err := applier.ReplayDeadLetter(ctx, poison.ID)
	if err != nil || !applied {
		t.Fatalf("replay after fix: applied=%v err=%v", applied, err)
	}
	var age string
	if err := connection.QueryRow(ctx,
		`SELECT age FROM public.`+table+` WHERE id = '42'`).Scan(&age); err != nil {
		t.Fatalf("row after replay: %v", err)
	}
	if age != "oops" {
		t.Fatalf("age after replay = %q, want oops", age)
	}
}
