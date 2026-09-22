package source

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"example.com/pg-live-sync/internal/cdc"
	"github.com/jackc/pglogrepl"
)

func TestDeleteKeyTupleUsesRelationColumnPositions(t *testing.T) {
	tests := []struct {
		name       string
		columns    []*pglogrepl.RelationMessageColumn
		tuple      *pglogrepl.TupleData
		primaryKey string
	}{
		{
			name: "primary key is the first column",
			columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", Flags: 1},
				{Name: "name"},
				{Name: "email"},
			},
			tuple:      tupleData(textColumn("42"), nullColumn(), nullColumn()),
			primaryKey: "42",
		},
		{
			name: "primary key is not the first column",
			columns: []*pglogrepl.RelationMessageColumn{
				{Name: "name"},
				{Name: "id", Flags: 1},
				{Name: "email"},
			},
			tuple:      tupleData(nullColumn(), textColumn("84"), nullColumn()),
			primaryKey: "84",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := NewDecoder()
			relation := &pglogrepl.RelationMessage{
				RelationID:   17,
				Namespace:    "public",
				RelationName: "users",
				Columns:      test.columns,
			}

			mustHandleWithoutCommit(t, decoder, relation)
			mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 700})
			mustHandleWithoutCommit(t, decoder, &pglogrepl.DeleteMessage{
				RelationID:   relation.RelationID,
				OldTupleType: pglogrepl.DeleteMessageTupleTypeKey,
				OldTuple:     test.tuple,
			})

			transaction, err := decoder.Handle(&pglogrepl.CommitMessage{
				CommitLSN:         pglogrepl.LSN(0x100),
				TransactionEndLSN: pglogrepl.LSN(0x120),
				CommitTime:        time.Unix(1_700_000_000, 0).UTC(),
			})
			if err != nil {
				t.Fatalf("commit transaction: %v", err)
			}
			if transaction == nil || transaction.Count != 1 {
				t.Fatalf("got transaction %#v, want count 1", transaction)
			}

			events := pullEvents(t, decoder)
			if len(events) != 1 {
				t.Fatalf("got %d events, want 1", len(events))
			}
			event := events[0]
			if event.Operation != cdc.OperationDelete {
				t.Fatalf("operation = %q, want %q", event.Operation, cdc.OperationDelete)
			}
			id := event.Key["id"]
			if id == nil || *id != test.primaryKey {
				t.Fatalf("key id = %v, want %q", id, test.primaryKey)
			}
			if len(event.Before) != 1 {
				t.Fatalf("before = %#v, want only the replica-identity column", event.Before)
			}
		})
	}
}

func TestTransactionIsReleasedOnlyAtCommit(t *testing.T) {
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID:   23,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
			{Name: "email"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 701})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple: tupleData(
			textColumn("9"),
			textColumn("before commit"),
			textColumn("phase2@example.com"),
		),
	})

	transaction, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(0x200),
		TransactionEndLSN: pglogrepl.LSN(0x240),
		CommitTime:        time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if transaction == nil || transaction.Count != 1 {
		t.Fatalf("got transaction %#v, want count 1", transaction)
	}
	if transaction.TransactionID != 701 {
		t.Fatalf("transaction id = %d, want 701", transaction.TransactionID)
	}

	events := pullEvents(t, decoder)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Source.Sequence != 1 || events[0].Source.TransactionEventCount != 1 {
		t.Fatalf("source metadata = %#v, want sequence 1 of 1", events[0].Source)
	}
}

func TestEventIDsAreStableAndSequentialCommitLSNPlusSequence(t *testing.T) {
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID:   31,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
		},
	}
	mustHandleWithoutCommit(t, decoder, relation)

	insert := func(id string) {
		mustHandleWithoutCommit(t, decoder, &pglogrepl.InsertMessage{
			RelationID: relation.RelationID,
			Tuple:      tupleData(textColumn(id), textColumn("name "+id)),
		})
	}

	commit := func(lsn uint64) {
		transaction, err := decoder.Handle(&pglogrepl.CommitMessage{
			CommitLSN:         pglogrepl.LSN(lsn),
			TransactionEndLSN: pglogrepl.LSN(lsn + 0x40),
			CommitTime:        time.Unix(1_700_000_000, 0).UTC(),
		})
		if err != nil {
			t.Fatalf("commit transaction: %v", err)
		}
		if transaction == nil {
			t.Fatalf("commit returned no transaction")
		}
	}

	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 710})
	insert("1")
	insert("2")
	commit(0x100)
	events := pullEvents(t, decoder)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	for index, wantID := range []string{"0/100:1", "0/100:2"} {
		if events[index].ID != wantID {
			t.Fatalf("event %d id = %q, want %q", index, events[index].ID, wantID)
		}
		if events[index].Source.Sequence != index+1 {
			t.Fatalf("event %d sequence = %d, want %d", index, events[index].Source.Sequence, index+1)
		}
	}

	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 711})
	insert("3")
	commit(0x200)
	events = pullEvents(t, decoder)
	if len(events) != 1 || events[0].ID != "0/200:1" {
		t.Fatalf("second transaction events = %#v, want single id %q", events, "0/200:1")
	}
}

func TestUpdateChangingPrimaryKeyEmitsDeleteThenInsert(t *testing.T) {
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID:   41,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 720})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   relation.RelationID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple:     tupleData(textColumn("1"), nullColumn()),

		NewTuple: tupleData(textColumn("2"), textColumn("renamed")),
	})

	transaction, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(0x300),
		TransactionEndLSN: pglogrepl.LSN(0x340),
		CommitTime:        time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if transaction == nil || transaction.Count != 2 {
		t.Fatalf("got transaction %#v, want count 2", transaction)
	}

	events := pullEvents(t, decoder)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Operation != cdc.OperationDelete {
		t.Fatalf("first operation = %q, want delete", events[0].Operation)
	}
	if id := *events[0].Key["id"]; id != "1" {
		t.Fatalf("delete key = %q, want old key 1", id)
	}
	if events[1].Operation != cdc.OperationInsert {
		t.Fatalf("second operation = %q, want insert", events[1].Operation)
	}
	if id := *events[1].Key["id"]; id != "2" {
		t.Fatalf("insert key = %q, want new key 2", id)
	}
}

func TestUnchangedToastFilledFromFullOldImage(t *testing.T) {
	// REPLICA IDENTITY FULL: the old image carries every column, so the
	// unchanged toast value is copied from it (it is the source's truth) and the
	// row stays complete for the destination.
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID:   43,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
			{Name: "bio"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 721})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   relation.RelationID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple: tupleData(
			textColumn("1"),
			textColumn("Old Name"),
			textColumn("Old Bio"),
		),

		NewTuple: tupleData(
			textColumn("1"),
			unchangedColumn(),
			textColumn("New Bio"),
		),
	})

	commit, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(0x310),
		TransactionEndLSN: pglogrepl.LSN(0x350),
		CommitTime:        time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if commit == nil {
		t.Fatalf("commit returned no transaction")
	}

	events := pullEvents(t, decoder)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	after := events[0].After
	if name := *after["name"]; name != "Old Name" {
		t.Fatalf("unchanged toast name = %q, want old image %q", name, "Old Name")
	}
	if bio := *after["bio"]; bio != "New Bio" {
		t.Fatalf("changed bio = %q, want %q", bio, "New Bio")
	}
	if len(events[0].UnchangedColumns) != 0 {
		t.Fatalf("a full old image must produce no preservation hints, got %v", events[0].UnchangedColumns)
	}
}

func TestUnchangedToastPreservedWithoutOldImage(t *testing.T) {
	// Default replica identity: the old tuple is absent (or key-only), so the
	// unchanged toast value genuinely cannot be known. It becomes a
	// preservation hint instead of a guessed value. No REPLICA IDENTITY FULL
	// requirement: this configuration must decode cleanly.
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID:   44,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 722})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID: relation.RelationID,
		NewTuple: tupleData(
			textColumn("1"),
			unchangedColumn(),
		),
	})

	commit, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(0x320),
		TransactionEndLSN: pglogrepl.LSN(0x360),
		CommitTime:        time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if commit == nil {
		t.Fatalf("commit returned no transaction")
	}

	events := pullEvents(t, decoder)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if _, present := events[0].After["name"]; present {
		t.Fatalf("unknown unchanged toast column must not be in After: %#v", events[0].After)
	}
	if len(events[0].UnchangedColumns) != 1 || events[0].UnchangedColumns[0] != "name" {
		t.Fatalf("unchanged columns = %v, want [name]", events[0].UnchangedColumns)
	}
}

func TestUnchangedToastPreservedWithKeyOnlyOldImage(t *testing.T) {
	// Default replica identity sends a key-only old tuple (K): the old image has
	// the primary key but not the toast column, so that column is still unknown
	// and must be preserved rather than filled.
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID:   46,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 724})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   relation.RelationID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple: tupleData(
			textColumn("1"),
			nullColumn(),
		),

		NewTuple: tupleData(
			textColumn("1"),
			unchangedColumn(),
		),
	})

	commit, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(0x340),
		TransactionEndLSN: pglogrepl.LSN(0x380),
		CommitTime:        time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if commit == nil {
		t.Fatalf("commit returned no transaction")
	}

	events := pullEvents(t, decoder)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if _, present := events[0].After["name"]; present {
		t.Fatalf("unknown unchanged toast column must not be in After: %#v", events[0].After)
	}
	if len(events[0].UnchangedColumns) != 1 || events[0].UnchangedColumns[0] != "name" {
		t.Fatalf("unchanged columns = %v, want [name]", events[0].UnchangedColumns)
	}
}

func TestKeyChangeFillsUnchangedToastFromOldImage(t *testing.T) {
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID:   45,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
			{Name: "bio"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 723})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   relation.RelationID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple: tupleData(
			textColumn("1"),
			textColumn("Old Name"),
			textColumn("Old Bio"),
		),

		NewTuple: tupleData(
			textColumn("2"),
			unchangedColumn(),
			textColumn("New Bio"),
		),
	})

	commit, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(0x330),
		TransactionEndLSN: pglogrepl.LSN(0x370),
		CommitTime:        time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if commit == nil {
		t.Fatalf("commit returned no transaction")
	}

	events := pullEvents(t, decoder)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (delete then insert)", len(events))
	}
	if events[0].Operation != cdc.OperationDelete {
		t.Fatalf("first operation = %q, want delete", events[0].Operation)
	}
	insert := events[1]
	if insert.Operation != cdc.OperationInsert {
		t.Fatalf("second operation = %q, want insert", insert.Operation)
	}
	// The new-key row must be complete: the unchanged toast value comes from the
	// old image, so the insert never fabricates NULL for it.
	name := insert.After["name"]
	if name == nil || *name != "Old Name" {
		t.Fatalf("insert after name = %v, want old image %q", name, "Old Name")
	}
	if bio := *insert.After["bio"]; bio != "New Bio" {
		t.Fatalf("insert after bio = %q, want %q", bio, "New Bio")
	}
	if len(insert.UnchangedColumns) != 0 {
		t.Fatalf("the key-change insert must be complete, unchanged = %v", insert.UnchangedColumns)
	}
}

func TestHugeTransactionSpoolsBeyondMemoryCap(t *testing.T) {
	decoder := NewDecoderWithSpoolCap(2)
	relation := &pglogrepl.RelationMessage{
		RelationID:   47,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 730})
	for id := 1; id <= 5; id++ {
		idString := strconv.Itoa(id)
		mustHandleWithoutCommit(t, decoder, &pglogrepl.InsertMessage{
			RelationID: relation.RelationID,
			Tuple:      tupleData(textColumn(idString), textColumn("row "+idString)),
		})
	}

	transaction, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(0x400),
		TransactionEndLSN: pglogrepl.LSN(0x500),
		CommitTime:        time.Unix(1_700_000_002, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if transaction == nil || transaction.Count != 5 {
		t.Fatalf("got transaction %#v, want count 5", transaction)
	}

	spoolPath := decoder.pull.spoolPath
	if spoolPath == "" {
		t.Fatalf("expected a spool file for a transaction larger than the memory cap")
	}

	events := pullEvents(t, decoder)
	if len(events) != 5 {
		t.Fatalf("got %d events, want 5", len(events))
	}
	for index, event := range events {
		wantID := strconv.Itoa(index + 1)
		if id := *event.Key["id"]; id != wantID {
			t.Fatalf("event %d id = %q, want %q", index, id, wantID)
		}
		if event.Source.Sequence != index+1 || event.Source.TransactionEventCount != 5 {
			t.Fatalf("event %d source metadata = %#v", index, event.Source)
		}
	}
	if decoder.pull != nil {
		t.Fatalf("pull state not cleared after draining")
	}
	if _, err := os.Stat(spoolPath); !os.IsNotExist(err) {
		t.Fatalf("spool file %q still exists after draining", spoolPath)
	}
}

func pullEvents(t *testing.T, decoder *Decoder) []cdc.Event {
	t.Helper()
	var events []cdc.Event
	for {
		event, ok, err := decoder.NextEvent()
		if err != nil {
			t.Fatalf("pull next event: %v", err)
		}
		if !ok {
			return events
		}
		events = append(events, event)
	}
}

func mustHandleWithoutCommit(t *testing.T, decoder *Decoder, message pglogrepl.Message) {
	t.Helper()
	transaction, err := decoder.Handle(message)
	if err != nil {
		t.Fatalf("handle %T: %v", message, err)
	}
	if transaction != nil {
		t.Fatalf("handle %T returned a transaction before Commit: %#v", message, transaction)
	}
}

func tupleData(columns ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{
		ColumnNum: uint16(len(columns)),
		Columns:   columns,
	}
}

func textColumn(value string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{
		DataType: pglogrepl.TupleDataTypeText,
		Length:   uint32(len(value)),
		Data:     []byte(value),
	}
}

func nullColumn() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}

func unchangedColumn() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
}

func commit(t *testing.T, decoder *Decoder, lsn uint64) {
	t.Helper()
	transaction, err := decoder.Handle(&pglogrepl.CommitMessage{
		CommitLSN:         pglogrepl.LSN(lsn),
		TransactionEndLSN: pglogrepl.LSN(lsn + 0x20),
		CommitTime:        time.Unix(1_700_000_000, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if transaction == nil || transaction.Count < 1 {
		t.Fatalf("commit produced nothing; want at least 1 event")
	}
}

func assertKey(t *testing.T, events []cdc.Event, index int, column, want string) {
	t.Helper()
	if len(events) <= index {
		t.Fatalf("events = %#v, want at least %d", events, index+1)
	}
	key := events[index].Key
	if len(key) != 1 {
		t.Fatalf("event %d key = %#v, want exactly 1 column", index, key)
	}
	value, ok := key[column]
	if !ok {
		t.Fatalf("event %d key %#v lacks column %q", index, key, column)
	}
	if *value != want {
		t.Fatalf("event %d key %s = %q, want %q", index, column, *value, want)
	}
}

// FULL replica identity flags every column as a key in the relation metadata.
// Without catalog discovery that would fabricate a whole-row key; with it, the
// pipeline key stays the actual primary key.
func TestIdentityResolverNarrowsFullKeys(t *testing.T) {
	decoder := NewDecoderWithIdentity(func(namespace, relation string) []string {
		if namespace == "public" && relation == "events" {
			return []string{"id"}
		}
		return nil
	})
	columns := []*pglogrepl.RelationMessageColumn{
		{Name: "id", Flags: 1},
		{Name: "name", Flags: 1},
		{Name: "bio", Flags: 1},
		{Name: "region", Flags: 1},
	}
	relation := &pglogrepl.RelationMessage{
		RelationID: 40, Namespace: "public", RelationName: "events",
		Columns: columns,
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 800})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.InsertMessage{
		RelationID: 40, Tuple: tupleData(
			textColumn("1"), textColumn("ada"), textColumn("bio-one"), textColumn("eu")),
	})
	commit(t, decoder, 0x1000)
	events := pullEvents(t, decoder)
	assertKey(t, events, 0, "id", "1")

	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 801})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   40,
		OldTupleType: 'O',
		OldTuple: tupleData(
			textColumn("1"), textColumn("ada"), textColumn("bio-one"), textColumn("eu")),
		NewTuple: tupleData(
			textColumn("1"), textColumn("ada"), textColumn("bio-two"), textColumn("eu")),
	})
	commit(t, decoder, 0x2000)
	events = pullEvents(t, decoder)
	assertKey(t, events, 0, "id", "1")
	for _, event := range events {
		if event.Operation != cdc.OperationUpdate {
			t.Fatalf("non-key change under FULL should emit one update, got %#v", events)
		}
	}

	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 802})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   40,
		OldTupleType: 'O',
		OldTuple: tupleData(
			textColumn("1"), textColumn("ada"), textColumn("bio-two"), textColumn("eu")),
		NewTuple: tupleData(
			textColumn("2"), textColumn("ada"), textColumn("bio-two"), textColumn("us")),
	})
	commit(t, decoder, 0x3000)
	events = pullEvents(t, decoder)
	if events[0].Operation != cdc.OperationDelete || events[1].Operation != cdc.OperationInsert {
		t.Fatalf("key change under FULL should emit delete+insert, got %#v", events)
	}
	assertKey(t, events, 0, "id", "1")
	assertKey(t, events, 1, "id", "2")

	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 803})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.DeleteMessage{
		RelationID: 40, OldTupleType: 'O',
		OldTuple: tupleData(
			textColumn("2"), textColumn("ada"), textColumn("bio-two"), textColumn("us")),
	})
	commit(t, decoder, 0x4000)
	events = pullEvents(t, decoder)
	assertKey(t, events, 0, "id", "2")
}

// USING INDEX replica identity: the relation flags the index columns, and the
// resolved identity must be exactly those columns.
func TestIdentityResolverUsingIndex(t *testing.T) {
	decoder := NewDecoderWithIdentity(func(namespace, relation string) []string {
		return []string{"email"}
	})
	relation := &pglogrepl.RelationMessage{
		RelationID: 41, Namespace: "public", RelationName: "members",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id"},
			{Name: "email", Flags: 1},
			{Name: "region"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 810})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.InsertMessage{
		RelationID: 41, Tuple: tupleData(
			textColumn("7"), textColumn("a@b.org"), textColumn("eu")),
	})
	commit(t, decoder, 0x5000)
	assertKey(t, pullEvents(t, decoder), 0, "email", "a@b.org")

	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 811})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   41,
		OldTupleType: 'K',
		OldTuple: tupleData(
			nullColumn(), textColumn("a@b.org"), nullColumn()),
		NewTuple: tupleData(
			textColumn("7"), textColumn("a@b.org"), textColumn("us")),
	})
	commit(t, decoder, 0x6000)
	assertKey(t, pullEvents(t, decoder), 0, "email", "a@b.org")

	// Changing the identity column is a key change: delete(old identity) +
	// insert(new identity), keyed by the index, not the table's primary key.
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 812})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.UpdateMessage{
		RelationID:   41,
		OldTupleType: 'K',
		OldTuple: tupleData(
			nullColumn(), textColumn("a@b.org"), nullColumn()),
		NewTuple: tupleData(
			textColumn("7"), textColumn("c@d.org"), textColumn("us")),
	})
	commit(t, decoder, 0x7000)
	events := pullEvents(t, decoder)
	if events[0].Operation != cdc.OperationDelete || events[1].Operation != cdc.OperationInsert {
		t.Fatalf("identity-column change should emit delete+insert, got %#v", events)
	}
	assertKey(t, events, 0, "email", "a@b.org")
	assertKey(t, events, 1, "email", "c@d.org")
}

// A TRUNCATE references whole tables: the decoder emits one truncate event per
// relation, inside the same source transaction, and never fabricates a row key.
func TestTruncateDecodesOneEventPerRelation(t *testing.T) {
	decoder := NewDecoder()
	users := &pglogrepl.RelationMessage{
		RelationID: 50, Namespace: "public", RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", Flags: 1}},
	}
	orders := &pglogrepl.RelationMessage{
		RelationID: 51, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", Flags: 1}},
	}
	mustHandleWithoutCommit(t, decoder, users)
	mustHandleWithoutCommit(t, decoder, orders)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 900})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.TruncateMessage{
		RelationIDs: []uint32{50, 51},
		Option:      pglogrepl.TruncateOptionCascade,
	})
	commit(t, decoder, 0x9000)

	events := pullEvents(t, decoder)
	if len(events) != 2 {
		t.Fatalf("got %d truncate events, want 2: %#v", len(events), events)
	}
	if events[0].Operation != cdc.OperationTruncate || events[0].Table != "users" {
		t.Fatalf("event 0 = %s/%s, want truncate/users", events[0].Operation, events[0].Table)
	}
	if events[1].Operation != cdc.OperationTruncate || events[1].Table != "orders" {
		t.Fatalf("event 1 = %s/%s, want truncate/orders", events[1].Operation, events[1].Table)
	}
	for _, event := range events {
		if len(event.Key) != 0 || event.Before != nil || event.After != nil {
			t.Fatalf("truncate event must have no key/before/after payload: %#v", event)
		}
		if event.Source.TransactionID != 900 {
			t.Fatalf("truncate transaction id = %d, want 900", event.Source.TransactionID)
		}
		if event.Source.CommitLSN != "0/9000" {
			t.Fatalf("truncate commit LSN = %s, want 0/9000", event.Source.CommitLSN)
		}
	}
}

// A truncate that references a relation pgoutput never described is malformed:
// it must fail loudly instead of truncating the wrong (or no) table.
func TestTruncateWithUndescribedRelationFails(t *testing.T) {
	decoder := NewDecoder()
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 901})
	if transaction, err := decoder.Handle(&pglogrepl.TruncateMessage{
		RelationIDs: []uint32{999},
	}); err == nil {
		t.Fatalf("truncate with undescribed relation succeeded: %#v", transaction)
	} else if !strings.Contains(err.Error(), "undescribed relation") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A TRUNCATE outside a transaction is invalid and must be refused.
func TestTruncateOutsideTransactionFails(t *testing.T) {
	decoder := NewDecoder()
	relation := &pglogrepl.RelationMessage{
		RelationID: 52, Namespace: "public", RelationName: "t",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", Flags: 1}},
	}
	mustHandleWithoutCommit(t, decoder, relation)
	if transaction, err := decoder.Handle(&pglogrepl.TruncateMessage{
		RelationIDs: []uint32{52},
	}); err == nil {
		t.Fatalf("truncate outside a transaction succeeded: %#v", transaction)
	}
}

// A resolver function that does not know a table must not change behavior: the
// pgoutput key flags remain the key (the default replica identity).
func TestIdentityResolverUnmappedTableFallsBackToFlags(t *testing.T) {
	decoder := NewDecoderWithIdentity(func(string, string) []string { return nil })
	relation := &pglogrepl.RelationMessage{
		RelationID: 42, Namespace: "public", RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", Flags: 1},
			{Name: "name"},
		},
	}

	mustHandleWithoutCommit(t, decoder, relation)
	mustHandleWithoutCommit(t, decoder, &pglogrepl.BeginMessage{Xid: 820})
	mustHandleWithoutCommit(t, decoder, &pglogrepl.DeleteMessage{
		RelationID: 42, OldTupleType: 'K',
		OldTuple: tupleData(textColumn("99"), nullColumn()),
	})
	commit(t, decoder, 0x8000)
	assertKey(t, pullEvents(t, decoder), 0, "id", "99")
}
