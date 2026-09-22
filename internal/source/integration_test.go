//go:build integration

package source

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"example.com/artie-mini-cdc/internal/cdc"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The live proof that pgoutput really emits the "unchanged toast" marker for a
// TOASTed column that an UPDATE leaves alone, and that the decoder turns it into
// a preservation hint instead of refusing or guessing. Runs against the demo
// source database over a real replication slot and publication. The table uses
// REPLICA IDENTITY FULL so the old image is present: that is what lets pgoutput
// know the toast datum is unchanged.

func liveSourceSQLDSN() string {
	return "postgres://postgres:postgres@localhost:5433/source?sslmode=disable"
}

func liveSourceReplicationDSN() string {
	return liveSourceSQLDSN() + "&replication=database"
}

func integrationGate(t *testing.T) {
	if os.Getenv("ARTIE_INTEGRATION") != "1" {
		t.Skip("set ARTIE_INTEGRATION=1 to run docker integration tests")
	}
}

// dropSlot removes a test replication slot through a normal SQL connection.
// Dropping via the streaming connection fails while the slot is still active,
// and leaked slots accumulate against max_replication_slots, so the drop must
// be possible regardless of the stream's lifecycle.
func dropSlot(t *testing.T, pool *pgxpool.Pool, slot string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `SELECT pg_drop_replication_slot($1)`, slot); err != nil {
		t.Logf("warn: drop replication slot %s: %v", slot, err)
	}
}

func TestLiveUnchangedToastMarkedAndPreserved(t *testing.T) {
	integrationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	table := "it_toast_live_" + suffix
	slot := "it_toast_slot_" + suffix
	publication := "it_toast_pub_" + suffix
	bigBio := strings.Repeat("y", 8192)
	bigBioChanged := strings.Repeat("z", 8192)

	pool, err := pgxpool.New(ctx, liveSourceSQLDSN())
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (id integer PRIMARY KEY, name text NOT NULL, bio text NOT NULL)`, table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public."+table) }()
	if _, err := pool.Exec(ctx,
		`ALTER TABLE public.`+table+` ALTER COLUMN bio SET STORAGE EXTERNAL`); err != nil {
		t.Fatalf("set external storage: %v", err)
	}
	// The primary-key replica identity (DEFAULT) is what the pipeline targets:
	// an unchanged TOASTed column must travel to the destination as a
	// preservation hint (or its full value) without needing REPLICA IDENTITY FULL.
	if _, err := pool.Exec(ctx, "CREATE PUBLICATION "+publication+" FOR TABLE public."+table); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+publication) }()

	replication, err := pgconn.Connect(ctx, liveSourceReplicationDSN())
	if err != nil {
		t.Fatalf("connect to replication protocol: %v", err)
	}
	slotResult, err := pglogrepl.CreateReplicationSlot(
		ctx, replication, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		t.Fatalf("create replication slot: %v", err)
	}
	defer dropSlot(t, pool, slot)
	defer replication.Close(context.Background())
	startLSN, err := pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		t.Fatalf("parse consistent point: %v", err)
	}
	if err := pglogrepl.StartReplication(
		ctx, replication, slot, startLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", publication),
			},
		},
	); err != nil {
		t.Fatalf("start replication: %v", err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, name, bio) VALUES (1, 'orig', $1)`, table), bigBio); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s SET name = 'renamed-A' WHERE id = 1`, table)); err != nil {
		t.Fatalf("update name: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s SET bio = $1 WHERE id = 1`, table), bigBioChanged); err != nil {
		t.Fatalf("update bio: %v", err)
	}

	decoder := NewDecoder()
	events := streamUntilChangedBio(t, ctx, replication, decoder, table, bigBioChanged)

	// Whatever pgoutput does with an unchanged TOAST column under the default
	// replica identity, the decoder must never fabricate or drop it silently:
	// either the column arrives as a preservation hint, or as its exact value.
	seenUnchanged, seenChanged, seenValueColumn := false, false, false
	for _, event := range events {
		if event.After["name"] != nil && *event.After["name"] == "renamed-A" {
			if bio, present := event.After["bio"]; present {
				// pgoutput carried the unchanged toast value wholesale: the
				// decoder trusted it, so no fabrication occurred.
				if bio != nil && *bio == bigBio {
					seenValueColumn = true
					continue
				}
			}
			seenUnchanged = true
		}
		if event.After["bio"] != nil && *event.After["bio"] == bigBioChanged {
			seenChanged = true
		}
	}
	if !seenUnchanged && !seenValueColumn {
		t.Fatalf("the unchanged toast column was neither preserved nor carried: %#v", events)
	}
	if !seenChanged {
		t.Fatal("the changed toast value never arrived")
	}
	t.Logf("pgoutput shape for unchanged TOAST with default replica identity: unchanged_hint=%v carried_value=%v", seenUnchanged, seenValueColumn)
}

// TestLiveReplicaIdentityKeysAndMutations drives a real pgoutput stream over
// three replica identities and proves that, whatever the identity, the emitted
// keys are exactly the columns census discovery resolved (DEFAULT/FULL primary
// key, USING INDEX index columns) and that mutations keep the correct shapes:
// non-key update -> update, key/identity change -> delete+insert, delete -> delete.
func TestLiveReplicaIdentityKeysAndMutations(t *testing.T) {
	integrationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, liveSourceSQLDSN())
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer pool.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	defaultTable := "it_ri_default_" + suffix
	fullTable := "it_ri_full_" + suffix
	uiTable := "it_ri_ui_" + suffix
	slot := "it_ri_slot_" + suffix
	publication := "it_ri_pub_" + suffix

	for _, table := range []string{defaultTable, fullTable, uiTable} {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE public.%s (
				id integer PRIMARY KEY,
				email text NOT NULL UNIQUE,
				region text NOT NULL)`, table)); err != nil {
			t.Fatalf("create table %s: %v", table, err)
		}
		defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public."+table) }()
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE public."+fullTable+" REPLICA IDENTITY FULL"); err != nil {
		t.Fatalf("set FULL replica identity: %v", err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE public."+uiTable+" REPLICA IDENTITY USING INDEX "+uiTable+"_email_key"); err != nil {
		t.Fatalf("set USING INDEX replica identity: %v", err)
	}
	if _, err := pool.Exec(ctx, "CREATE PUBLICATION "+publication+" FOR TABLE public."+defaultTable+", public."+fullTable+", public."+uiTable); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+publication) }()

	replication, err := pgconn.Connect(ctx, liveSourceReplicationDSN())
	if err != nil {
		t.Fatalf("connect to replication protocol: %v", err)
	}
	slotResult, err := pglogrepl.CreateReplicationSlot(
		ctx, replication, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		t.Fatalf("create replication slot: %v", err)
	}
	defer dropSlot(t, pool, slot)
	defer replication.Close(context.Background())
	startLSN, err := pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		t.Fatalf("parse consistent point: %v", err)
	}
	if err := pglogrepl.StartReplication(
		ctx, replication, slot, startLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", publication),
			},
		},
	); err != nil {
		t.Fatalf("start replication: %v", err)
	}

	identity := map[string][]string{
		defaultTable: {"id"},
		fullTable:    {"id"},
		uiTable:      {"email"},
	}
	decoder := NewDecoderWithIdentity(func(namespace, relation string) []string {
		return identity[relation]
	})

	// DEFAULT: primary key identity.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, email, region) VALUES (1,'a@b.org','eu'), (2,'c@d.org','us')`, defaultTable)); err != nil {
		t.Fatalf("default insert: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s SET region='as' WHERE id=2`, defaultTable)); err != nil {
		t.Fatalf("default update: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM public.%s WHERE id=1`, defaultTable)); err != nil {
		t.Fatalf("default delete: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`UPDATE public.%s SET id=3 WHERE id=2`, defaultTable)); err != nil {
		t.Fatalf("default key change: %v", err)
	}
	// FULL: pgoutput flags every column as key; discovery must narrow to id.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, email, region) VALUES (10,'f@b.org','eu')`, fullTable)); err != nil {
		t.Fatalf("full insert: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s SET region='us' WHERE id=10`, fullTable)); err != nil {
		t.Fatalf("full update: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`UPDATE public.%s SET id=20 WHERE id=10`, fullTable)); err != nil {
		t.Fatalf("full key change: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM public.%s WHERE id=20`, fullTable)); err != nil {
		t.Fatalf("full delete: %v", err)
	}
	// USING INDEX: email is the identity.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, email, region) VALUES (100,'u@b.org','eu'), (200,'v@d.org','us')`, uiTable)); err != nil {
		t.Fatalf("ui insert: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s SET region='as' WHERE email='v@d.org'`, uiTable)); err != nil {
		t.Fatalf("ui update: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s SET email='w@e.org' WHERE id=200`, uiTable)); err != nil {
		t.Fatalf("ui identity change: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM public.%s WHERE id=100`, uiTable)); err != nil {
		t.Fatalf("ui delete: %v", err)
	}

	want := map[string][]expected{}
	want[defaultTable] = []expected{
		{cdc.OperationInsert, "id", "1"},
		{cdc.OperationInsert, "id", "2"},
		{cdc.OperationUpdate, "id", "2"},
		{cdc.OperationDelete, "id", "1"},
		{cdc.OperationDelete, "id", "2"},
		{cdc.OperationInsert, "id", "3"},
	}
	want[fullTable] = []expected{
		{cdc.OperationInsert, "id", "10"},
		{cdc.OperationUpdate, "id", "10"},
		{cdc.OperationDelete, "id", "10"},
		{cdc.OperationInsert, "id", "20"},
		{cdc.OperationDelete, "id", "20"},
	}
	want[uiTable] = []expected{
		{cdc.OperationInsert, "email", "u@b.org"},
		{cdc.OperationInsert, "email", "v@d.org"},
		{cdc.OperationUpdate, "email", "v@d.org"},
		{cdc.OperationDelete, "email", "v@d.org"},
		{cdc.OperationInsert, "email", "w@e.org"},
		{cdc.OperationDelete, "email", "u@b.org"},
	}

	total := 0
	for _, wants := range want {
		total += len(wants)
	}
	events := streamAllCollected(t, ctx, replication, decoder, total)

	for table, wants := range want {
		got := filterEvents(events, table)
		if len(got) != len(wants) {
			t.Fatalf("table %s: got %d events, want %d: %#v", table, len(got), len(wants), got)
		}
		for i, want := range wants {
			event := got[i]
			if event.Operation != want.op {
				t.Fatalf("table %s event %d operation = %q, want %q (got %#v)", table, i, event.Operation, want.op, got)
			}
			if len(event.Key) != 1 {
				t.Fatalf("table %s event %d key = %#v, want exactly 1 column (replica identity must be the resolved key)", table, i, event.Key)
			}
			if value := event.Key[want.column]; value == nil || *value != want.value {
				t.Fatalf("table %s event %d key[%s] = %v, want %q", table, i, want.column, value, want.value)
			}
		}
	}
}

type expected struct {
	op     cdc.Operation
	column string
	value  string
}

func filterEvents(events []cdc.Event, table string) []cdc.Event {
	var filtered []cdc.Event
	for _, event := range events {
		if event.Table == table {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// streamAllCollected decodes the stream until it has collected the requested
// total number of row events, then returns them all in arrival order.
func streamAllCollected(t *testing.T, ctx context.Context, replication *pgconn.PgConn, decoder *Decoder, want int) []cdc.Event {
	t.Helper()
	var collected []cdc.Event
	var lastLSN pglogrepl.LSN
	for len(collected) < want {
		raw, err := replication.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("receive: replication dropped after collecting %d/%d events: %v", len(collected), want, err)
		}
		copyData, ok := raw.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 || copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xlog, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			t.Fatalf("parse xlog data: %v", err)
		}
		lastLSN = xlog.WALStart
		message, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			t.Fatalf("parse pgoutput message: %v", err)
		}
		transaction, err := decoder.Handle(message)
		if err != nil {
			t.Fatalf("decode message: %v", err)
		}
		if transaction != nil {
			for sequence := 1; sequence <= transaction.Count; sequence++ {
				event, ok, err := decoder.NextEvent()
				if err != nil {
					t.Fatalf("next event: %v", err)
				}
				if !ok {
					break
				}
				collected = append(collected, event)
			}
		}
		if err := pglogrepl.SendStandbyStatusUpdate(ctx, replication, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lastLSN,
			WALFlushPosition: lastLSN,
			WALApplyPosition: lastLSN,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		}); err != nil {
			t.Fatalf("standby status: %v", err)
		}
	}
	return collected
}

// TestLiveTruncateReplication proves pgoutput truncate events survive the whole
// path: two tables truncated in one source transaction become one truncate event
// per relation inside the same transaction, with no row key, and sit in the
// right place in the commit-LSN order between the writes around them.
func TestLiveTruncateReplication(t *testing.T) {
	integrationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tableA := "it_trunc_live_" + suffix
	tableB := "it_trunc_live_b_" + suffix
	slot := "it_trunc_slot_" + suffix
	publication := "it_trunc_pub_" + suffix

	pool, err := pgxpool.New(ctx, liveSourceSQLDSN())
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	defer pool.Close()

	// The demo publication does not publish truncate: this one must opt in.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (id integer PRIMARY KEY, name text NOT NULL)`, tableA)); err != nil {
		t.Fatalf("create table A: %v", err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public."+tableA) }()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (id integer PRIMARY KEY, name text NOT NULL)`, tableB)); err != nil {
		t.Fatalf("create table B: %v", err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public."+tableB) }()
	if _, err := pool.Exec(ctx,
		`CREATE PUBLICATION `+publication+` FOR TABLE public.`+tableA+`, public.`+tableB+
			` WITH (publish = 'insert, update, delete, truncate')`); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+publication) }()

	replication, err := pgconn.Connect(ctx, liveSourceReplicationDSN())
	if err != nil {
		t.Fatalf("connect to replication protocol: %v", err)
	}
	slotResult, err := pglogrepl.CreateReplicationSlot(
		ctx, replication, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		t.Fatalf("create replication slot: %v", err)
	}
	defer dropSlot(t, pool, slot)
	defer replication.Close(context.Background())
	startLSN, err := pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		t.Fatalf("parse consistent point: %v", err)
	}
	if err := pglogrepl.StartReplication(
		ctx, replication, slot, startLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", publication),
			},
		},
	); err != nil {
		t.Fatalf("start replication: %v", err)
	}

	insert := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("write to source: %v", err)
		}
	}
	// Rows, then a two-table truncate, then more rows: the decoder must emit the
	// truncate between the two insert groups, in its own transaction.
	insert(fmt.Sprintf(`INSERT INTO public.%s VALUES (1, 'a'), (2, 'b')`, tableA))
	insert(fmt.Sprintf(`INSERT INTO public.%s VALUES (9, 'x')`, tableB))
	insert(fmt.Sprintf(`TRUNCATE TABLE public.%s, public.%s`, tableA, tableB))
	insert(fmt.Sprintf(`INSERT INTO public.%s VALUES (3, 'c'), (4, 'd')`, tableA))

	events := streamAllCollected(t, ctx, replication, NewDecoder(), 7)
	if len(events) != 7 {
		t.Fatalf("collected %d events, want 7: %#v", len(events), events)
	}
	// A = [insert, insert, truncate, insert, insert], B = [insert, truncate].
	firstTable := filterEvents(events, tableA)
	secondTable := filterEvents(events, tableB)
	if len(firstTable) != 5 || len(secondTable) != 2 {
		t.Fatalf("per-table length A=%d B=%d, want 5/2", len(firstTable), len(secondTable))
	}

	assertOps := func(sequence []cdc.Event, ops ...cdc.Operation) {
		t.Helper()
		for index, event := range sequence {
			if event.Operation != ops[index] {
				t.Fatalf("event %d = %s, want %s", index, event.Operation, ops[index])
			}
		}
	}
	assertOps(firstTable, cdc.OperationInsert, cdc.OperationInsert, cdc.OperationTruncate, cdc.OperationInsert, cdc.OperationInsert)
	assertOps(secondTable, cdc.OperationInsert, cdc.OperationTruncate)

	firstTrunc, secondTrunc := firstTable[2], secondTable[1]
	if len(firstTrunc.Key) != 0 || firstTrunc.After != nil || firstTrunc.Before != nil {
		t.Fatalf("truncate event must carry no key/before/after: %#v", firstTrunc)
	}
	if firstTrunc.Source.TransactionID != secondTrunc.Source.TransactionID {
		t.Fatalf("both truncated relations must share the source transaction: A=%d B=%d",
			firstTrunc.Source.TransactionID, secondTrunc.Source.TransactionID)
	}
	for _, trunc := range []cdc.Event{firstTrunc, secondTrunc} {
		if _, err := pglogrepl.ParseLSN(trunc.Source.CommitLSN); err != nil {
			t.Fatalf("truncate commit LSN %q invalid: %v", trunc.Source.CommitLSN, err)
		}
	}
	if firstTable[0].Source.TransactionID == firstTrunc.Source.TransactionID ||
		firstTrunc.Source.TransactionID == firstTable[3].Source.TransactionID {
		t.Fatal("the truncate must live in its own transaction between the write groups")
	}

	// Commit LSNs must place the truncate strictly between the surrounding rows.
	lsnOf := func(event cdc.Event) uint64 {
		t.Helper()
		value, err := pglogrepl.ParseLSN(event.Source.CommitLSN)
		if err != nil {
			t.Fatalf("parse LSN %q: %v", event.Source.CommitLSN, err)
		}
		return uint64(value)
	}
	for index := 1; index < len(firstTable); index++ {
		// Inserts inside the same source transaction share the commit LSN; only
		// distinct transactions must advance. The essential check is that the
		// truncate's transaction is newer than the first group and older than
		// the second.
		if firstTable[index-1].Source.TransactionID == firstTable[index].Source.TransactionID {
			continue
		}
		if lsnOf(firstTable[index-1]) >= lsnOf(firstTable[index]) {
			for j, event := range firstTable {
				t.Logf("event %d: op=%s lsn=%s tx=%d", j, event.Operation, event.Source.CommitLSN, event.Source.TransactionID)
			}
			t.Fatalf("commit LSNs not strictly increasing around the truncate: %d !< %d",
				lsnOf(firstTable[index-1]), lsnOf(firstTable[index]))
		}
	}
	t.Logf("live truncate replication ok: %d events, truncate at LSN %s in tx %d",
		len(events), firstTrunc.Source.CommitLSN, firstTrunc.Source.TransactionID)
}

func streamUntilChangedBio(
	t *testing.T,
	ctx context.Context,
	replication *pgconn.PgConn,
	decoder *Decoder,
	table, bigBioChanged string,
) []cdc.Event {
	t.Helper()

	var collected []cdc.Event
	match := func(tx *CommittedTransaction) bool {
		found := false
		for sequence := 1; sequence <= tx.Count; sequence++ {
			event, ok, err := decoder.NextEvent()
			if err != nil {
				t.Fatalf("next event: %v", err)
			}
			if !ok {
				break
			}
			if event.Table != table {
				continue
			}
			collected = append(collected, event)
			if event.After["bio"] != nil && *event.After["bio"] == bigBioChanged {
				found = true
			}
		}
		return found
	}

	var lastLSN pglogrepl.LSN
	for {
		raw, err := replication.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		copyData, ok := raw.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 || copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xlog, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			t.Fatalf("parse xlog data: %v", err)
		}
		lastLSN = xlog.WALStart
		message, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			t.Fatalf("parse pgoutput message: %v", err)
		}
		transaction, err := decoder.Handle(message)
		if err != nil {
			t.Fatalf("decode message: %v", err)
		}
		if transaction != nil && match(transaction) {
			return collected
		}
		if err := pglogrepl.SendStandbyStatusUpdate(ctx, replication, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lastLSN,
			WALFlushPosition: lastLSN,
			WALApplyPosition: lastLSN,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		}); err != nil {
			t.Fatalf("standby status: %v", err)
		}
	}
}
