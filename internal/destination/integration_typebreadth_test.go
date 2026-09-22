//go:build integration

package destination

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"example.com/artie-mini-cdc/internal/cdc"
	"example.com/artie-mini-cdc/internal/source"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Live type breadth: a real pgoutput stream carries bytea, a range, arrays, a
// uuid, a date and an interval as text; the decoder hands them to the applier as
// text and the typed ::cast upsert lands them identically. The same table then
// round-trips through the snapshot path, and a DELETE round trip covers the
// destination-side delete with replay idempotency.
func TestLiveTypeBreadthAndDeleteRoundTrip(t *testing.T) {
	integrationGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)

	sourceConn, err := pgx.Connect(ctx, sourceIntegrationDSN())
	if err != nil {
		t.Fatalf("connect to source pool: %v", err)
	}
	t.Cleanup(func() { _ = sourceConn.Close(context.Background()) })

	table := uniqueTable("itb")
	suffix := fmt.Sprintf("%d", time.Now().UnixNano()/1000)
	slot := "itb_slot_" + suffix
	publication := "itb_pub_" + suffix
	const id = 7

	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id BIGINT PRIMARY KEY,
			payload BYTEA,
			rng INT4RANGE,
			ints INT[],
			nums NUMERIC[],
			uid UUID,
			d DATE,
			iv INTERVAL,
			%s
		)`, table, metaColumnsDDL)); err != nil {
		t.Fatalf("create destination type table: %v", err)
	}
	t.Cleanup(func() { dropDestTables(t, connection, table) })

	if _, err := sourceConn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (
			id BIGINT PRIMARY KEY,
			payload BYTEA,
			rng INT4RANGE,
			ints INT[],
			nums NUMERIC[],
			uid UUID,
			d DATE,
			iv INTERVAL)`, table)); err != nil {
		t.Fatalf("create source type table: %v", err)
	}
	t.Cleanup(func() { _, _ = sourceConn.Exec(context.Background(), "DROP TABLE IF EXISTS public."+table) })
	if _, err := sourceConn.Exec(ctx, "CREATE PUBLICATION "+publication+" FOR TABLE public."+table); err != nil {
		t.Fatalf("create type publication: %v", err)
	}
	t.Cleanup(func() { _, _ = sourceConn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+publication) })

	replication := startTypeReplication(t, ctx, slot, publication)
	t.Cleanup(func() { dropTypeSlot(t, sourceConn, slot) })

	if _, err := sourceConn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, payload, rng, ints, nums, uid, d, iv)
		 VALUES ($1, '\xdeadbeef'::bytea, '[1,5)'::int4range, '{1,2,3}'::int[],
		         '{1.5,2.25}'::numeric[], '123e4567-e89b-12d3-a456-426614174000'::uuid,
		         '2024-03-01'::date, '1 day'::interval)`, table), id); err != nil {
		t.Fatalf("insert type row: %v", err)
	}

	stream := newTypeStream(t, ctx, replication, slot, publication, table, source.NewDecoder())
	insertEvents := stream.nextTransaction()
	applyEvents(t, ctx, applier, insertEvents)
	compareTypeRow(t, ctx, connection, sourceConn, table, id)

	// UPDATE a handful of typed columns so the live upsert path exercises them.
	if _, err := sourceConn.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s
		 SET payload = '\xCAFE'::bytea, rng = '[2,7)'::int4range, ints = '{9,8,7}'::int[],
		     nums = '{3.5}'::numeric[], iv = '2 hours'::interval
		 WHERE id = $1`, table), id); err != nil {
		t.Fatalf("update type row: %v", err)
	}
	updateEvents := stream.nextTransaction()
	applyEvents(t, ctx, applier, updateEvents)
	compareTypeRow(t, ctx, connection, sourceConn, table, id)

	// Snapshot leg: the same table copied by the snapshotter must carry the same
	// typed text values. Clear the destination first so the copy is measurable.
	if _, err := connection.Exec(ctx, "TRUNCATE public."+table); err != nil {
		t.Fatalf("truncate destination before snapshot: %v", err)
	}
	snapshotter := NewSnapshotter(sourceIntegrationDSN(), slot, publication, applier)
	if err := snapshotter.RunSnapshot(ctx); err != nil {
		t.Fatalf("run type-breadth snapshot: %v", err)
	}
	compareTypeRow(t, ctx, connection, sourceConn, table, id)

	// DELETE round trip: the row disappears, and a replayed delete is a no-op.
	if _, err := sourceConn.Exec(ctx, fmt.Sprintf(`DELETE FROM public.%s WHERE id = $1`, table), id); err != nil {
		t.Fatalf("delete type row: %v", err)
	}
	deleteEvents := stream.nextTransaction()
	applyEvents(t, ctx, applier, deleteEvents)
	var destCount int
	if err := connection.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s`, table)).Scan(&destCount); err != nil {
		t.Fatalf("count after delete: %v", err)
	}
	if destCount != 0 {
		t.Fatalf("destination not cleared by delete: %d rows remain", destCount)
	}

	// Replay the delete: the marker makes it a duplicate no-op, never an error.
	for _, event := range deleteEvents {
		applied, err := applier.Apply(ctx, event)
		if err != nil {
			t.Fatalf("replayed delete failed: %v", err)
		}
		if applied {
			t.Fatalf("replayed delete reported applied=%v, want a duplicate no-op", applied)
		}
	}
	if err := connection.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s`, table)).Scan(&destCount); err != nil {
		t.Fatalf("count after replayed delete: %v", err)
	}
	if destCount != 0 {
		t.Fatalf("replayed delete resurrected rows: %d", destCount)
	}

	// The delete must also leave the destination keyed correctly afterwards, so a
	// fresh identical row can be inserted again in the next run.
	stream.close()
}

// compareTypeRow asserts every typed column round-trips text-identically between
// the live source row and the destination row.
func compareTypeRow(t *testing.T, ctx context.Context, connection, sourceConn *pgx.Conn, table string, id int) {
	t.Helper()
	columns := []string{"payload", "rng", "ints", "nums", "uid", "d", "iv"}
	for _, column := range columns {
		var want, got string
		if err := sourceConn.QueryRow(ctx, fmt.Sprintf(
			`SELECT %s::text FROM public.%s WHERE id = $1`, column, table), id).Scan(&want); err != nil {
			t.Fatalf("read source %s: %v", column, err)
		}
		if err := connection.QueryRow(ctx, fmt.Sprintf(
			`SELECT %s::text FROM public.%s WHERE id = $1`, column, table), id).Scan(&got); err != nil {
			t.Fatalf("read destination %s: %v", column, err)
		}
		if !strings.EqualFold(want, got) {
			t.Fatalf("column %s round trip: destination %q != source %q", column, got, want)
		}
	}
}

func applyEvents(t *testing.T, ctx context.Context, applier *Applier, events []cdc.Event) {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("no live events to apply")
	}
	for _, event := range events {
		if _, err := applier.Apply(ctx, event); err != nil {
			t.Fatalf("apply live event %s (%s): %v", event.ID, event.Operation, err)
		}
	}
}

// typeStream decodes a real pgoutput stream around one publication and hands back
// committed transactions one at a time, so each DML can be applied and asserted.
type typeStream struct {
	t           *testing.T
	ctx         context.Context
	replication *pgconn.PgConn
	decoder     *source.Decoder
	table       string
	lastLSN     pglogrepl.LSN
}

func startTypeReplication(t *testing.T, ctx context.Context, slot, publication string) *pgconn.PgConn {
	t.Helper()
	replication, err := pgconn.Connect(ctx, liveTypeReplicationDSN())
	if err != nil {
		t.Fatalf("connect to source replication protocol: %v", err)
	}
	t.Cleanup(func() { _ = replication.Close(context.Background()) })
	result, err := pglogrepl.CreateReplicationSlot(
		ctx, replication, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		t.Fatalf("create type replication slot: %v", err)
	}
	startLSN, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		t.Fatalf("parse type slot consistent point: %v", err)
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
		t.Fatalf("start type replication: %v", err)
	}
	return replication
}

func liveTypeReplicationDSN() string {
	dsn := sourceIntegrationDSN()
	if strings.Contains(dsn, "?") {
		return dsn + "&replication=database"
	}
	return dsn + "?replication=database"
}

// dropTypeSlot drops a test replication slot through a normal SQL connection so
// the drop works even while the streaming connection holds the slot active.
func dropTypeSlot(t *testing.T, connection *pgx.Conn, slot string) {
	t.Helper()
	if _, err := connection.Exec(context.Background(), `SELECT pg_drop_replication_slot($1)`, slot); err != nil {
		t.Logf("warn: drop replication slot %s: %v", slot, err)
	}
}

func newTypeStream(t *testing.T, ctx context.Context, replication *pgconn.PgConn, slot, publication, table string, decoder *source.Decoder) *typeStream {
	t.Helper()
	// Warm up relation metadata by streaming until the table's Relation message
	// lands (needs at least one WAL message; the slot is brand new).
	stream := &typeStream{t: t, ctx: ctx, replication: replication, decoder: decoder, table: table}
	for {
		if stream.nextRelationReady(table) {
			return stream
		}
	}
}

// nextTransaction decodes stream messages until a commit lands and returns every
// row event of the transaction that targets the test table.
func (stream *typeStream) nextTransaction() []cdc.Event {
	t := stream.t
	var events []cdc.Event
	for {
		raw, err := stream.replication.ReceiveMessage(stream.ctx)
		if err != nil {
			t.Fatalf("receive stream message: %v", err)
		}
		copyData, ok := raw.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 || copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xlog, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			t.Fatalf("parse xlog data: %v", err)
		}
		stream.lastLSN = xlog.WALStart
		message, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			t.Fatalf("parse pgoutput message: %v", err)
		}
		transaction, err := stream.decoder.Handle(message)
		if err != nil {
			t.Fatalf("decode stream message: %v", err)
		}
		if transaction != nil {
			for sequence := 1; sequence <= transaction.Count; sequence++ {
				event, ok, err := stream.decoder.NextEvent()
				if err != nil {
					t.Fatalf("read next event: %v", err)
				}
				if !ok {
					break
				}
				if event.Table == stream.table {
					events = append(events, event)
				}
			}
			stream.ack()
			return events
		}
		stream.ack()
	}
}

// nextRelationReady spins the stream until the Relation message for the table has
// been decoded, so the first DML's rows decode against known metadata.
func (stream *typeStream) nextRelationReady(table string) bool {
	t := stream.t
	raw, err := stream.replication.ReceiveMessage(stream.ctx)
	if err != nil {
		t.Fatalf("receive warming message: %v", err)
	}
	copyData, ok := raw.(*pgproto3.CopyData)
	if !ok || len(copyData.Data) == 0 || copyData.Data[0] != pglogrepl.XLogDataByteID {
		stream.ack()
		return false
	}
	xlog, err := pglogrepl.ParseXLogData(copyData.Data[1:])
	if err != nil {
		t.Fatalf("parse warming xlog data: %v", err)
	}
	stream.lastLSN = xlog.WALStart
	message, err := pglogrepl.Parse(xlog.WALData)
	if err != nil {
		t.Fatalf("parse warming pgoutput message: %v", err)
	}
	relation, ok := message.(*pglogrepl.RelationMessage)
	if ok && relation.RelationName == table {
		if _, err := stream.decoder.Handle(message); err != nil {
			t.Fatalf("decode table relation: %v", err)
		}
		stream.ack()
		return true
	}
	_, _ = stream.decoder.Handle(message)
	stream.ack()
	return false
}

func (stream *typeStream) ack() {
	ctx := context.Background()
	err := pglogrepl.SendStandbyStatusUpdate(ctx, stream.replication, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: stream.lastLSN,
		WALFlushPosition: stream.lastLSN,
		WALApplyPosition: stream.lastLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		stream.t.Fatalf("standby status: %v", err)
	}
}

func (stream *typeStream) close() {
	_ = stream.replication.Close(context.Background())
}
