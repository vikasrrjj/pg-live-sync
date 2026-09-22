package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"example.com/pg-live-sync/internal/cdc"
	"example.com/pg-live-sync/internal/errclass"
	"example.com/pg-live-sync/internal/transport"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedColumns are the metadata columns every destination table must carry.
// They let the pipeline refuse stale events and expose provenance.
var reservedColumns = map[string]bool{
	"source_lsn":         true,
	"source_commit_time": true,
	"replicated_at":      true,
}

const (
	metaSourceLSN        = "source_lsn"
	metaSourceCommitTime = "source_commit_time"
	metaReplicatedAt     = "replicated_at"
)

type Applier struct {
	connection *pgx.Conn

	// columnsMu guards the destination column-type cache. The cache answers
	// "what casts can the destination accept" so arrays, enums, numerics and
	// friends are coerced on the wire instead of being sent as bare text.
	columnsMu      sync.Mutex
	columnsSeenAt  map[string]time.Time
	columnsByTable map[string][]columnDef
}

func NewApplier(connection *pgx.Conn) *Applier {
	return &Applier{
		connection:     connection,
		columnsSeenAt:  make(map[string]time.Time),
		columnsByTable: make(map[string][]columnDef),
	}
}

// columnDef is one non-dropped column of a destination table together with its
// PostgreSQL type, expressed with format_type so it can be used directly as a
// CAST, e.g. "bigint", "text[]", "public.mood", "timestamp with time zone".
type columnDef struct {
	name     string
	dataType string
}

// columnCacheTTL bounds how long a column-type snapshot is trusted before the
// destination catalog is re-read, so out-of-process ALTERs pick up on their own.
const columnCacheTTL = 2 * time.Minute

// tableColumns returns the destination table's columns keyed by name with their
// exact types. Errors (missing table etc.) are not cached, so a repair that
// creates the table recovers on the next attempt.
func (applier *Applier) tableColumns(ctx context.Context, schema, table string) (map[string]string, error) {
	qualified := quoteIdent(schema) + "." + quoteIdent(table)

	applier.columnsMu.Lock()
	if fetched, ok := applier.columnsSeenAt[qualified]; ok && time.Since(fetched) < columnCacheTTL {
		types := make(map[string]string, len(applier.columnsByTable[qualified]))
		for _, def := range applier.columnsByTable[qualified] {
			types[def.name] = def.dataType
		}
		applier.columnsMu.Unlock()
		return types, nil
	}
	applier.columnsSeenAt[qualified] = time.Now()
	applier.columnsMu.Unlock()

	rows, err := applier.connection.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)::text
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.oid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, qualified)
	if err != nil {
		return nil, fmt.Errorf("list columns of destination %s: %w", qualified, err)
	}
	defer rows.Close()

	defs := make([]columnDef, 0)
	for rows.Next() {
		var def columnDef
		if err := rows.Scan(&def.name, &def.dataType); err != nil {
			return nil, fmt.Errorf("scan column of destination %s: %w", qualified, err)
		}
		defs = append(defs, def)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns of destination %s: %w", qualified, err)
	}

	applier.columnsMu.Lock()
	if len(defs) == 0 {
		delete(applier.columnsSeenAt, qualified)
	} else {
		applier.columnsByTable[qualified] = defs
	}
	applier.columnsMu.Unlock()

	types := make(map[string]string, len(defs))
	for _, def := range defs {
		types[def.name] = def.dataType
	}
	return types, nil
}

// invalidateColumns drops a destination table's cached column types so the next
// write re-reads the catalog. The writer calls it after a schema repair.
func (applier *Applier) invalidateColumns(schema, table string) {
	qualified := quoteIdent(schema) + "." + quoteIdent(table)
	applier.columnsMu.Lock()
	defer applier.columnsMu.Unlock()
	delete(applier.columnsSeenAt, qualified)
	delete(applier.columnsByTable, qualified)
}

// castFor turns a resolved destination column type into a SQL cast suffix.
// An empty type means the value is sent without a cast (PostgreSQL infers).
func castFor(dataType string) string {
	if dataType == "" {
		return ""
	}
	return "::" + dataType
}

// EnsureMetaSchema creates the coordination tables the writer needs on a
// destination database that was set up before them.
func (applier *Applier) EnsureMetaSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS public.cdc_applied_events (
			event_id TEXT PRIMARY KEY,
			source_lsn PG_LSN NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS public.cdc_dead_letters (
			event_id TEXT PRIMARY KEY,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			operation TEXT NOT NULL,
			source_lsn TEXT NOT NULL,
			source_commit_time TIMESTAMPTZ NOT NULL,
			key JSONB,
			before JSONB,
			after JSONB,
			unchanged_columns JSONB,
			reason TEXT NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS public.cdc_table_watermarks (
			table_name TEXT PRIMARY KEY,
			watermark_lsn PG_LSN NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS public.cdc_state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	}
	for _, statement := range statements {
		if _, err := applier.connection.Exec(ctx, statement); err != nil {
			return fmt.Errorf("ensure destination metadata schema: %w", err)
		}
	}
	// Idempotent migrations for databases that created cdc_dead_letters before
	// the replay fields existed. Replayed dead letters must carry the columns
	// the apply path needs (preserved TOAST columns, and the before row for
	// REPLICA IDENTITY FULL updates).
	for _, migration := range []string{
		`ALTER TABLE public.cdc_dead_letters ADD COLUMN IF NOT EXISTS before JSONB`,
		`ALTER TABLE public.cdc_dead_letters ADD COLUMN IF NOT EXISTS unchanged_columns JSONB`,
	} {
		if _, err := applier.connection.Exec(ctx, migration); err != nil {
			return fmt.Errorf("migrate destination metadata schema: %w", err)
		}
	}
	return nil
}

// Apply applies one event in its own destination transaction. The writer uses it
// for the whole-transaction fallback after a group application hit a poison event.
func (applier *Applier) Apply(ctx context.Context, event cdc.Event) (bool, error) {
	if err := event.Validate(); err != nil {
		return false, err
	}
	transaction, err := applier.connection.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin destination transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	applied, err := applier.ApplyInTx(ctx, transaction, event)
	if err != nil {
		return false, err
	}
	if err := transaction.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit destination transaction: %w", err)
	}
	return applied, nil
}

// ApplyInTx writes the deduplication marker and the row change in the caller's
// transaction, so they commit atomically. A replayed event becomes a harmless
// no-op: the marker conflicts, nothing else is written, and (false, nil) returns.
// A permanent error is wrapped so the writer can quarantine the event.
func (applier *Applier) ApplyInTx(ctx context.Context, transaction pgx.Tx, event cdc.Event) (bool, error) {
	if err := event.Validate(); err != nil {
		return false, err
	}
	if err := verifyIdent(event.Schema); err != nil {
		return false, permanent("event %s: %v", event.ID, err)
	}
	if err := verifyIdent(event.Table); err != nil {
		return false, permanent("event %s: %v", event.ID, err)
	}

	if event.Operation == cdc.OperationTruncate {
		return applier.applyTruncateInTx(ctx, transaction, event)
	}

	applied, err := markApplied(ctx, transaction, event.ID, event.Source.CommitLSN)
	if err != nil {
		return false, err
	}
	if !applied {
		return false, nil
	}
	if err := applier.applyMutation(ctx, transaction, event); err != nil {
		return false, err
	}
	return true, nil
}

func (applier *Applier) applyMutation(ctx context.Context, transaction pgx.Tx, event cdc.Event) error {
	if err := verifyColumns(event); err != nil {
		return permanent("event %s: %v", event.ID, err)
	}

	table := quoteIdent(event.Schema) + "." + quoteIdent(event.Table)
	dataTypes, err := applier.tableColumns(ctx, event.Schema, event.Table)
	if err != nil {
		return err
	}
	switch event.Operation {
	case cdc.OperationInsert, cdc.OperationUpdate:
		columns, values, err := transcodeRow(event.After)
		if err != nil {
			return permanent("event %s: %v", event.ID, err)
		}
		keyColumns, keyValues, err := transcodeRow(event.Key)
		if err != nil {
			return permanent("event %s: %v", event.ID, err)
		}
		for _, column := range keyColumns {
			if !containsString(columns, column) {
				return permanent("event %s: primary-key column %q is absent from the change row", event.ID, column)
			}
		}
		// Unchanged TOAST columns were deliberately left out of After by the
		// decoder so this upsert preserves the destination value for them. A
		// primary-key column can never be unchanged: the WHERE/conflict target
		// has to know it, and an unchanged key would make the change un-keyable.
		for _, column := range keyColumns {
			if containsString(event.UnchangedColumns, column) {
				return permanent("event %s: primary-key column %q is marked unchanged", event.ID, column)
			}
		}
		parameters := appendNumbered(values, event.Source.CommitLSN, event.Source.CommitTime)
		description := fmt.Sprintf("upsert event %s on %s.%s", event.ID, event.Schema, event.Table)
		if len(event.UnchangedColumns) > 0 {
			// An upsert builds a candidate INSERT row for the conflict target,
			// whose unchanged columns would fall to their defaults (often NULL
			// and NOT NULL). Only the existing row is a truthful source for
			// those values, so preservation is a guarded UPDATE first and an
			// INSERT only for a row the destination has never seen.
			return applyUpdatePreserving(ctx, transaction, table, columns, keyColumns, dataTypes, parameters, keyValues, description)
		}
		return execWithPoison(ctx, transaction,
			buildUpsertSQL(table, columns, keyColumns, dataTypes),
			parameters,
			description,
		)

	case cdc.OperationDelete:
		keyColumns, keyValues, err := transcodeRow(event.Key)
		if err != nil {
			return permanent("event %s: %v", event.ID, err)
		}
		parameters := append(keyValues, event.Source.CommitLSN)
		return execWithPoison(ctx, transaction,
			buildDeleteSQL(table, keyColumns, dataTypes),
			parameters,
			fmt.Sprintf("delete event %s on %s.%s", event.ID, event.Schema, event.Table),
		)
	}
	return nil
}

// UpsertSnapshotRow applies a row captured by the initial snapshot exactly like
// an insert event whose commit LSN is the snapshot's consistent point.
func (applier *Applier) UpsertSnapshotRow(
	ctx context.Context,
	transaction pgx.Tx,
	schema, table string,
	after cdc.Row,
	key cdc.Row,
	snapshotLSN string,
	commitTime time.Time,
) error {
	if err := verifyIdent(schema); err != nil {
		return permanent("snapshot row %s.%s: %v", schema, table, err)
	}
	if err := verifyIdent(table); err != nil {
		return permanent("snapshot row %s.%s: %v", schema, table, err)
	}
	columns, values, err := transcodeRow(after)
	if err != nil {
		return permanent("snapshot row %s.%s: %v", schema, table, err)
	}
	keyColumns, _, err := transcodeRow(key)
	if err != nil {
		return permanent("snapshot row %s.%s: %v", schema, table, err)
	}
	for _, column := range keyColumns {
		if !containsString(columns, column) {
			return permanent("snapshot row %s.%s: primary-key column %q is absent from the row", schema, table, column)
		}
	}
	qualified := quoteIdent(schema) + "." + quoteIdent(table)
	dataTypes, err := applier.tableColumns(ctx, schema, table)
	if err != nil {
		return err
	}
	parameters := appendNumbered(values, snapshotLSN, commitTime)
	return execWithPoison(ctx, transaction,
		buildUpsertSQL(qualified, columns, keyColumns, dataTypes),
		parameters,
		fmt.Sprintf("snapshot upsert on %s.%s", schema, table),
	)
}

// BatchError identifies which event inside an ApplyBatch caused a poison or
// permanent failure, so the caller can quarantine and replay the rest.
type BatchError struct {
	Index int
	Event cdc.Event
	Err   error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("event %s (index %d): %v", e.Event.ID, e.Index, e.Err)
}
func (e *BatchError) Unwrap() error { return e.Err }

// ApplyBatch pipelines a whole source transaction inside one already-open
// destination transaction with two pgx.Batch phases:
//
//  1. every event's dedupe marker (ON CONFLICT DO NOTHING),
//  2. mutations for only the events whose markers landed, in source order.
//
// Truncates and unchanged-column updates that require conditional execution
// flush the mutation batch before running, preserving exact source order.
// The first error aborts the batch and returns a *BatchError so the caller can
// rollback the transaction, replay non-poison events, and quarantine the rest.
func (applier *Applier) ApplyBatch(ctx context.Context, tx pgx.Tx, events []cdc.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Phase 1: marker batch.
	markerBatch := &pgx.Batch{}
	for i, event := range events {
		if err := event.Validate(); err != nil {
			return &BatchError{Index: i, Event: event, Err: err}
		}
		if err := verifyIdent(event.Schema); err != nil {
			return &BatchError{Index: i, Event: event, Err: permanent("event %s: %v", event.ID, err)}
		}
		if err := verifyIdent(event.Table); err != nil {
			return &BatchError{Index: i, Event: event, Err: permanent("event %s: %v", event.ID, err)}
		}
		markerBatch.Queue(
			`INSERT INTO public.cdc_applied_events (event_id, source_lsn)
			VALUES ($1, $2::pg_lsn) ON CONFLICT (event_id) DO NOTHING`,
			event.ID, event.Source.CommitLSN,
		)
	}
	markerResults := tx.SendBatch(ctx, markerBatch)
	fresh := make([]bool, len(events))
	for i, event := range events {
		tag, err := markerResults.Exec()
		if err != nil {
			_ = markerResults.Close()
			return &BatchError{Index: i, Event: event, Err: poisonOrWrap(err, "record applied event "+event.ID)}
		}
		fresh[i] = tag.RowsAffected() == 1
	}
	if err := markerResults.Close(); err != nil {
		return fmt.Errorf("flush marker batch: %w", err)
	}

	// Phase 2: ordered mutation batch with conditional flushes.
	type queuedExec struct {
		index int
		event cdc.Event
	}
	var mutationBatch *pgx.Batch
	var queued []queuedExec

	flush := func() error {
		if mutationBatch == nil || mutationBatch.Len() == 0 {
			return nil
		}
		results := tx.SendBatch(ctx, mutationBatch)
		for _, item := range queued {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return &BatchError{
					Index: item.index,
					Event: item.event,
					Err:   poisonOrWrap(err, fmt.Sprintf("mutation for event %s on %s.%s", item.event.ID, item.event.Schema, item.event.Table)),
				}
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("flush mutation batch: %w", err)
		}
		mutationBatch = nil
		queued = queued[:0]
		return nil
	}

	for i, event := range events {
		if !fresh[i] {
			continue
		}

		switch event.Operation {
		case cdc.OperationTruncate:
			if err := flush(); err != nil {
				return err
			}
			if err := applier.applyTruncateBatched(ctx, tx, event); err != nil {
				return &BatchError{Index: i, Event: event, Err: err}
			}
			continue

		case cdc.OperationDelete:
			if err := verifyColumns(event); err != nil {
				return &BatchError{Index: i, Event: event, Err: permanent("event %s: %v", event.ID, err)}
			}
			table := quoteIdent(event.Schema) + "." + quoteIdent(event.Table)
			dataTypes, err := applier.tableColumns(ctx, event.Schema, event.Table)
			if err != nil {
				return &BatchError{Index: i, Event: event, Err: err}
			}
			keyColumns, keyValues, err := transcodeRow(event.Key)
			if err != nil {
				return &BatchError{Index: i, Event: event, Err: permanent("event %s: %v", event.ID, err)}
			}
			parameters := append(keyValues, event.Source.CommitLSN)
			if mutationBatch == nil {
				mutationBatch = &pgx.Batch{}
			}
			mutationBatch.Queue(buildDeleteSQL(table, keyColumns, dataTypes), parameters...)
			queued = append(queued, queuedExec{index: i, event: event})
			continue

		case cdc.OperationInsert, cdc.OperationUpdate:
			if err := verifyColumns(event); err != nil {
				return &BatchError{Index: i, Event: event, Err: permanent("event %s: %v", event.ID, err)}
			}
			table := quoteIdent(event.Schema) + "." + quoteIdent(event.Table)
			dataTypes, err := applier.tableColumns(ctx, event.Schema, event.Table)
			if err != nil {
				return &BatchError{Index: i, Event: event, Err: err}
			}
			columns, values, err := transcodeRow(event.After)
			if err != nil {
				return &BatchError{Index: i, Event: event, Err: permanent("event %s: %v", event.ID, err)}
			}
			keyColumns, keyValues, err := transcodeRow(event.Key)
			if err != nil {
				return &BatchError{Index: i, Event: event, Err: permanent("event %s: %v", event.ID, err)}
			}
			for _, column := range keyColumns {
				if !containsString(columns, column) {
					return &BatchError{Index: i, Event: event, Err: permanent("event %s: primary-key column %q is absent from the change row", event.ID, column)}
				}
			}
			for _, column := range keyColumns {
				if containsString(event.UnchangedColumns, column) {
					return &BatchError{Index: i, Event: event, Err: permanent("event %s: primary-key column %q is marked unchanged", event.ID, column)}
				}
			}
			parameters := appendNumbered(values, event.Source.CommitLSN, event.Source.CommitTime)

			if len(event.UnchangedColumns) > 0 {
				// Unchanged-column update requires a conditional UPDATE + probe
				// that cannot be unconditionally batched. Flush, apply, continue.
				if err := flush(); err != nil {
					return err
				}
				description := fmt.Sprintf("upsert event %s on %s.%s", event.ID, event.Schema, event.Table)
				if err := applyUpdatePreserving(ctx, tx, table, columns, keyColumns, dataTypes, parameters, keyValues, description); err != nil {
					return &BatchError{Index: i, Event: event, Err: err}
				}
				continue
			}

			if mutationBatch == nil {
				mutationBatch = &pgx.Batch{}
			}
			mutationBatch.Queue(buildUpsertSQL(table, columns, keyColumns, dataTypes), parameters...)
			queued = append(queued, queuedExec{index: i, event: event})
		}
	}
	return flush()
}

// applyTruncateBatched applies the non-marker steps of a truncate event: watermark
// guard, TRUNCATE TABLE, watermark upsert. The marker was already written in
// phase 1 by ApplyBatch, so this skips markApplied.
func (applier *Applier) applyTruncateBatched(ctx context.Context, tx pgx.Tx, event cdc.Event) error {
	eventLSN, err := pglogrepl.ParseLSN(event.Source.CommitLSN)
	if err != nil {
		return permanent("event %s: invalid commit LSN %q", event.ID, event.Source.CommitLSN)
	}
	table := quoteIdent(event.Schema) + "." + quoteIdent(event.Table)
	watermark, err := tableWatermark(ctx, tx, table)
	if err != nil {
		return err
	}
	if truncateIsStale(eventLSN, watermark) {
		return nil
	}
	if _, err := tx.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
		return poisonOrWrap(err, "truncate "+table)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO public.cdc_table_watermarks (table_name, watermark_lsn)
		VALUES ($1, $2::pg_lsn)
		ON CONFLICT (table_name) DO UPDATE SET watermark_lsn = EXCLUDED.watermark_lsn, updated_at = now()`,
		table, event.Source.CommitLSN); err != nil {
		return poisonOrWrap(err, "advance truncate watermark for "+table)
	}
	return nil
}

// RecordDeadLetter quarantines a poison event: it records the marker so the event
// is never attempted again and stashes a copy for investigation, atomically.
func (applier *Applier) RecordDeadLetter(ctx context.Context, event cdc.Event, reason string) error {
	transaction, err := applier.connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dead-letter transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	if _, err := markApplied(ctx, transaction, event.ID, event.Source.CommitLSN); err != nil {
		return err
	}
	key, err := json.Marshal(event.Key)
	if err != nil {
		return fmt.Errorf("marshal dead-letter key: %w", err)
	}
	before, err := json.Marshal(event.Before)
	if err != nil {
		return fmt.Errorf("marshal dead-letter before row: %w", err)
	}
	after, err := json.Marshal(event.After)
	if err != nil {
		return fmt.Errorf("marshal dead-letter value: %w", err)
	}
	unchangedColumns, err := json.Marshal(event.UnchangedColumns)
	if err != nil {
		return fmt.Errorf("marshal dead-letter unchanged columns: %w", err)
	}
	_, err = transaction.Exec(ctx, `
		INSERT INTO public.cdc_dead_letters (
			event_id, schema_name, table_name, operation,
			source_lsn, source_commit_time, key, before, after, unchanged_columns, reason
		) VALUES ($1, $2, $3, $4,
			COALESCE($5::text, ''), $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11)
		ON CONFLICT (event_id) DO UPDATE SET reason = EXCLUDED.reason`,
		event.ID, event.Schema, event.Table, event.Operation,
		event.Source.CommitLSN, event.Source.CommitTime,
		string(key), string(before), string(after), string(unchangedColumns), reason,
	)
	if err != nil {
		return fmt.Errorf("insert dead-letter record: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit dead-letter transaction: %w", err)
	}
	return nil
}

// markApplied returns true when this event is new. The marker and the row change
// live in the same transaction, so a commit implies both happened and a rollback
// forgets both.
func markApplied(ctx context.Context, transaction pgx.Tx, eventID, commitLSN string) (bool, error) {
	var insertedID string
	err := transaction.QueryRow(ctx, `
		INSERT INTO public.cdc_applied_events (event_id, source_lsn)
		VALUES ($1, $2::pg_lsn)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING event_id`, eventID, commitLSN).Scan(&insertedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record applied event %s: %w", eventID, err)
	}
	return true, nil
}

// applyTruncateInTx applies a TRUNCATE event inside the already-open destination
// transaction, so it stays atomic with every other change of the source
// transaction (including truncates of other relations in the same message).
//
// Durability is provided by a per-table watermark, never by MAX(row_lsn): a
// successfully truncated table may contain zero rows. The watermark is read,
// the cleared rows written, and the watermark advanced inside the same
// transaction as the applied-event marker, so a commit implies the truncate
// happened at or beyond this LSN and a stale or replayed truncate is a no-op.
func (applier *Applier) applyTruncateInTx(ctx context.Context, transaction pgx.Tx, event cdc.Event) (bool, error) {
	applied, err := markApplied(ctx, transaction, event.ID, event.Source.CommitLSN)
	if err != nil {
		return false, err
	}
	if !applied {
		return false, nil
	}

	eventLSN, err := pglogrepl.ParseLSN(event.Source.CommitLSN)
	if err != nil {
		return false, permanent("event %s: invalid commit LSN %q", event.ID, event.Source.CommitLSN)
	}

	table := quoteIdent(event.Schema) + "." + quoteIdent(event.Table)
	watermark, err := tableWatermark(ctx, transaction, table)
	if err != nil {
		return false, err
	}
	if truncateIsStale(eventLSN, watermark) {
		// A replayed or out-of-order truncate: the table was already cleared at
		// or after this LSN. Clearing again would destroy rows written later.
		return false, nil
	}

	if _, err := transaction.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
		return false, poisonOrWrap(err, "truncate "+table)
	}
	if _, err := transaction.Exec(ctx, `
		INSERT INTO public.cdc_table_watermarks (table_name, watermark_lsn)
		VALUES ($1, $2::pg_lsn)
		ON CONFLICT (table_name) DO UPDATE SET watermark_lsn = EXCLUDED.watermark_lsn, updated_at = now()`,
		table, event.Source.CommitLSN); err != nil {
		return false, poisonOrWrap(err, "advance truncate watermark for "+table)
	}
	return true, nil
}

// tableWatermark returns the durable truncate watermark of a destination table,
// or LSN 0 when the table was never truncated through the pipeline.
func tableWatermark(ctx context.Context, transaction pgx.Tx, table string) (pglogrepl.LSN, error) {
	var watermarkText string
	err := transaction.QueryRow(ctx,
		`SELECT watermark_lsn::text FROM public.cdc_table_watermarks WHERE table_name = $1`, table).Scan(&watermarkText)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read truncate watermark for %s: %w", table, err)
	}
	watermark, err := pglogrepl.ParseLSN(watermarkText)
	if err != nil {
		return 0, permanent("corrupted truncate watermark %q for %s: %v", watermarkText, table, err)
	}
	return watermark, nil
}

// truncateIsStale reports whether a truncate at eventLSN must be a no-op because
// the table is already known to have been cleared at or after that LSN.
func truncateIsStale(eventLSN, watermark pglogrepl.LSN) bool {
	return watermark != 0 && eventLSN <= watermark
}

func execWithPoison(ctx context.Context, transaction pgx.Tx, statement string, parameters []any, description string) error {
	if _, err := transaction.Exec(ctx, statement, parameters...); err != nil {
		return poisonOrWrap(err, description)
	}
	return nil
}

func verifyColumns(event cdc.Event) error {
	for column := range event.After {
		if reservedColumns[column] {
			return fmt.Errorf("column %q collides with a reserved pipeline column", column)
		}
	}
	for column := range event.Key {
		if reservedColumns[column] {
			return fmt.Errorf("column %q collides with a reserved pipeline column", column)
		}
	}
	return nil
}

// transcodeRow turns a cdc.Row into a deterministic, sorted column list and the
// matching parameter values. A nil value becomes SQL NULL; other values are sent
// as text and coerced by PostgreSQL to the destination column type.
func transcodeRow(row cdc.Row) ([]string, []any, error) {
	if len(row) == 0 {
		return nil, nil, fmt.Errorf("row has no columns")
	}
	columns := make([]string, 0, len(row))
	for column := range row {
		if err := verifyIdent(column); err != nil {
			return nil, nil, err
		}
		if reservedColumns[column] {
			return nil, nil, fmt.Errorf("column %q collides with a reserved pipeline column", column)
		}
		columns = append(columns, column)
	}
	sort.Strings(columns)
	values := make([]any, 0, len(columns))
	for _, column := range columns {
		values = append(values, row[column])
	}
	return columns, values, nil
}

func appendNumbered(values []any, lsn string, commitTime time.Time) []any {
	return append(values, lsn, commitTime)
}

// buildUpdateSQL builds a guarded UPDATE. Parameter layout: 1..n user columns,
// n+1 the commit LSN (::pg_lsn), n+2 the commit time, n+3..n+2+k the key values.
func buildUpdateSQL(table string, columns, keyColumns []string, dataTypes map[string]string) string {
	var builder strings.Builder
	builder.WriteString("UPDATE ")
	builder.WriteString(table)
	builder.WriteString(" SET ")
	for index, column := range columns {
		if index > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "%s = $%d%s", quoteIdent(column), index+1, castFor(dataTypes[column]))
	}
	fmt.Fprintf(&builder,
		", %s = $%d::pg_lsn, %s = $%d, %s = now()",
		metaSourceLSN, len(columns)+1, metaSourceCommitTime, len(columns)+2, metaReplicatedAt)
	builder.WriteString(" WHERE ")
	for index, column := range keyColumns {
		if index > 0 {
			builder.WriteString(" AND ")
		}
		fmt.Fprintf(&builder, "%s = $%d%s", quoteIdent(column), len(columns)+3+index, castFor(dataTypes[column]))
	}
	writeLSNGuard(&builder, table, len(columns)+1)
	return builder.String()
}

// writeLSNGuard appends " AND (t.source_lsn IS NULL OR t.source_lsn <= $n::pg_lsn)"
// with $n the LSN parameter. Events at or newer than the guard win; events older
// than the row's recorded LSN are no-ops.
func writeLSNGuard(builder *strings.Builder, table string, lsnParameter int) {
	fmt.Fprintf(builder,
		" AND (%s.%s IS NULL OR %s.%s <= $%d::pg_lsn)",
		table, metaSourceLSN, table, metaSourceLSN, lsnParameter)
}

// buildInsertSQL builds a guarded plain INSERT (no conflict target; it is only
// used when the destination has no row for the key at all). The guard field is
// written without a predicate so the first insert always lands; parameter layout
// matches the upsert: 1..n user columns, n+1 the commit LSN, n+2 the commit time.
func buildInsertSQL(table string, columns []string, dataTypes map[string]string) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(table)
	builder.WriteString(" (")
	writeColumns(&builder, columns)
	fmt.Fprintf(&builder, ", %s, %s, %s", metaSourceLSN, metaSourceCommitTime, metaReplicatedAt)
	builder.WriteString(") VALUES (")
	for index, column := range columns {
		if index > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "$%d%s", index+1, castFor(dataTypes[column]))
	}
	fmt.Fprintf(&builder, ", $%d::pg_lsn, $%d, now())",
		len(columns)+1, len(columns)+2)
	return builder.String()
}

// applyUpdatePreserving is the preservation path for events that carry unchanged
// TOAST columns: a guarded UPDATE (the destination row is the truthful owner of
// the unchanged values) and an INSERT only for a key the destination has never
// seen. An UPDATE that affects no row is followed by a probe: if the key exists,
// the event is a stale replay and no-op is correct; if it is absent, the row is
// created with its defaults. This deliberately avoids the upsert's candidate
// INSERT row, which would fabricate NULL for a NOT NULL unchanged column.
func applyUpdatePreserving(
	ctx context.Context,
	transaction pgx.Tx,
	table string,
	columns, keyColumns []string,
	dataTypes map[string]string,
	parameters []any,
	keyValues []any,
	description string,
) error {
	update := buildUpdateSQL(table, columns, keyColumns, dataTypes)
	if _, err := transaction.Exec(ctx, update, append(parameters, keyValues...)...); err != nil {
		return poisonOrWrap(err, description)
	}

	query := "SELECT " + metaSourceLSN + " FROM " + table + " WHERE " + buildKeyWhere(keyColumns, 1, dataTypes)
	var guardLSN *string
	switch err := transaction.QueryRow(ctx, query, keyValues...).Scan(&guardLSN); err {
	case pgx.ErrNoRows:
		// The destination has never seen this key: create it. Unchanged columns
		// fall to their defaults, which is the only honest unknown; if the
		// column is NOT NULL, the failure surfaces as a poisonable error.
		if _, err := transaction.Exec(ctx, buildInsertSQL(table, columns, dataTypes), parameters...); err != nil {
			return poisonOrWrap(err, description)
		}
		return nil
	case nil:
		// The key exists but the guarded UPDATE skipped it: the row already
		// holds a newer LSN, so this event is a stale replay. Leaving the row
		// alone preserves the destination value exactly.
		return nil
	default:
		return poisonOrWrap(err, description)
	}
}

func buildKeyWhere(keyColumns []string, firstParameter int, dataTypes map[string]string) string {
	var builder strings.Builder
	for index, column := range keyColumns {
		if index > 0 {
			builder.WriteString(" AND ")
		}
		fmt.Fprintf(&builder, "%s = $%d%s", quoteIdent(column), firstParameter+index, castFor(dataTypes[column]))
	}
	return builder.String()
}

// buildUpsertSQL builds a guarded upsert. Parameter layout:
//
//	1..n user columns, n+1 the commit LSN (::pg_lsn), n+2 the commit time.
//
// The replicated_at column is refreshed with now() on every write and inserted
// with now() on a new row, so it never needs a parameter.
func buildUpsertSQL(table string, columns, keyColumns []string, dataTypes map[string]string) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(table)
	builder.WriteString(" (")
	writeColumns(&builder, columns)
	fmt.Fprintf(&builder, ", %s, %s, %s", metaSourceLSN, metaSourceCommitTime, metaReplicatedAt)
	builder.WriteString(") VALUES (")
	for index, column := range columns {
		if index > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "$%d%s", index+1, castFor(dataTypes[column]))
	}
	fmt.Fprintf(&builder, ", $%d::pg_lsn, $%d, now()) ", len(columns)+1, len(columns)+2)
	builder.WriteString("ON CONFLICT (")
	writeColumns(&builder, keyColumns)
	builder.WriteString(") DO UPDATE SET ")
	for index, column := range columns {
		if index > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "%s = EXCLUDED.%s", quoteIdent(column), quoteIdent(column))
	}
	fmt.Fprintf(&builder,
		", %s = EXCLUDED.%s, %s = EXCLUDED.%s, %s = now()",
		metaSourceLSN, metaSourceLSN, metaSourceCommitTime, metaSourceCommitTime, metaReplicatedAt)
	builder.WriteString(" WHERE ")
	builder.WriteString(table)
	builder.WriteString(".")
	builder.WriteString(metaSourceLSN)
	builder.WriteString(" IS NULL OR ")
	builder.WriteString(table)
	builder.WriteString(".")
	builder.WriteString(metaSourceLSN)
	builder.WriteString(" <= EXCLUDED.")
	builder.WriteString(metaSourceLSN)
	return builder.String()
}

func buildDeleteSQL(table string, keyColumns []string, dataTypes map[string]string) string {
	var builder strings.Builder
	builder.WriteString("DELETE FROM ")
	builder.WriteString(table)
	builder.WriteString(" WHERE ")
	for index, column := range keyColumns {
		if index > 0 {
			builder.WriteString(" AND ")
		}
		fmt.Fprintf(&builder, "%s = $%d%s", quoteIdent(column), index+1, castFor(dataTypes[column]))
	}
	fmt.Fprintf(&builder, " AND (%s IS NULL OR %s <= $%d::pg_lsn)",
		metaSourceLSN, metaSourceLSN, len(keyColumns)+1)
	return builder.String()
}

func writeColumns(builder *strings.Builder, columns []string) {
	for index, column := range columns {
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(quoteIdent(column))
	}
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func verifyIdent(name string) error {
	if !identPattern.MatchString(name) {
		return fmt.Errorf("identifier %q is not a simple [A-Za-z_][A-Za-z0-9_]* name", name)
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// poisonOrWrap classifies a PostgreSQL error: statement-level failures such as
// constraint violations, unknown tables or columns, and un-castable types can
// never succeed and are marked permanent (poison). Everything else stays
// transient so the writer can retry the destination transaction.
func poisonOrWrap(err error, description string) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return errclass.Permanent(fmt.Errorf("%s: %w", description, err))
	}
	return fmt.Errorf("%s: %w", description, err)
}

func permanent(format string, args ...any) error {
	return errclass.Permanentf(format, args...)
}

// IsUndefinedColumn classifies a permanent failure caused by a column that does
// not exist on the destination, the drift pattern a column-added DDL on the
// source produces. This is the one drift form the writer can repair.
func IsUndefinedColumn(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "42703"
}

// RepairSchema brings the destination table up to date with the source for an
// event that failed because it references columns the destination does not yet
// have. Each missing column is added with the exact source definition (name,
// type, nullability, default), so a column-added DDL no longer poisons: the
// destination's existing rows get the same backfilled value the source's
// pre-existing rows got, and the retried event applies normally. The writer
// retries the event exactly once after a repair; anything else (a type change of
// an existing column, a missing ENUM type, NOT NULL without a default on a
// non-empty table) still falls through to the dead-letter queue.
func (applier *Applier) RepairSchema(ctx context.Context, sourceDSN string, event cdc.Event) (bool, error) {
	dataTypes, err := applier.tableColumns(ctx, event.Schema, event.Table)
	if err != nil {
		return false, err
	}

	var missing []string
	for column := range event.After {
		if _, ok := dataTypes[column]; !ok {
			missing = append(missing, column)
		}
	}
	for column := range event.Key {
		if _, ok := dataTypes[column]; !ok {
			missing = append(missing, column)
		}
	}
	if len(missing) == 0 {
		return false, nil
	}
	sort.Strings(missing)

	source, err := transport.ConnectPostgres(ctx, sourceDSN)
	if err != nil {
		return false, fmt.Errorf("connect to source for schema repair: %w", err)
	}
	defer func() { _ = source.Close(context.Background()) }()

	added := 0
	for _, column := range missing {
		var dataType *string
		var defaultValue *string
		var nullable bool
		var typeKind string
		var typeSchema string
		var typeName string
		err := source.QueryRow(ctx, `
			SELECT format_type(a.atttypid, a.atttypmod)::text,
			       NOT a.attnotnull,
			       pg_get_expr(d.adbin, d.adrelid),
			       t.typtype,
			       n.nspname,
			       t.typname
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			JOIN pg_type t ON t.oid = a.atttypid
			LEFT JOIN pg_namespace n ON n.oid = t.typnamespace
			LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHERE c.oid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped AND a.attname = $2`,
			quoteIdent(event.Schema)+"."+quoteIdent(event.Table), column).
			Scan(&dataType, &nullable, &defaultValue, &typeKind, &typeSchema, &typeName)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf(
				"column %q is also missing on the source %s.%s; it cannot be auto-added, align the schemas manually",
				column, event.Schema, event.Table)
		}
		if err != nil {
			return false, fmt.Errorf("read source definition of column %q: %w", column, err)
		}
		if dataType == nil {
			return false, fmt.Errorf("column %q on the source %s.%s has no type", column, event.Schema, event.Table)
		}
		if typeKind == "e" {
			if err := applier.ensureDestinationType(ctx, source, typeSchema, typeName); err != nil {
				return false, fmt.Errorf("column %q on %s.%s: %w", column, event.Schema, event.Table, err)
			}
		}

		var builder strings.Builder
		builder.WriteString("ALTER TABLE ")
		builder.WriteString(quoteIdent(event.Schema) + "." + quoteIdent(event.Table))
		builder.WriteString(" ADD COLUMN ")
		builder.WriteString(quoteIdent(column))
		builder.WriteString(" ")
		builder.WriteString(*dataType)
		if !nullable {
			builder.WriteString(" NOT NULL")
		}
		if defaultValue != nil {
			builder.WriteString(" DEFAULT ")
			builder.WriteString(*defaultValue)
		}
		if _, err := applier.connection.Exec(ctx, builder.String()); err != nil {
			return false, fmt.Errorf("auto-apply column %q on the destination %s.%s: %w", column, event.Schema, event.Table, err)
		}
		added++
	}

	applier.invalidateColumns(event.Schema, event.Table)
	return added > 0, nil
}

// ensureDestinationType makes sure a user-defined ENUM type exists on the
// destination before a repair ALTERs a column of that type, so the destination
// can store exactly the values the source can. Only enums are auto-created;
// other user-defined type kinds (domains, composites, ranges) fall through and
// let the ALTER fail, quarantining the event instead of guessing at it.
func (applier *Applier) ensureDestinationType(ctx context.Context, source *pgx.Conn, schema, name string) error {
	if schema == "" || name == "" {
		return nil
	}

	var exists bool
	if err := applier.connection.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE n.nspname = $1 AND t.typname = $2
		)`, schema, name).Scan(&exists); err != nil {
		return fmt.Errorf("check destination type %s.%s: %w", schema, name, err)
	}
	if exists {
		return nil
	}

	labels, kind, err := sourceEnumDefn(ctx, source, schema, name)
	if err != nil {
		return err
	}
	if kind != "e" {
		return fmt.Errorf("source type %s.%s is a %s, not an enum; auto-creating it is not supported", schema, name, kind)
	}
	if _, err := applier.connection.Exec(ctx,
		"CREATE TYPE "+quoteIdent(schema)+"."+quoteIdent(name)+" AS ENUM ("+enumValues(labels)+")"); err != nil {
		return fmt.Errorf("auto-create destination enum type %s.%s: %w", schema, name, err)
	}
	return nil
}

// enumValues renders ENUM labels for a CREATE TYPE statement, quoting apostrophes.
func enumValues(labels []string) string {
	escaped := make([]string, 0, len(labels))
	for _, label := range labels {
		escaped = append(escaped, "'"+strings.ReplaceAll(label, "'", "''")+"'")
	}
	return strings.Join(escaped, ", ")
}

// sourceEnumDefn reads a type's kind and (for enums) its labels in order from
// the source database, used by schema repair to replay a missing ENUM onto the
// destination so enum-typed columns can be auto-added.
func sourceEnumDefn(ctx context.Context, source *pgx.Conn, schema, name string) ([]string, string, error) {
	var kind string
	rows, err := source.Query(ctx, `
		SELECT t.typtype, e.enumlabel
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		LEFT JOIN pg_enum e ON e.enumtypid = t.oid
		WHERE n.nspname = $1 AND t.typname = $2
		ORDER BY e.enumsortorder`, schema, name)
	if err != nil {
		return nil, "", fmt.Errorf("read source type %s.%s: %w", schema, name, err)
	}
	defer rows.Close()

	var labels []string
	var found bool
	for rows.Next() {
		found = true
		var label *string
		if err := rows.Scan(&kind, &label); err != nil {
			return nil, "", fmt.Errorf("scan source type %s.%s: %w", schema, name, err)
		}
		if label != nil {
			labels = append(labels, *label)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate source type %s.%s: %w", schema, name, err)
	}
	if !found {
		return nil, "", fmt.Errorf("source type %s.%s does not exist", schema, name)
	}
	return labels, kind, nil
}
