package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"example.com/pg-live-sync/internal/cdc"
	"github.com/jackc/pgx/v5"
)

// DeadLetter is a quarantined event persisted in public.cdc_dead_letters. The
// record carries everything the apply path needs so a replay never depends on
// the event still being live on the source: the row payloads, the truncated
// preserve hints, and the LSN/time metadata the guards compare against.
type DeadLetter struct {
	EventID          string
	Schema           string
	Table            string
	Operation        cdc.Operation
	SourceLSN        string
	SourceCommitTime time.Time
	Key              cdc.Row
	Before           cdc.Row
	After            cdc.Row
	UnchangedColumns []string
	Reason           string
	OccurredAt       time.Time
}

// Event rebuilds the pipeline event the dead letter was quarantined from, in
// the exact shape Apply expects. The source-only fields (transaction id,
// sequence, event count) do not reach destination apply logic.
func (record DeadLetter) Event() cdc.Event {
	return cdc.Event{
		Version:          cdc.EventVersion,
		ID:               record.EventID,
		Schema:           record.Schema,
		Table:            record.Table,
		Operation:        record.Operation,
		Key:              record.Key,
		Before:           record.Before,
		After:            record.After,
		UnchangedColumns: record.UnchangedColumns,
		Source: cdc.SourceMetadata{
			CommitLSN:  record.SourceLSN,
			CommitTime: record.SourceCommitTime,
		},
	}
}

const deadLetterColumns = `event_id, schema_name, table_name, operation,
		source_lsn, source_commit_time, key, before, after, unchanged_columns, reason, occurred_at`

// ListDeadLetters returns the quarantined events, oldest first. An empty schema
// and table means every dead letter.
func (applier *Applier) ListDeadLetters(ctx context.Context, schema, table string) ([]DeadLetter, error) {
	query := "SELECT " + deadLetterColumns + ` FROM public.cdc_dead_letters`
	var args []any
	if schema != "" || table != "" {
		var filters []string
		if schema != "" {
			args = append(args, schema)
			filters = append(filters, fmt.Sprintf("schema_name = $%d", len(args)))
		}
		if table != "" {
			args = append(args, table)
			filters = append(filters, fmt.Sprintf("table_name = $%d", len(args)))
		}
		query += " WHERE " + strings.Join(filters, " AND ")
	}
	query += " ORDER BY occurred_at"

	rows, err := applier.connection.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()

	var records []DeadLetter
	for rows.Next() {
		record, err := scanDeadLetter(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letters: %w", err)
	}
	return records, nil
}

// GetDeadLetter loads a single quarantined event by id.
func (applier *Applier) GetDeadLetter(ctx context.Context, eventID string) (DeadLetter, error) {
	row := applier.connection.QueryRow(ctx,
		"SELECT "+deadLetterColumns+` FROM public.cdc_dead_letters WHERE event_id = $1`, eventID)
	record, err := scanDeadLetter(row)
	if err != nil {
		return DeadLetter{}, fmt.Errorf("load dead letter %s: %w", eventID, err)
	}
	return record, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDeadLetter(row rowScanner) (DeadLetter, error) {
	var record DeadLetter
	var operation string
	var keyJSON, beforeJSON, afterJSON, unchangedJSON json.RawMessage
	if err := row.Scan(
		&record.EventID, &record.Schema, &record.Table, &operation,
		&record.SourceLSN, &record.SourceCommitTime,
		&keyJSON, &beforeJSON, &afterJSON, &unchangedJSON,
		&record.Reason, &record.OccurredAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DeadLetter{}, fmt.Errorf("no such dead letter")
		}
		return DeadLetter{}, fmt.Errorf("scan dead letter: %w", err)
	}
	record.Operation = cdc.Operation(operation)
	var err error
	if record.Key, err = jsonRow(keyJSON); err != nil {
		return DeadLetter{}, fmt.Errorf("decode dead-letter key: %w", err)
	}
	if record.Before, err = jsonRow(beforeJSON); err != nil {
		return DeadLetter{}, fmt.Errorf("decode dead-letter before row: %w", err)
	}
	if record.After, err = jsonRow(afterJSON); err != nil {
		return DeadLetter{}, fmt.Errorf("decode dead-letter after row: %w", err)
	}
	if len(unchangedJSON) != 0 && string(unchangedJSON) != "null" {
		if err := json.Unmarshal(unchangedJSON, &record.UnchangedColumns); err != nil {
			return DeadLetter{}, fmt.Errorf("decode dead-letter unchanged columns: %w", err)
		}
	}
	return record, nil
}

// jsonRow turns a JSONB object column back into the text-format row the event
// contract uses. NULLs in the JSON match the nil-pointer value of cdc.Row.
func jsonRow(raw json.RawMessage) (cdc.Row, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var row map[string]*string
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return cdc.Row(row), nil
}

// ReplayDeadLetter re-applies a quarantined event exactly once. The quarantine
// marker is rewound first (it is what makes the event a no-op), then the event
// goes through the same Applier.Apply the live writer uses. Success removes the
// dead-letter record; a permanent failure re-stashes the marker so a restart
// still skips the event, and the record stays for the next attempt.
func (applier *Applier) ReplayDeadLetter(ctx context.Context, eventID string) (bool, error) {
	record, err := applier.GetDeadLetter(ctx, eventID)
	if err != nil {
		return false, err
	}
	event := record.Event()

	if _, err := applier.connection.Exec(ctx,
		`DELETE FROM public.cdc_applied_events WHERE event_id = $1`, eventID); err != nil {
		return false, fmt.Errorf("rewind quarantine marker %s: %w", eventID, err)
	}

	if _, err := applier.Apply(ctx, event); err != nil {
		// The marker was already rewound. If we leave it gone, a pipeline
		// restart would silently skip this event; re-stash it (and update the
		// reason) keeps the event safely quarantined for the next attempt.
		if restashErr := applier.RecordDeadLetter(ctx, event, "replay failed: "+err.Error()); restashErr != nil {
			return false, fmt.Errorf("replay %s failed: %v (and re-stashing the quarantine marker failed: %v)", eventID, err, restashErr)
		}
		return false, fmt.Errorf("replay %s: %w", eventID, err)
	}
	// When applied == false the apply's own guards (LSN/watermark, dedupe)
	// decided the change is already reflected downstream, so closing the dead
	// letter is the correct end state rather than a data change. Either way the
	// quarantine record has served its purpose and is removed.
	if _, err := applier.connection.Exec(ctx,
		`DELETE FROM public.cdc_dead_letters WHERE event_id = $1`, eventID); err != nil {
		return false, fmt.Errorf("drop replayed dead letter %s: %w", eventID, err)
	}
	return true, nil
}

// DropDeadLetter discards a quarantined event without applying it: it removes
// the dead-letter record and its quarantine marker so neither the writer nor a
// later replay attempt ever sees the event again.
func (applier *Applier) DropDeadLetter(ctx context.Context, eventID string) error {
	transaction, err := applier.connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dead-letter drop: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	if _, err := transaction.Exec(ctx,
		`DELETE FROM public.cdc_dead_letters WHERE event_id = $1`, eventID); err != nil {
		return fmt.Errorf("drop dead-letter record %s: %w", eventID, err)
	}
	if _, err := transaction.Exec(ctx,
		`DELETE FROM public.cdc_applied_events WHERE event_id = $1`, eventID); err != nil {
		return fmt.Errorf("drop quarantine marker %s: %w", eventID, err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit dead-letter drop %s: %w", eventID, err)
	}
	return nil
}
