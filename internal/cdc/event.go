package cdc

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const EventVersion = 1

type Operation string

const (
	OperationInsert   Operation = "insert"
	OperationUpdate   Operation = "update"
	OperationDelete   Operation = "delete"
	OperationTruncate Operation = "truncate"
)

// Row contains PostgreSQL text-format column values. A nil pointer is SQL NULL.
type Row map[string]*string

// Event is the small, explicit contract between the source reader and writer.
type Event struct {
	Version   int            `json:"version"`
	ID        string         `json:"id"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	Operation Operation      `json:"operation"`
	Key       Row            `json:"key"`
	Before    Row            `json:"before,omitempty"`
	After     Row            `json:"after,omitempty"`
	Source    SourceMetadata `json:"source"`
	// UnchangedColumns lists columns whose TOASTed source value did not change
	// during an UPDATE. pgoutput sends those as an "unchanged toast" marker
	// instead of the value, so they are deliberately absent from After: the
	// destination must keep its existing value for them rather than write a
	// guessed one. The writer preserves the destination value for these columns.
	UnchangedColumns []string `json:"unchanged_columns,omitempty"`
}

type SourceMetadata struct {
	TransactionID         uint32    `json:"transaction_id"`
	CommitLSN             string    `json:"commit_lsn"`
	TransactionEndLSN     string    `json:"transaction_end_lsn"`
	CommitTime            time.Time `json:"commit_time"`
	Sequence              int       `json:"sequence"`
	TransactionEventCount int       `json:"transaction_event_count"`
}

func (event Event) Validate() error {
	if event.Version != EventVersion {
		return fmt.Errorf("unsupported event version %d", event.Version)
	}
	if event.ID == "" || event.Schema == "" || event.Table == "" {
		return fmt.Errorf("event id, schema, and table are required")
	}
	switch event.Operation {
	case OperationInsert, OperationUpdate:
		if len(event.Key) == 0 {
			return fmt.Errorf("event %s has no key", event.ID)
		}
		if len(event.After) == 0 {
			return fmt.Errorf("%s event %s has no after row", event.Operation, event.ID)
		}
	case OperationDelete:
		if len(event.Key) == 0 {
			return fmt.Errorf("event %s has no key", event.ID)
		}
	case OperationTruncate:
		// A truncate addresses the whole table, never one row: no key, no
		// before/after payload. The destination guard compares the event's
		// commit LSN against the table's durable watermark.
	default:
		return fmt.Errorf("unsupported operation %q", event.Operation)
	}
	return nil
}

// KafkaKey deterministically groups changes for the same row.
func (event Event) KafkaKey() []byte {
	columns := make([]string, 0, len(event.Key))
	for column := range event.Key {
		columns = append(columns, column)
	}
	sort.Strings(columns)

	var key strings.Builder
	key.WriteString(event.Schema)
	key.WriteByte('.')
	key.WriteString(event.Table)
	for _, column := range columns {
		key.WriteByte('|')
		key.WriteString(column)
		key.WriteByte('=')
		if event.Key[column] == nil {
			key.WriteString("NULL")
		} else {
			key.WriteString(*event.Key[column])
		}
	}
	return []byte(key.String())
}
