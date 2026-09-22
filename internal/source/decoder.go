package source

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"example.com/pg-live-sync/internal/cdc"
	"github.com/jackc/pglogrepl"
)

// DefaultSpoolCap is the number of row changes kept in memory for one
// transaction before the remainder is spooled to a temporary file. A huge
// transaction therefore costs O(spool cap) memory instead of O(rows).
const DefaultSpoolCap = 4096

// CommittedTransaction describes a committed source transaction whose events
// are ready to be published. Events are pulled one at a time with NextEvent so
// a transaction with millions of rows never has to exist in memory at once.
type CommittedTransaction struct {
	TransactionID uint32
	CommitLSN     pglogrepl.LSN
	EndLSN        pglogrepl.LSN
	CommitTime    time.Time
	Count         int
}

type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
	// keys caches the destination identity columns per relation, discovered
	// from the source catalog (primary key, replica identity index, or the
	// primary key of a REPLICA IDENTITY FULL table). Only the resolver's
	// columns count as the row key, even when pgoutput flags every column as a
	// key (FULL) or flags an index that is not the destination primary key.
	keys        map[uint32][]string
	identity    func(namespace, relation string) []string
	transaction *pendingTransaction
	pull        *pullState
	spoolCap    int
}

// pendingChange is one row mutation. The JSON shape is the disk spool format.
type pendingChange struct {
	Schema    string   `json:"schema"`
	Table     string   `json:"table"`
	Operation string   `json:"operation"`
	Key       cdc.Row  `json:"key"`
	Before    cdc.Row  `json:"before,omitempty"`
	After     cdc.Row  `json:"after,omitempty"`
	Unchanged []string `json:"unchanged,omitempty"`
}

type pendingTransaction struct {
	id        uint32
	mem       []pendingChange
	spool     *os.File
	spoolPath string
	spoolN    int
}

// pullState streams a committed transaction's events from memory and then from
// its spool file without ever assembling the whole transaction in memory.
type pullState struct {
	commit      CommittedTransaction
	changes     []pendingChange
	spool       *os.File
	spoolReader *bufio.Reader
	spoolPath   string
	seq         int
}

func NewDecoder() *Decoder {
	return &Decoder{relations: make(map[uint32]*pglogrepl.RelationMessage), spoolCap: DefaultSpoolCap}
}

// NewDecoderWithIdentity builds a decoder whose keys are the columns that the
// source-catalog discovery returned for each table (DEFAULT primary key, USING
// INDEX columns, or primary key for FULL). Consulted lazily and cached per
// relation id. A nil or empty result falls back to the pgoutput key flags,
// which is correct for the default replica identity and for synthetic tests.
func NewDecoderWithIdentity(resolve func(namespace, relation string) []string) *Decoder {
	return &Decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		keys:      make(map[uint32][]string),
		identity:  resolve,
		spoolCap:  DefaultSpoolCap,
	}
}

// NewDecoderWithSpoolCap is provided for tests that exercise the spool.
func NewDecoderWithSpoolCap(spoolCap int) *Decoder {
	return &Decoder{relations: make(map[uint32]*pglogrepl.RelationMessage), spoolCap: spoolCap}
}

// Handle consumes one pgoutput message. Row changes stay buffered (and possibly
// spooled) until Commit, so an aborted or incomplete source transaction is never
// published to Kafka. At Commit it returns a descriptor; pull the events with
// NextEvent before processing the next message.
func (decoder *Decoder) Handle(message pglogrepl.Message) (*CommittedTransaction, error) {
	switch message := message.(type) {
	case *pglogrepl.RelationMessage:
		decoder.relations[message.RelationID] = message
		if decoder.identity != nil {
			decoder.keys[message.RelationID] = decoder.identity(message.Namespace, message.RelationName)
		}
		return nil, nil

	case *pglogrepl.BeginMessage:
		if decoder.transaction != nil {
			return nil, fmt.Errorf("received begin for transaction %d while transaction %d is open", message.Xid, decoder.transaction.id)
		}
		decoder.transaction = &pendingTransaction{id: message.Xid}
		return nil, nil

	case *pglogrepl.InsertMessage:
		relation, err := decoder.relation(message.RelationID)
		if err != nil {
			return nil, err
		}
		after, unchanged, err := decodeTuple(relation, message.Tuple, false, nil)
		if err != nil {
			return nil, fmt.Errorf("decode insert for %s.%s: %w", relation.Namespace, relation.RelationName, err)
		}
		if len(unchanged) > 0 {
			return nil, fmt.Errorf("insert for %s.%s carries unchanged TOAST markers", relation.Namespace, relation.RelationName)
		}
		key, err := decoder.primaryKey(relation, after)
		if err != nil {
			return nil, err
		}
		return nil, decoder.append(pendingChange{
			Schema: relation.Namespace, Table: relation.RelationName,
			Operation: string(cdc.OperationInsert), Key: key, After: after,
		})

	case *pglogrepl.UpdateMessage:
		relation, err := decoder.relation(message.RelationID)
		if err != nil {
			return nil, err
		}

		var oldRow cdc.Row
		var oldKey cdc.Row
		if message.OldTuple != nil {
			oldRow, _, err = decodeTuple(relation, message.OldTuple, message.OldTupleType == 'K', nil)
			if err != nil {
				return nil, fmt.Errorf("decode old update tuple for %s.%s: %w", relation.Namespace, relation.RelationName, err)
			}
			oldKey, err = decoder.primaryKey(relation, oldRow)
			if err != nil {
				return nil, err
			}
		}

		// New tuple. A column marked 'u' (unchanged TOAST value) is filled from
		// the full old image when one was sent (REPLICA IDENTITY FULL) because
		// that image is the source's truth; drawing from nothing would fabricate.
		// Without a full old image the column is reported as unchanged so the
		// writer preserves the destination value instead of guessing.
		after, afterUnchanged, err := decodeTuple(relation, message.NewTuple, false, oldRow)
		if err != nil {
			return nil, fmt.Errorf("decode new update tuple for %s.%s: %w", relation.Namespace, relation.RelationName, err)
		}
		newKey, err := decoder.primaryKey(relation, after)
		if err != nil {
			return nil, err
		}

		if message.OldTuple != nil && !equalRows(oldKey, newKey) {
			// A changed primary key is modelled as delete(old) then insert(new),
			// the only shape a keyed destination can apply idempotently. The new
			// row must be complete, so unchanged TOAST columns are filled from
			// the old image, which is available because a key change can only be
			// detected when the full old tuple was sent (REPLICA IDENTITY FULL).
			for _, column := range afterUnchanged {
				value, ok := oldRow[column]
				if !ok || value == nil {
					return nil, fmt.Errorf(
						"unchanged TOAST column %q has no old image while changing the key of %s.%s; set REPLICA IDENTITY FULL",
						column, relation.Namespace, relation.RelationName)
				}
				after[column] = value
			}
			if err := decoder.append(pendingChange{
				Schema: relation.Namespace, Table: relation.RelationName,
				Operation: string(cdc.OperationDelete), Key: oldKey, Before: oldRow,
			}); err != nil {
				return nil, err
			}
			return nil, decoder.append(pendingChange{
				Schema: relation.Namespace, Table: relation.RelationName,
				Operation: string(cdc.OperationInsert), Key: newKey, After: after,
			})
		}

		key := newKey
		if oldKey != nil {
			key = oldKey
		}
		return nil, decoder.append(pendingChange{
			Schema: relation.Namespace, Table: relation.RelationName,
			Operation: string(cdc.OperationUpdate), Key: key, Before: oldRow, After: after,
			Unchanged: afterUnchanged,
		})

	case *pglogrepl.DeleteMessage:
		relation, err := decoder.relation(message.RelationID)
		if err != nil {
			return nil, err
		}
		before, _, err := decodeTuple(relation, message.OldTuple, message.OldTupleType == 'K', nil)
		if err != nil {
			return nil, fmt.Errorf("decode delete for %s.%s: %w", relation.Namespace, relation.RelationName, err)
		}
		key, err := decoder.primaryKey(relation, before)
		if err != nil {
			return nil, err
		}
		return nil, decoder.append(pendingChange{
			Schema: relation.Namespace, Table: relation.RelationName,
			Operation: string(cdc.OperationDelete), Key: key, Before: before,
		})

	case *pglogrepl.TruncateMessage:
		// A TRUNCATE acts on whole tables, not rows. It travels through the
		// pipeline as one event per truncated relation inside the same source
		// transaction, so the destination can clear exactly those tables and
		// keep the multi-table operation atomic with the rest of the group.
		if decoder.transaction == nil {
			return nil, fmt.Errorf("received truncate outside a transaction")
		}
		for _, relationID := range message.RelationIDs {
			relation, err := decoder.relation(relationID)
			if err != nil {
				return nil, fmt.Errorf("truncate references undescribed relation %d: %w", relationID, err)
			}
			if err := decoder.append(pendingChange{
				Schema:    relation.Namespace,
				Table:     relation.RelationName,
				Operation: string(cdc.OperationTruncate),
			}); err != nil {
				return nil, err
			}
		}
		return nil, nil

	case *pglogrepl.CommitMessage:
		if decoder.transaction == nil {
			return nil, fmt.Errorf("received commit with no open transaction")
		}
		transaction := decoder.transaction
		decoder.transaction = nil

		decoder.pull = &pullState{
			commit: CommittedTransaction{
				TransactionID: transaction.id,
				CommitLSN:     message.CommitLSN,
				EndLSN:        message.TransactionEndLSN,
				CommitTime:    message.CommitTime,
				Count:         len(transaction.mem) + transaction.spoolN,
			},
			changes:   transaction.mem,
			spool:     transaction.spool,
			spoolPath: transaction.spoolPath,
		}
		if decoder.pull.spool != nil {
			if _, err := decoder.pull.spool.Seek(0, io.SeekStart); err != nil {
				decoder.closePull()
				return nil, fmt.Errorf("rewind transaction spool: %w", err)
			}
			decoder.pull.spoolReader = bufio.NewReader(decoder.pull.spool)
		}
		return &decoder.pull.commit, nil

	default:
		// Type, Origin, and logical-message records do not alter table rows here.
		return nil, nil
	}
}

// NextEvent returns the committed transaction's events one at a time. It
// returns (Event{}, false, nil) when the transaction is fully drained, after
// which the temporary spool file has been removed.
func (decoder *Decoder) NextEvent() (cdc.Event, bool, error) {
	pull := decoder.pull
	if pull == nil {
		return cdc.Event{}, false, fmt.Errorf("no open transaction to pull")
	}

	var change pendingChange
	switch {
	case pull.seq < len(pull.changes):
		change = pull.changes[pull.seq]
	default:
		if pull.spoolReader != nil {
			line, err := pull.spoolReader.ReadBytes('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				decoder.closePull()
				return cdc.Event{}, false, fmt.Errorf("read spooled transaction events: %w", err)
			}
			if len(line) == 0 {
				decoder.closePull()
				return cdc.Event{}, false, nil
			}
			if err := json.Unmarshal(line, &change); err != nil {
				decoder.closePull()
				return cdc.Event{}, false, fmt.Errorf("decode spooled transaction event: %w", err)
			}
		} else {
			decoder.closePull()
			return cdc.Event{}, false, nil
		}
	}

	pull.seq++
	sequence := pull.seq
	event := cdc.Event{
		Version:   cdc.EventVersion,
		ID:        pull.commit.CommitLSN.String() + ":" + strconv.Itoa(sequence),
		Schema:    change.Schema,
		Table:     change.Table,
		Operation: cdc.Operation(change.Operation),
		Key:       change.Key,
		Before:    change.Before,
		After:     change.After,
		Source: cdc.SourceMetadata{
			TransactionID:         pull.commit.TransactionID,
			CommitLSN:             pull.commit.CommitLSN.String(),
			TransactionEndLSN:     pull.commit.EndLSN.String(),
			CommitTime:            pull.commit.CommitTime,
			Sequence:              sequence,
			TransactionEventCount: pull.commit.Count,
		},
		UnchangedColumns: change.Unchanged,
	}
	return event, true, nil
}

// Reset discards any in-flight transaction and pull state, closing and removing
// any temporary spool file. The reader calls it after a replication reconnect so
// a partially decoded transaction never leaks across the gap; PostgreSQL resends
// everything from the last acknowledged position.
func (decoder *Decoder) Reset() {
	decoder.closePull()
	if decoder.transaction != nil && decoder.transaction.spool != nil {
		_ = decoder.transaction.spool.Close()
		if decoder.transaction.spoolPath != "" {
			_ = os.Remove(decoder.transaction.spoolPath)
		}
	}
	decoder.transaction = nil
	decoder.relations = make(map[uint32]*pglogrepl.RelationMessage)
}

func (decoder *Decoder) closePull() {
	if decoder.pull == nil {
		return
	}
	if decoder.pull.spool != nil {
		_ = decoder.pull.spool.Close()
		if decoder.pull.spoolPath != "" {
			_ = os.Remove(decoder.pull.spoolPath)
		}
	}
	decoder.pull = nil
}

func (decoder *Decoder) relation(id uint32) (*pglogrepl.RelationMessage, error) {
	relation, ok := decoder.relations[id]
	if !ok {
		return nil, fmt.Errorf("relation %d has not been described", id)
	}
	return relation, nil
}

func (decoder *Decoder) append(change pendingChange) error {
	if decoder.transaction == nil {
		return fmt.Errorf("received row change outside a transaction")
	}
	transaction := decoder.transaction
	if len(transaction.mem) < decoder.spoolCap {
		transaction.mem = append(transaction.mem, change)
		return nil
	}
	if transaction.spool == nil {
		file, err := os.CreateTemp("", "live-cdc-spool-*.jsonl")
		if err != nil {
			return fmt.Errorf("create transaction spool: %w", err)
		}
		transaction.spool = file
		transaction.spoolPath = file.Name()
	}
	line, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("spool transaction event: %w", err)
	}
	if _, err := transaction.spool.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write transaction spool: %w", err)
	}
	transaction.spoolN++
	return nil
}

// decodeTuple converts a pgoutput tuple into a row, pairing values with
// relation-column positions. keyOnly tuples carry NULL placeholders in the
// non-key positions.
//
// An unchanged TOAST ('u') value is never blindly trusted. When the old image
// supplies the column's real value (a REPLICA IDENTITY FULL table), that value is
// used because it is the source's truth. When the old image is only key columns
// (the default replica identity) or absent, there is no truth to copy, so the
// column is reported as unchanged: the writer preserves the destination's own
// value rather than guess. A key column can never be 'u'.
func decodeTuple(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData, keyOnly bool, fill cdc.Row) (cdc.Row, []string, error) {
	if tuple == nil {
		return nil, nil, fmt.Errorf("tuple is missing")
	}
	if len(tuple.Columns) != len(relation.Columns) {
		return nil, nil, fmt.Errorf(
			"tuple has %d columns, relation metadata has %d",
			len(tuple.Columns),
			len(relation.Columns),
		)
	}

	row := make(cdc.Row)
	var unchanged []string
	for index, column := range relation.Columns {
		if keyOnly && column.Flags&1 == 0 {
			continue
		}

		value := tuple.Columns[index]
		switch value.DataType {
		case 'n':
			row[column.Name] = nil
		case 't':
			textValue := string(value.Data)
			row[column.Name] = &textValue
		case 'u':
			if keyOnly {
				return nil, nil, fmt.Errorf("primary-key column %q cannot be an unchanged TOAST value", column.Name)
			}
			if filled, ok := fill[column.Name]; ok && filled != nil {
				row[column.Name] = filled
				continue
			}
			unchanged = append(unchanged, column.Name)
		case 'b':
			return nil, nil, fmt.Errorf("binary column %q is not supported", column.Name)
		default:
			return nil, nil, fmt.Errorf("column %q has unknown tuple type %q", column.Name, value.DataType)
		}
	}
	return row, unchanged, nil
}

func (decoder *Decoder) primaryKey(relation *pglogrepl.RelationMessage, row cdc.Row) (cdc.Row, error) {
	key := make(cdc.Row)
	columns := decoder.keys[relation.RelationID]
	if len(columns) == 0 {
		// No catalog resolution: fall back to the pgoutput key flags, which is
		// the default replica identity (primary key).
		for _, column := range relation.Columns {
			if column.Flags&1 != 0 {
				columns = append(columns, column.Name)
			}
		}
	}
	for _, column := range columns {
		value, ok := row[column]
		if !ok {
			return nil, fmt.Errorf("primary-key column %q is missing", column)
		}
		if value == nil {
			return nil, fmt.Errorf("primary-key column %q is NULL", column)
		}
		key[column] = value
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("table %s.%s has no primary key", relation.Namespace, relation.RelationName)
	}
	return key, nil
}

func equalRows(left, right cdc.Row) bool {
	if len(left) != len(right) {
		return false
	}
	for column, leftValue := range left {
		rightValue, ok := right[column]
		if !ok || !equalValues(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func equalValues(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
