package destination

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"example.com/artie-mini-cdc/internal/cdc"
	"example.com/artie-mini-cdc/internal/transport"
	"github.com/jackc/pgx/v5"
)

// Snapshotter performs the initial consistent-ish copy of the published tables
// from the source into the destination, then hands off to the live WAL stream.
// It is writer-led: the writer waits for the replication slot to exist, snapshots
// everything at the slot's consistent point, and only then starts consuming
// Kafka. Live events use commit LSNs at or after that point, so the LSN guard in
// the applier lets the live stream safely overwrite the snapshot rows.
type Snapshotter struct {
	sourceDSN   string
	slot        string
	publication string
	applier     *Applier
}

func NewSnapshotter(sourceDSN, slot, publication string, applier *Applier) *Snapshotter {
	return &Snapshotter{sourceDSN: sourceDSN, slot: slot, publication: publication, applier: applier}
}

const snapshotStateKey = "snapshot_done"

// Completed reports whether the initial snapshot has already been taken.
func (snapshotter *Snapshotter) Completed(ctx context.Context) (bool, error) {
	var value string
	err := snapshotter.applier.connection.QueryRow(ctx,
		`SELECT value FROM public.cdc_state WHERE key = $1`, snapshotStateKey).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read snapshot state: %w", err)
	}
	return strings.EqualFold(value, "done"), nil
}

// VerifyNotCorrupt refuses a destination that has already finished the snapshot
// but whose source replication slot has vanished: replaying Kafka events without
// the slot's context would fabricate data, so the pipeline stops loudly.
func (snapshotter *Snapshotter) VerifyNotCorrupt(ctx context.Context) error {
	done, err := snapshotter.Completed(ctx)
	if err != nil {
		return err
	}
	if !done {
		return nil
	}
	source, err := transport.ConnectPostgres(ctx, snapshotter.sourceDSN)
	if err != nil {
		return fmt.Errorf("connect to source for corruption check: %w", err)
	}
	defer func() { _ = source.Close(context.Background()) }()

	var found bool
	err = source.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		snapshotter.slot).Scan(&found)
	if err != nil {
		return fmt.Errorf("check replication slot %q: %w", snapshotter.slot, err)
	}
	if !found {
		return fmt.Errorf(
			"corrupted state: snapshot is marked done but replication slot %q is missing on the source; "+
				"restore the slot or reset the pipeline", snapshotter.slot)
	}
	return nil
}

// WaitForSlot polls until the source replication slot exists with a consistent
// point. The reader creates it shortly after boot, but the writer must wait for
// it when both start at the same time.
func (snapshotter *Snapshotter) WaitForSlot(ctx context.Context, timeout time.Duration) error {
	source, err := transport.ConnectPostgres(ctx, snapshotter.sourceDSN)
	if err != nil {
		return fmt.Errorf("connect to source while waiting for replication slot: %w", err)
	}
	defer func() { _ = source.Close(context.Background()) }()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var restartLSN string
		err := source.QueryRow(ctx, `
			SELECT restart_lsn::text
			FROM pg_replication_slots
			WHERE slot_name = $1 AND restart_lsn IS NOT NULL`,
			snapshotter.slot).Scan(&restartLSN)
		if err == nil {
			log.Printf("replication slot %s is ready at consistent point %s", snapshotter.slot, restartLSN)
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("wait for replication slot %q: %w", snapshotter.slot, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("replication slot %q was not created within %s", snapshotter.slot, timeout)
}

func (snapshotter *Snapshotter) MarkCompleted(ctx context.Context) error {
	_, err := snapshotter.applier.connection.Exec(ctx, `
		INSERT INTO public.cdc_state (key, value)
		VALUES ($1, 'done')
		ON CONFLICT (key) DO UPDATE SET value = 'done', updated_at = now()`, snapshotStateKey)
	if err != nil {
		return fmt.Errorf("mark snapshot complete: %w", err)
	}
	return nil
}

// RunSnapshot copies every published table into the destination inside a single
// source REPEATABLE READ read-only transaction and a single destination
// transaction. Rows carry the slot's restart LSN as their source metadata, so the
// live stream (which only ever has events at or after that LSN) can overwrite
// them. Values are read as to_jsonb(...)::text, the JSON round trip of the
// PostgreSQL text representation: numbers stay numbers, strings stay strings.
func (snapshotter *Snapshotter) RunSnapshot(ctx context.Context) error {
	source, err := transport.ConnectPostgres(ctx, snapshotter.sourceDSN)
	if err != nil {
		return fmt.Errorf("connect to source for snapshot: %w", err)
	}
	defer func() { _ = source.Close(context.Background()) }()

	var restartLSN string
	if err := source.QueryRow(ctx, `
		SELECT restart_lsn::text
		FROM pg_replication_slots
		WHERE slot_name = $1`, snapshotter.slot).Scan(&restartLSN); err != nil {
		return fmt.Errorf("read replication slot %q consistent point: %w", snapshotter.slot, err)
	}

	tables, err := snapshotter.publishedTables(ctx, source)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("publication %q publishes no tables", snapshotter.publication)
	}

	// The consistent snapshot: one read-only repeatable-read transaction on the
	// source holds a snapshot of the whole database at the slot's consistent point.
	transaction, err := source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin source snapshot transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()

	var commitTime time.Time
	if err := transaction.QueryRow(ctx, "SELECT now()").Scan(&commitTime); err != nil {
		return fmt.Errorf("read snapshot commit time: %w", err)
	}

	destination, err := snapshotter.applier.connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin destination snapshot transaction: %w", err)
	}
	defer func() { _ = destination.Rollback(context.Background()) }()

	total := 0
	for _, table := range tables {
		primaryKey, err := destinationPrimaryKey(ctx, destination, table.schema+"."+table.name)
		if err != nil {
			return err
		}
		tableCount, err := snapshotter.copyTable(ctx, transaction, destination, table.schema, table.name, primaryKey, restartLSN, commitTime)
		if err != nil {
			return err
		}
		total += tableCount
		log.Printf("snapshot loaded %d rows from %s.%s at consistent point %s",
			tableCount, table.schema, table.name, restartLSN)
	}

	if err := destination.Commit(ctx); err != nil {
		return fmt.Errorf("commit destination snapshot transaction: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit source snapshot transaction: %w", err)
	}
	log.Printf("snapshot complete: %d rows in %d tables", total, len(tables))
	return nil
}

type publishedTable struct {
	schema string
	name   string
}

func (snapshotter *Snapshotter) publishedTables(ctx context.Context, source *pgx.Conn) ([]publishedTable, error) {
	rows, err := source.Query(ctx, `
		SELECT DISTINCT schemaname, tablename
		FROM pg_publication_tables
		WHERE pubname = $1
		ORDER BY schemaname, tablename`, snapshotter.publication)
	if err != nil {
		return nil, fmt.Errorf("list tables of publication %q: %w", snapshotter.publication, err)
	}
	defer rows.Close()

	var tables []publishedTable
	for rows.Next() {
		var table publishedTable
		if err := rows.Scan(&table.schema, &table.name); err != nil {
			return nil, fmt.Errorf("scan publication table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate publication tables: %w", err)
	}
	return tables, nil
}

func (snapshotter *Snapshotter) copyTable(
	ctx context.Context,
	source pgx.Tx,
	destination pgx.Tx,
	schema, table string,
	primaryKey []string,
	restartLSN string,
	commitTime time.Time,
) (int, error) {
	// The destination defines the target shape: only destination columns that
	// also exist on the source are copied. The column list is resolved from the
	// destination catalog so the SELECT is always spelled the same way here and
	// in the live path's typed-cast upserts.
	dstColumns, err := listTableColumns(ctx, destination, schema+"."+table)
	if err != nil {
		return 0, err
	}
	present, err := sourceColumnsPresent(ctx, source, schema, table, dstColumns)
	if err != nil {
		return 0, err
	}
	columns := make([]string, 0, len(dstColumns))
	for _, column := range dstColumns {
		if present[column] {
			columns = append(columns, column)
		}
	}
	for _, column := range primaryKey {
		if !containsString(columns, column) {
			return 0, fmt.Errorf(
				"destination primary-key column %q of %s.%s is missing on the source; align the schemas before snapshotting",
				column, schema, table)
		}
	}

	var builder strings.Builder
	builder.WriteString("SELECT ")
	for index, column := range columns {
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(quoteIdent(column))
		builder.WriteString("::text")
	}
	builder.WriteString(" FROM ")
	builder.WriteString(quoteIdent(schema))
	builder.WriteString(".")
	builder.WriteString(quoteIdent(table))

	rows, err := source.Query(ctx, builder.String())
	if err != nil {
		return 0, fmt.Errorf("select snapshot rows from %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return count, fmt.Errorf("scan snapshot row from %s.%s: %w", schema, table, err)
		}
		row := make(cdc.Row, len(columns))
		for index, column := range columns {
			row[column] = textPointer(values[index])
		}
		if err := verifyRowAgainstPrimaryKey(row, primaryKey); err != nil {
			return count, fmt.Errorf("snapshot row from %s.%s: %w", schema, table, err)
		}
		key := cdc.Row{}
		for _, column := range primaryKey {
			key[column] = row[column]
		}
		if err := snapshotter.applier.UpsertSnapshotRow(ctx, destination, schema, table, row, key, restartLSN, commitTime); err != nil {
			return count, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterate snapshot rows from %s.%s: %w", schema, table, err)
	}
	return count, nil
}

// textPointer turns a scanned column value into the *string the pipeline uses.
// Columns read as "::text" arrive as strings; NULL arrives as a typed nil.
// Double pointers are dereferenced to keep the helper forgiving.
func textPointer(value any) *string {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case *string:
		return typed
	case string:
		return &typed
	default:
		text := fmt.Sprint(typed)
		return &text
	}
}

// listTableColumns returns the destination table's non-dropped columns in
// declaration order, so the snapshot reads exactly the columns the destination
// holds.
func listTableColumns(ctx context.Context, destination pgx.Tx, qualifiedTable string) ([]string, error) {
	rows, err := destination.Query(ctx, `
		SELECT a.attname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.oid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, qualifiedTable)
	if err != nil {
		return nil, fmt.Errorf("list columns of destination %s: %w", qualifiedTable, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan column of destination %s: %w", qualifiedTable, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns of destination %s: %w", qualifiedTable, err)
	}
	return columns, nil
}

// sourceColumnsPresent reports which of the given columns exist on the source,
// so the snapshot never reads a column the destination cannot store.
func sourceColumnsPresent(ctx context.Context, source pgx.Tx, schema, table string, columns []string) (map[string]bool, error) {
	rows, err := source.Query(ctx, `
		SELECT a.attname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.oid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
		  AND a.attname = ANY($2::text[])`,
		quoteIdent(schema)+"."+quoteIdent(table), columns)
	if err != nil {
		return nil, fmt.Errorf("list source columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	present := make(map[string]bool, len(columns))
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan source column of %s.%s: %w", schema, table, err)
		}
		present[column] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source columns of %s.%s: %w", schema, table, err)
	}
	return present, nil
}

// destinationPrimaryKey discovers the destination table's primary key columns so
// the snapshot upserts conflict on the same key the live stream uses.
func destinationPrimaryKey(ctx context.Context, destination pgx.Tx, qualifiedTable string) ([]string, error) {
	rows, err := destination.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`, qualifiedTable)
	if err != nil {
		return nil, fmt.Errorf("read primary key of destination %s: %w", qualifiedTable, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan primary key of destination %s: %w", qualifiedTable, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate primary key of destination %s: %w", qualifiedTable, err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("destination table %s has no primary key; add one or drop the table before snapshotting", qualifiedTable)
	}
	return columns, nil
}

func verifyRowAgainstPrimaryKey(row cdc.Row, primaryKey []string) error {
	for _, column := range primaryKey {
		value, ok := row[column]
		if !ok || value == nil {
			return fmt.Errorf("primary-key column %q is missing or NULL", column)
		}
	}
	return nil
}
