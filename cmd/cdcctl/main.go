// Command cdcctl inspects and repairs the destination dead-letter queue.
//
// The destination writer quarantines events it can never apply (schema drift,
// incompatible type changes) into public.cdc_dead_letters instead of retrying
// them forever. Once the destination schema is fixed, an operator replays them
// into the pipeline with cdcctl replay; the event goes through the same
// Applier.Apply path the live writer uses, so the LSN/watermark guards and the
// dedupe markers still apply.
//
//	cdcctl -dsn "$DESTINATION_DSN" list
//	cdcctl -dsn "$DESTINATION_DSN" list -table public.users -json
//	cdcctl -dsn "$DESTINATION_DSN" replay -event 0xAB12-3
//	cdcctl -dsn "$DESTINATION_DSN" replay -table public.users
//	cdcctl -dsn "$DESTINATION_DSN" drop -event 0xAB12-3
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"example.com/artie-mini-cdc/internal/destination"
	"github.com/jackc/pgx/v5"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "cdcctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command, err := parseArgs(args)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), command.timeout)
	defer cancel()

	connection, err := pgx.Connect(ctx, command.dsn)
	if err != nil {
		return fmt.Errorf("connect to destination: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()

	applier := destination.NewApplier(connection)
	if err := applier.EnsureMetaSchema(ctx); err != nil {
		return fmt.Errorf("ensure destination metadata schema: %w", err)
	}

	switch command.action {
	case actionList:
		records, err := applier.ListDeadLetters(ctx, command.schema, command.table)
		if err != nil {
			return err
		}
		return renderList(os.Stdout, records, command.json)
	case actionReplay:
		return replayHandler(ctx, applier, command)
	case actionDrop:
		return dropHandler(ctx, applier, command)
	}
	return fmt.Errorf("unsupported action %q", command.action)
}

type action int

const (
	actionList action = iota
	actionReplay
	actionDrop
)

type command struct {
	action  action
	dsn     string
	schema  string
	table   string
	eventID string
	all     bool
	json    bool
	timeout time.Duration
}

func parseArgs(args []string) (command, error) {
	if len(args) == 0 {
		flag.Usage()
		return command{}, errors.New("missing command (list, replay, drop)")
	}

	actionWord, rest := args[0], args[1:]
	var (
		cmd  command
		flagSet = flag.NewFlagSet("cdcctl "+actionWord, flag.ExitOnError)
	)
	flagSet.StringVar(&cmd.dsn, "dsn", envOrDefault("DESTINATION_DSN", "postgres://postgres:postgres@localhost:5434/destination?sslmode=disable"), "destination connection DSN")
	flagSet.StringVar(&cmd.schema, "schema", "", "filter: destination schema (with or without -table)")
	flagSet.StringVar(&cmd.table, "table", "", "filter: table within the schema, e.g. public.users")
	flagSet.StringVar(&cmd.eventID, "event", "", "single dead-letter event id")
	flagSet.BoolVar(&cmd.all, "all", false, "apply the action to every matching dead letter")
	flagSet.BoolVar(&cmd.json, "json", false, "print records as JSON (list)")
	flagSet.DurationVar(&cmd.timeout, "timeout", 60*time.Second, "operation timeout")
	if err := flagSet.Parse(rest); err != nil {
		return command{}, err
	}

	switch actionWord {
	case "list":
		cmd.action = actionList
	case "replay":
		cmd.action = actionReplay
	case "drop":
		cmd.action = actionDrop
	default:
		return command{}, fmt.Errorf("unknown command %q (want list, replay, or drop)", actionWord)
	}

	// A filter is either a bare table name or schema.table; split what we were
	// given without inventing a default schema.
	if cmd.table != "" && strings.Contains(cmd.table, ".") && cmd.schema == "" {
		parts := strings.SplitN(cmd.table, ".", 2)
		cmd.schema, cmd.table = parts[0], parts[1]
	}

	if (cmd.action == actionReplay || cmd.action == actionDrop) && cmd.eventID == "" && !cmd.all {
		return command{}, errors.New("replay and drop need -event ID or -all (to act on every matching dead letter)")
	}
	if cmd.eventID != "" && cmd.all {
		return command{}, errors.New("-event and -all are mutually exclusive")
	}
	return cmd, nil
}

func replayHandler(ctx context.Context, applier *destination.Applier, cmd command) error {
	replayOne := func(ctx context.Context, eventID string) error {
		applied, err := applier.ReplayDeadLetter(ctx, eventID)
		if err != nil {
			return err
		}
		fmt.Printf("replayed %s applied=%v\n", eventID, applied)
		return nil
	}

	if cmd.eventID != "" {
		if err := replayOne(ctx, cmd.eventID); err != nil {
			return err
		}
		return nil
	}

	records, err := applier.ListDeadLetters(ctx, cmd.schema, cmd.table)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		fmt.Println("no dead letters match")
		return nil
	}
	var replayed, failed []string
	for _, record := range records {
		if err := replayOne(ctx, record.EventID); err != nil {
			failed = append(failed, record.EventID)
			fmt.Fprintf(os.Stderr, "replay failed: %v\n", err)
		} else {
			replayed = append(replayed, record.EventID)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("replayed %d, failed %d: %s", len(replayed), len(failed), strings.Join(failed, ", "))
	}
	fmt.Printf("replayed %d dead letters\n", len(records))
	return nil
}

func dropHandler(ctx context.Context, applier *destination.Applier, cmd command) error {
	if cmd.eventID != "" {
		if err := applier.DropDeadLetter(ctx, cmd.eventID); err != nil {
			return err
		}
		fmt.Printf("dropped %s\n", cmd.eventID)
		return nil
	}

	records, err := applier.ListDeadLetters(ctx, cmd.schema, cmd.table)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		fmt.Println("no dead letters match")
		return nil
	}
	for _, record := range records {
		if err := applier.DropDeadLetter(ctx, record.EventID); err != nil {
			return fmt.Errorf("drop %s: %w", record.EventID, err)
		}
	}
	fmt.Printf("dropped %d dead letters\n", len(records))
	return nil
}

func renderList(out *os.File, records []destination.DeadLetter, asJSON bool) error {
	if len(records) == 0 {
		fmt.Fprintln(out, "no dead letters")
		return nil
	}
	for _, record := range records {
		if asJSON {
			render, err := json.MarshalIndent(record, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal dead letter %s: %w", record.EventID, err)
			}
			fmt.Fprintln(out, string(render))
			continue
		}
		fmt.Fprintf(out, "%s  op=%s  %s.%s  lsn=%s  at=%s\n  reason: %s\n",
			record.EventID, record.Operation, record.Schema, record.Table, record.SourceLSN,
			record.OccurredAt.Format(time.RFC3339), record.Reason)
		if len(record.UnchangedColumns) > 0 {
			fmt.Fprintf(out, "  unchanged: %s\n", strings.Join(record.UnchangedColumns, ", "))
		}
	}
	return nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}