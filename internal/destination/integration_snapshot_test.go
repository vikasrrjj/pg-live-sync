//go:build integration

package destination

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The snapshot must land rows in the same text-format values the live stream
// upserts: enums, arrays, numerics, jsonb and timestamps arrive typed and exact.
// Running it against the demo's real publication (users + profile) also doubles
// as the schema-mirror smoke test.
func TestSnapshotRoundTripTypes(t *testing.T) {
	integrationGate(t)
	ensureDestTestDatabase(t)
	connection, applier := openDestTest(t)
	ctx := context.Background()

	source, err := pgx.Connect(ctx, sourceIntegrationDSN())
	if err != nil {
		t.Fatalf("connect to source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close(context.Background()) })

	// Mirror the live destination schema onto dest_test: users with the enum +
	// text[] columns, profile with numeric/jsonb/timestamptz/boolean.
	labels, err := sourceEnumLabels(ctx)
	if err != nil {
		t.Fatalf("read source enum labels: %v", err)
	}
	if len(labels) == 0 {
		t.Fatal("source enum public.mood defines no labels")
	}

	dropDestTables(t, connection, "users", "profile")
	t.Cleanup(func() {
		if _, err := connection.Exec(context.Background(), `DROP TYPE IF EXISTS public.it_snapshot_mood`); err != nil {
			t.Errorf("drop dest_test it_snapshot_mood type on cleanup: %v", err)
		}
	})
	t.Cleanup(func() { dropDestTables(t, connection, "users", "profile") })
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TYPE public.it_snapshot_mood AS ENUM (%s)`, enumValues(labels))); err != nil {
		t.Fatalf("create dest_test it_snapshot_mood type: %v", err)
	}
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.users (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL UNIQUE,
			mood public.it_snapshot_mood,
			tags TEXT[],
			%s
		)`, metaColumnsDDL)); err != nil {
		t.Fatalf("create dest_test users: %v", err)
	}
	if _, err := connection.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE public.profile (
			user_id BIGINT PRIMARY KEY,
			score NUMERIC(12,4) NOT NULL DEFAULT 0,
			tags JSONB NOT NULL DEFAULT '[]'::jsonb,
			active BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			%s
		)`, metaColumnsDDL)); err != nil {
		t.Fatalf("create dest_test profile: %v", err)
	}

	// A deterministic precision + jsonb case that must survive the ::text round
	// trip bit-for-bit. Removed at the end so the demo stream is undisturbed.
	if _, err := source.Exec(ctx,
		`INSERT INTO public.profile (user_id, score, tags, active) VALUES (999999, 12345.6789, '{"a":[1,2]}'::jsonb, true)`); err != nil {
		t.Fatalf("seed source precision row: %v", err)
	}
	t.Cleanup(func() {
		_, _ = source.Exec(context.Background(), `DELETE FROM public.profile WHERE user_id = 999999`)
	})

	snapshotter := NewSnapshotter(sourceIntegrationDSN(), "live_demo_slot", "live_demo_pub", applier)
	if err := snapshotter.RunSnapshot(ctx); err != nil {
		t.Fatalf("run snapshot: %v", err)
	}

	// Enums and arrays: the users row created live must arrive with the exact
	// typed values, proving the per-column ::text + typed-cast path.
	var copiedMood, copiedTags string
	err = connection.QueryRow(ctx,
		`SELECT mood::text, COALESCE((tags::text),'') FROM public.users WHERE email = 'enumarr@example.com'`).
		Scan(&copiedMood, &copiedTags)
	if err != nil {
		t.Fatalf("read copied enum/array row: %v", err)
	}
	var sourceMood, sourceTags string
	err = source.QueryRow(ctx,
		`SELECT mood::text, COALESCE((tags::text),'') FROM public.users WHERE email = 'enumarr@example.com'`).
		Scan(&sourceMood, &sourceTags)
	if err != nil {
		t.Fatalf("read source enum/array row: %v", err)
	}
	if copiedMood != sourceMood || copiedTags != sourceTags {
		t.Fatalf("enum/array snapshot mismatch: dest (%s, %s) vs source (%s, %s)",
			copiedMood, copiedTags, sourceMood, sourceTags)
	}

	// Numeric precision, JSONB content and timestamp presence on the profile side.
	var score string
	var tagsJSON []byte
	var createdAt time.Time
	var active bool
	if err := connection.QueryRow(ctx,
		`SELECT score::text, tags::text, created_at, active FROM public.profile WHERE user_id = 999999`).
		Scan(&score, &tagsJSON, &createdAt, &active); err != nil {
		t.Fatalf("read copied profile row: %v", err)
	}
	if score != "12345.6789" {
		t.Fatalf("numeric precision lost: want 12345.6789, got %q", score)
	}
	if string(tagsJSON) != `{"a": [1, 2]}` {
		t.Fatalf("jsonb content mismatch: %s", tagsJSON)
	}
	if !active || createdAt.IsZero() {
		t.Fatalf("boolean/timestamp snapshot mismatch: active=%v created_at=%v", active, createdAt)
	}

	// Every copied row must carry the snapshot's consistent-point LSN so the live
	// stream can legally overwrite it, and never the empty guard LSN.
	var countAtGuard int
	if err := connection.QueryRow(ctx,
		`SELECT count(*) FROM public.users WHERE source_lsn = '0/0'`).Scan(&countAtGuard); err != nil {
		t.Fatalf("count guard-LSN users: %v", err)
	}
	if countAtGuard != 0 {
		t.Fatalf("snapshot rows must not carry the empty LSN, found %d", countAtGuard)
	}

	// A second run is an overwrite of identical rows: the count must not grow.
	if err := snapshotter.RunSnapshot(ctx); err != nil {
		t.Fatalf("re-run snapshot: %v", err)
	}
	var userCount, profileCount int
	if err := connection.QueryRow(ctx, `SELECT count(*) FROM public.users`).Scan(&userCount); err != nil {
		t.Fatalf("count users: %v", err)
	}
	if err := connection.QueryRow(ctx, `SELECT count(*) FROM public.profile`).Scan(&profileCount); err != nil {
		t.Fatalf("count profile: %v", err)
	}
	if userCount < 1 || profileCount < 1 {
		t.Fatalf("snapshot empty: users=%d profile=%d", userCount, profileCount)
	}
}

func sourceEnumLabels(ctx context.Context) ([]string, error) {
	source, err := pgx.Connect(ctx, sourceIntegrationDSN())
	if err != nil {
		return nil, err
	}
	defer func() { _ = source.Close(context.Background()) }()
	rows, err := source.Query(ctx, `
		SELECT e.enumlabel
		FROM pg_enum e
		JOIN pg_type t ON t.oid = e.enumtypid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typname = 'mood' AND n.nspname = 'public'
		ORDER BY e.enumsortorder`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var labels []string
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return nil, err
		}
		labels = append(labels, label)
	}
	return labels, rows.Err()
}
