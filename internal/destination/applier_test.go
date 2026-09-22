package destination

import (
	"errors"
	"strings"
	"testing"

	"example.com/pg-live-sync/internal/cdc"
	"example.com/pg-live-sync/internal/errclass"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func strPointer(value string) *string {
	return &value
}

func TestBuildUpsertSQL(t *testing.T) {
	cases := []struct {
		name       string
		table      string
		columns    []string
		keyColumns []string
		dataTypes  map[string]string
		want       string
	}{
		{
			name:       "typed casts",
			table:      `"public"."users"`,
			columns:    []string{"email", "name"},
			keyColumns: []string{"id"},
			dataTypes:  map[string]string{"email": "text", "name": "text", "id": "bigint"},
			want:       `INSERT INTO "public"."users" ("email", "name", source_lsn, source_commit_time, replicated_at) VALUES ($1::text, $2::text, $3::pg_lsn, $4, now()) ON CONFLICT ("id") DO UPDATE SET "email" = EXCLUDED."email", "name" = EXCLUDED."name", source_lsn = EXCLUDED.source_lsn, source_commit_time = EXCLUDED.source_commit_time, replicated_at = now() WHERE "public"."users".source_lsn IS NULL OR "public"."users".source_lsn <= EXCLUDED.source_lsn`,
		},
		{
			name:       "no casts when types unknown",
			table:      `"public"."t"`,
			columns:    []string{"a"},
			keyColumns: []string{"a"},
			dataTypes:  nil,
			want:       `INSERT INTO "public"."t" ("a", source_lsn, source_commit_time, replicated_at) VALUES ($1, $2::pg_lsn, $3, now()) ON CONFLICT ("a") DO UPDATE SET "a" = EXCLUDED."a", source_lsn = EXCLUDED.source_lsn, source_commit_time = EXCLUDED.source_commit_time, replicated_at = now() WHERE "public"."t".source_lsn IS NULL OR "public"."t".source_lsn <= EXCLUDED.source_lsn`,
		},
		{
			name:       "multi-word type and enum type",
			table:      `"public"."t"`,
			columns:    []string{"at", "mood"},
			keyColumns: []string{"id"},
			dataTypes:  map[string]string{"at": "timestamp with time zone", "mood": "public.mood", "id": "bigint"},
			want:       `INSERT INTO "public"."t" ("at", "mood", source_lsn, source_commit_time, replicated_at) VALUES ($1::timestamp with time zone, $2::public.mood, $3::pg_lsn, $4, now()) ON CONFLICT ("id") DO UPDATE SET "at" = EXCLUDED."at", "mood" = EXCLUDED."mood", source_lsn = EXCLUDED.source_lsn, source_commit_time = EXCLUDED.source_commit_time, replicated_at = now() WHERE "public"."t".source_lsn IS NULL OR "public"."t".source_lsn <= EXCLUDED.source_lsn`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildUpsertSQL(tc.table, tc.columns, tc.keyColumns, tc.dataTypes)
			if got != tc.want {
				t.Fatalf("buildUpsertSQL mismatch\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestBuildDeleteSQL(t *testing.T) {
	cases := []struct {
		name       string
		table      string
		keyColumns []string
		dataTypes  map[string]string
		want       string
	}{
		{
			name:       "single key with cast",
			table:      `"public"."t"`,
			keyColumns: []string{"id"},
			dataTypes:  map[string]string{"id": "bigint"},
			want:       `DELETE FROM "public"."t" WHERE "id" = $1::bigint AND (source_lsn IS NULL OR source_lsn <= $2::pg_lsn)`,
		},
		{
			name:       "composite key keeps placeholder parity",
			table:      `"public"."t"`,
			keyColumns: []string{"tenant", "id"},
			dataTypes:  map[string]string{"tenant": "text", "id": "bigint"},
			want:       `DELETE FROM "public"."t" WHERE "tenant" = $1::text AND "id" = $2::bigint AND (source_lsn IS NULL OR source_lsn <= $3::pg_lsn)`,
		},
		{
			name:       "no casts",
			table:      `"public"."t"`,
			keyColumns: []string{"id"},
			dataTypes:  nil,
			want:       `DELETE FROM "public"."t" WHERE "id" = $1 AND (source_lsn IS NULL OR source_lsn <= $2::pg_lsn)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildDeleteSQL(tc.table, tc.keyColumns, tc.dataTypes)
			if got != tc.want {
				t.Fatalf("buildDeleteSQL mismatch\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestBuildUpdateSQL(t *testing.T) {
	cases := []struct {
		name       string
		table      string
		columns    []string
		keyColumns []string
		dataTypes  map[string]string
		want       string
	}{
		{
			name:       "typed casts and guard",
			table:      `"public"."users"`,
			columns:    []string{"email", "name"},
			keyColumns: []string{"id"},
			dataTypes:  map[string]string{"email": "text", "name": "text", "id": "bigint"},
			want:       `UPDATE "public"."users" SET "email" = $1::text, "name" = $2::text, source_lsn = $3::pg_lsn, source_commit_time = $4, replicated_at = now() WHERE "id" = $5::bigint AND ("public"."users".source_lsn IS NULL OR "public"."users".source_lsn <= $3::pg_lsn)`,
		},
		{
			name:       "composite key",
			table:      `"public"."t"`,
			columns:    []string{"v"},
			keyColumns: []string{"tenant", "id"},
			dataTypes:  map[string]string{"v": "text", "tenant": "text", "id": "bigint"},
			want:       `UPDATE "public"."t" SET "v" = $1::text, source_lsn = $2::pg_lsn, source_commit_time = $3, replicated_at = now() WHERE "tenant" = $4::text AND "id" = $5::bigint AND ("public"."t".source_lsn IS NULL OR "public"."t".source_lsn <= $2::pg_lsn)`,
		},
		{
			name:       "no casts when types unknown",
			table:      `"public"."t"`,
			columns:    []string{"v"},
			keyColumns: []string{"id"},
			dataTypes:  nil,
			want:       `UPDATE "public"."t" SET "v" = $1, source_lsn = $2::pg_lsn, source_commit_time = $3, replicated_at = now() WHERE "id" = $4 AND ("public"."t".source_lsn IS NULL OR "public"."t".source_lsn <= $2::pg_lsn)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildUpdateSQL(tc.table, tc.columns, tc.keyColumns, tc.dataTypes)
			if got != tc.want {
				t.Fatalf("buildUpdateSQL mismatch\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestBuildInsertSQL(t *testing.T) {
	cases := []struct {
		name      string
		table     string
		columns   []string
		dataTypes map[string]string
		want      string
	}{
		{
			name:      "typed casts and metadata",
			table:     `"public"."t"`,
			columns:   []string{"a"},
			dataTypes: map[string]string{"a": "integer"},
			want:      `INSERT INTO "public"."t" ("a", source_lsn, source_commit_time, replicated_at) VALUES ($1::integer, $2::pg_lsn, $3, now())`,
		},
		{
			name:      "multi-word and enum type",
			table:     `"public"."t"`,
			columns:   []string{"at", "mood"},
			dataTypes: map[string]string{"at": "timestamp with time zone", "mood": "public.mood"},
			want:      `INSERT INTO "public"."t" ("at", "mood", source_lsn, source_commit_time, replicated_at) VALUES ($1::timestamp with time zone, $2::public.mood, $3::pg_lsn, $4, now())`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildInsertSQL(tc.table, tc.columns, tc.dataTypes)
			if got != tc.want {
				t.Fatalf("buildInsertSQL mismatch\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestTranscodeRow(t *testing.T) {
	t.Run("sorts columns and passes nil through", func(t *testing.T) {
		columns, values, err := transcodeRow(cdc.Row{
			"b": strPointer("2"),
			"a": nil,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(columns) != 2 || columns[0] != "a" || columns[1] != "b" {
			t.Fatalf("unexpected column order: %v", columns)
		}
		nullValue, ok := values[0].(*string)
		if !ok || nullValue != nil {
			t.Fatalf("unexpected null value: %v", values[0])
		}
		if values[1] == nil || *values[1].(*string) != "2" {
			t.Fatalf("unexpected values: %v", values)
		}
	})

	t.Run("rejects reserved pipeline column", func(t *testing.T) {
		if _, _, err := transcodeRow(cdc.Row{"source_lsn": strPointer("x")}); err == nil {
			t.Fatal("expected reserved-column rejection, got nil")
		}
	})

	t.Run("rejects empty row", func(t *testing.T) {
		if _, _, err := transcodeRow(cdc.Row{}); err == nil {
			t.Fatal("expected empty-row rejection, got nil")
		}
	})

	t.Run("rejects non-simple identifier", func(t *testing.T) {
		if _, _, err := transcodeRow(cdc.Row{"bad-name": strPointer("x")}); err == nil {
			t.Fatal("expected identifier rejection, got nil")
		}
	})
}

func TestCastFor(t *testing.T) {
	cases := map[string]string{
		"":                         "",
		"bigint":                   "::bigint",
		"text[]":                   "::text[]",
		"public.mood":              "::public.mood",
		"timestamp with time zone": "::timestamp with time zone",
	}
	for input, want := range cases {
		if got := castFor(input); got != want {
			t.Fatalf("castFor(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestIsUndefinedColumn(t *testing.T) {
	if !IsUndefinedColumn(&pgconn.PgError{Code: "42703"}) {
		t.Fatal("expected 42703 to be reported as an undefined column")
	}
	if IsUndefinedColumn(&pgconn.PgError{Code: "42704"}) {
		t.Fatal("expected 42704 (undefined type) to NOT be reported as an undefined column")
	}
	if IsUndefinedColumn(errors.New("boom")) {
		t.Fatal("expected a non-pg error to NOT be an undefined column")
	}
}

func TestPoisonOrWrapClassification(t *testing.T) {
	statementError := poisonOrWrap(&pgconn.PgError{Code: "23505", Message: "duplicate key"}, "upsert")
	if !errclass.IsPermanent(statementError) {
		t.Fatalf("expected a pg statement error to be permanent, got: %v", statementError)
	}
	transportError := poisonOrWrap(errors.New("connection reset"), "upsert")
	if errclass.IsPermanent(transportError) {
		t.Fatal("expected a transport error to stay transient")
	}
	if got := transportError.Error(); !strings.Contains(got, "upsert: connection reset") {
		t.Fatalf("unexpected wrapped message: %s", got)
	}
}

func TestTruncateIsStale(t *testing.T) {
	lsn := func(value uint64) pglogrepl.LSN { return pglogrepl.LSN(value) }
	cases := []struct {
		name      string
		eventLSN  pglogrepl.LSN
		watermark pglogrepl.LSN
		want      bool
	}{
		{"no watermark yet applies", lsn(0x1000), 0, false},
		{"never truncated watermark cannot guard", lsn(0x1000), lsn(0), false},
		{"equal LSN is a replay", lsn(0x2000), lsn(0x2000), true},
		{"older truncate is stale", lsn(0x2999), lsn(0x3000), true},
		{"newer truncate applies", lsn(0x4000), lsn(0x3000), false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := truncateIsStale(test.eventLSN, test.watermark); got != test.want {
				t.Fatalf("truncateIsStale(%v, %v) = %v, want %v", test.eventLSN, test.watermark, got, test.want)
			}
		})
	}
}

func TestEnumValues(t *testing.T) {
	cases := []struct {
		labels []string
		want   string
	}{
		{[]string{"happy", "sad", "neutral"}, `'happy', 'sad', 'neutral'`},
		{[]string{"it's", "a'b"}, `'it''s', 'a''b'`},
		{nil, ``},
	}
	for _, tc := range cases {
		if got := enumValues(tc.labels); got != tc.want {
			t.Fatalf("enumValues(%v) = %q, want %q", tc.labels, got, tc.want)
		}
	}
}
