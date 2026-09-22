CREATE TABLE public.users (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    source_lsn PG_LSN NOT NULL,
    source_commit_time TIMESTAMPTZ NOT NULL,
    replicated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.profile (
    user_id BIGINT PRIMARY KEY,
    score NUMERIC(12, 4) NOT NULL DEFAULT 0,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_lsn PG_LSN NOT NULL,
    source_commit_time TIMESTAMPTZ NOT NULL,
    replicated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.cdc_applied_events (
    event_id TEXT PRIMARY KEY,
    source_lsn PG_LSN NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.cdc_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.cdc_dead_letters (
    event_id TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    source_lsn TEXT NOT NULL,
    source_commit_time TIMESTAMPTZ NOT NULL,
    key JSONB,
    after JSONB,
    reason TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);