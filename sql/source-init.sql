CREATE TABLE public.users (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
);

CREATE TABLE public.profile (
    user_id BIGINT PRIMARY KEY,
    score NUMERIC(12, 4) NOT NULL DEFAULT 0,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION artie_demo_pub
    FOR TABLE public.users, public.profile
    WITH (publish = 'insert, update, delete');