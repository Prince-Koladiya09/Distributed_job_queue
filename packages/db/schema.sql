-- Distributed Job Queue -- PostgreSQL schema
-- Mirrors SRS section 6 (Data Model)

CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- for gen_random_uuid()

DO $$ BEGIN
    CREATE TYPE job_priority AS ENUM ('high', 'normal', 'low');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE job_status AS ENUM ('queued', 'delayed', 'active', 'completed', 'failed', 'dead');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 6.1 jobs
CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type             VARCHAR(255) NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority         job_priority NOT NULL DEFAULT 'normal',
    status           job_status NOT NULL DEFAULT 'queued',
    attempt          INT NOT NULL DEFAULT 0,
    max_retries      INT NOT NULL DEFAULT 3,
    idempotency_key  VARCHAR(255),
    scheduled_for    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    error            TEXT,
    worker_id        VARCHAR(255)
);

-- 6.2 job_events (audit log)
CREATE TABLE IF NOT EXISTS job_events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    status      VARCHAR(50) NOT NULL,
    message     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6.3 scheduled_jobs (recurring definitions)
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type              VARCHAR(255) NOT NULL,
    cron_expression   VARCHAR(120) NOT NULL,
    payload_template  JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority          job_priority NOT NULL DEFAULT 'normal',
    max_retries       INT NOT NULL DEFAULT 3,
    enabled           BOOLEAN NOT NULL DEFAULT true,
    last_run_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6.4 Indexes
CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_created
    ON jobs (status, priority, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_job_events_job_created
    ON job_events (job_id, created_at);

CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (type);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_enabled ON scheduled_jobs (enabled);

-- keep updated_at fresh
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_scheduled_jobs_updated_at ON scheduled_jobs;
CREATE TRIGGER trg_scheduled_jobs_updated_at
    BEFORE UPDATE ON scheduled_jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
