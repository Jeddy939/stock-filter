-- Transparent, deduplicated screen jobs and reproducible schema migrations.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS dedupe_key TEXT,
    ADD COLUMN IF NOT EXISTS config_hash TEXT;

ALTER TABLE scan_runs
    ADD COLUMN IF NOT EXISTS config_hash TEXT,
    ADD COLUMN IF NOT EXISTS market_snapshot_date DATE;

CREATE TABLE IF NOT EXISTS job_events (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    stage_code TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
    current_count BIGINT NOT NULL DEFAULT 0,
    total_count BIGINT,
    percent DOUBLE PRECISION,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_events_poll
    ON job_events (job_id, id);

CREATE INDEX IF NOT EXISTS idx_job_events_created
    ON job_events (created_at_utc);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_runs_active_dedupe
    ON job_runs (job_type, market, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS idx_scan_runs_latest_config
    ON scan_runs (market, config_hash, created_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_job_runs_recent_market
    ON job_runs (job_type, market, started_at_utc DESC);

DO $$
DECLARE
    reader_role text := 'firebasereader_moneymaker_public';
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = reader_role) THEN
        EXECUTE format('GRANT SELECT ON TABLE job_events, schema_migrations TO %I', reader_role);
    END IF;
END $$;
