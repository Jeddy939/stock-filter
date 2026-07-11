-- Canonical online schema for MoneyMaker.
-- Raw SQLite files remain backup/import sources; they are not the live database.

CREATE TABLE IF NOT EXISTS companies (
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    info_json JSONB NOT NULL,
    fetched_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market, ticker)
);

CREATE TABLE IF NOT EXISTS price_history (
    market TEXT NOT NULL,
    provider TEXT NOT NULL,
    ticker TEXT NOT NULL,
    price_date DATE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    fetched_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market, provider, ticker, price_date)
);

CREATE INDEX IF NOT EXISTS idx_price_history_lookup
    ON price_history (market, provider, ticker, price_date DESC);

CREATE TABLE IF NOT EXISTS scan_runs (
    id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL,
    source_id BIGINT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    provider TEXT NOT NULL,
    cache_file TEXT NOT NULL,
    years INTEGER,
    limit_count INTEGER,
    query TEXT,
    scanned_count INTEGER NOT NULL,
    result_count INTEGER NOT NULL,
    skipped_no_history INTEGER NOT NULL,
    config_json JSONB NOT NULL,
    ticker_universe_json JSONB NOT NULL,
    UNIQUE (market, source_id)
);

CREATE TABLE IF NOT EXISTS scan_results (
    id BIGSERIAL PRIMARY KEY,
    scan_id BIGINT NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL,
    rank INTEGER NOT NULL,
    ticker TEXT NOT NULL,
    signal_date DATE,
    close_price DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    avg_volume DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
    sector TEXT,
    industry TEXT,
    result_json JSONB NOT NULL,
    UNIQUE (scan_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_scan_results_ticker
    ON scan_results (ticker);

CREATE TABLE IF NOT EXISTS scan_labels (
    id BIGSERIAL PRIMARY KEY,
    scan_id BIGINT NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL,
    ticker TEXT NOT NULL,
    label TEXT NOT NULL CHECK (label IN ('winner', 'potential_winner', 'maybe', 'bad')),
    note TEXT,
    labeled_at_utc TIMESTAMPTZ NOT NULL,
    UNIQUE (scan_id, source_id, ticker)
);

CREATE TABLE IF NOT EXISTS rating_events (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT,
    event_at_utc TIMESTAMPTZ NOT NULL,
    action TEXT NOT NULL,
    rated_by TEXT,
    market TEXT,
    cache_file TEXT,
    scan_id BIGINT,
    scan_created_at_utc TIMESTAMPTZ,
    provider TEXT,
    query TEXT,
    ticker TEXT NOT NULL,
    label TEXT,
    note TEXT,
    rank INTEGER,
    signal_date DATE,
    close_price DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    avg_volume DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
    sector TEXT,
    industry TEXT,
    result_json JSONB,
    yahoo_url TEXT,
    UNIQUE (source_id, event_at_utc, ticker, action)
);

CREATE INDEX IF NOT EXISTS idx_rating_events_ticker
    ON rating_events (market, ticker, event_at_utc DESC);

CREATE TABLE IF NOT EXISTS rating_outcomes (
    rating_event_id BIGINT NOT NULL REFERENCES rating_events(id) ON DELETE CASCADE,
    horizon_days INTEGER NOT NULL,
    measured_at_utc TIMESTAMPTZ NOT NULL,
    price_at_signal DOUBLE PRECISION,
    price_at_horizon DOUBLE PRECISION,
    return_percent DOUBLE PRECISION,
    PRIMARY KEY (rating_event_id, horizon_days)
);

CREATE TABLE IF NOT EXISTS job_runs (
    id UUID PRIMARY KEY,
    job_type TEXT NOT NULL,
    market TEXT,
    status TEXT NOT NULL,
    stage TEXT,
    current_count BIGINT NOT NULL DEFAULT 0,
    total_count BIGINT,
    percent DOUBLE PRECISION NOT NULL DEFAULT 0,
    detail TEXT,
    log_tail TEXT,
    started_at_utc TIMESTAMPTZ NOT NULL,
    finished_at_utc TIMESTAMPTZ,
    error TEXT,
    parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_job_runs_recent
    ON job_runs (started_at_utc DESC);
