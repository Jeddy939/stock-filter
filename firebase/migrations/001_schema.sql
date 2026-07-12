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

-- Derived weekly candles used by the screener. Daily price_history remains the
-- authoritative source for charting and can rebuild this table at any time.
CREATE TABLE IF NOT EXISTS weekly_price_history (
    market TEXT NOT NULL,
    provider TEXT NOT NULL,
    ticker TEXT NOT NULL,
    week_date DATE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    refreshed_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, provider, ticker, week_date)
);

CREATE INDEX IF NOT EXISTS idx_weekly_price_history_lookup
    ON weekly_price_history (market, provider, ticker, week_date DESC);

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

-- Shared scans are readable by all authenticated users. Appraisals are private
-- to the Firebase identity that created them.
CREATE TABLE IF NOT EXISTS app_user_invites (
    email TEXT PRIMARY KEY,
    role TEXT NOT NULL DEFAULT 'analyst',
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('pending', 'active', 'disabled')),
    invited_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE app_user_invites DROP CONSTRAINT IF EXISTS app_user_invites_role_check;
ALTER TABLE app_user_invites ALTER COLUMN role SET DEFAULT 'analyst';
UPDATE app_user_invites
SET role = CASE role WHEN 'owner' THEN 'admin' WHEN 'member' THEN 'analyst' ELSE role END;
ALTER TABLE app_user_invites ADD CONSTRAINT app_user_invites_role_check
    CHECK (role IN ('admin', 'analyst', 'viewer'));

INSERT INTO app_user_invites (email, role)
VALUES
    ('mrbowcock@gmail.com', 'admin'),
    ('brady.bowcock@gmail.com', 'analyst'),
    ('damien.sundgren@gmail.com', 'analyst')
ON CONFLICT (email) DO UPDATE SET role = EXCLUDED.role;

CREATE TABLE IF NOT EXISTS user_profiles (
    firebase_uid TEXT PRIMARY KEY,
    email TEXT UNIQUE,
    display_name TEXT,
    role TEXT NOT NULL DEFAULT 'analyst',
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('pending', 'active', 'disabled')),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE user_profiles DROP CONSTRAINT IF EXISTS user_profiles_role_check;
ALTER TABLE user_profiles ALTER COLUMN role SET DEFAULT 'viewer';
UPDATE user_profiles
SET role = CASE role WHEN 'owner' THEN 'admin' WHEN 'member' THEN 'analyst' ELSE role END;
ALTER TABLE user_profiles ADD CONSTRAINT user_profiles_role_check
    CHECK (role IN ('admin', 'analyst', 'viewer'));

CREATE TABLE IF NOT EXISTS user_appraisals (
    firebase_uid TEXT NOT NULL REFERENCES user_profiles(firebase_uid) ON DELETE CASCADE,
    scan_id BIGINT NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    label TEXT CHECK (label IN ('winner', 'potential_winner', 'maybe', 'bad')),
    note TEXT,
    status TEXT,
    appraised_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (firebase_uid, scan_id, source_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_user_appraisals_scan
    ON user_appraisals (firebase_uid, scan_id, ticker);

CREATE TABLE IF NOT EXISTS user_picks (
    firebase_uid TEXT NOT NULL REFERENCES user_profiles(firebase_uid) ON DELETE CASCADE,
    scan_id BIGINT NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
    source_id BIGINT NOT NULL,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    label TEXT NOT NULL CHECK (label IN ('winner', 'potential_winner', 'maybe', 'bad')),
    status TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (firebase_uid, scan_id, source_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_user_picks_user_updated
    ON user_picks (firebase_uid, updated_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_user_picks_scan
    ON user_picks (firebase_uid, scan_id, ticker);

CREATE TABLE IF NOT EXISTS user_notes (
    id BIGSERIAL PRIMARY KEY,
    firebase_uid TEXT NOT NULL REFERENCES user_profiles(firebase_uid) ON DELETE CASCADE,
    scan_id BIGINT REFERENCES scan_runs(id) ON DELETE CASCADE,
    source_id BIGINT,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    note TEXT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (firebase_uid, scan_id, source_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_user_notes_user_updated
    ON user_notes (firebase_uid, updated_at_utc DESC);

INSERT INTO user_picks
    (firebase_uid, scan_id, source_id, market, ticker, label, status, created_at_utc, updated_at_utc)
SELECT firebase_uid, scan_id, source_id, market, ticker, label, status, appraised_at_utc, appraised_at_utc
FROM user_appraisals
WHERE label IS NOT NULL
ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
    label = EXCLUDED.label,
    status = EXCLUDED.status,
    updated_at_utc = EXCLUDED.updated_at_utc;

INSERT INTO user_notes
    (firebase_uid, scan_id, source_id, market, ticker, note, created_at_utc, updated_at_utc)
SELECT firebase_uid, scan_id, source_id, market, ticker, note, appraised_at_utc, appraised_at_utc
FROM user_appraisals
WHERE note IS NOT NULL AND btrim(note) <> ''
ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
    note = EXCLUDED.note,
    updated_at_utc = EXCLUDED.updated_at_utc;

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

ALTER TABLE rating_events ADD COLUMN IF NOT EXISTS firebase_uid TEXT;
ALTER TABLE rating_events ADD COLUMN IF NOT EXISTS user_email TEXT;

CREATE INDEX IF NOT EXISTS idx_rating_events_user
    ON rating_events (firebase_uid, event_at_utc DESC);

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
