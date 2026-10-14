-- Screen hits and near-misses as a measured population.
--
-- Every stock-week the screen flags (scan_results) or nearly flags
-- (scan_near_misses: failed exactly one rule by a small margin) becomes one
-- screen_observation with a point-in-time feature snapshot as of its signal
-- week and fixed-horizon outcomes. This gives rated picks a comparison group
-- (hits nobody picked) and lets each screen rule be tested against stocks that
-- only just failed it.

CREATE TABLE IF NOT EXISTS scan_near_misses (
    id BIGSERIAL PRIMARY KEY,
    scan_id BIGINT NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    signal_date DATE NOT NULL,
    failed_rule TEXT NOT NULL,
    observed_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
    sector TEXT,
    industry TEXT,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (scan_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_scan_near_misses_ticker_signal
    ON scan_near_misses (ticker, signal_date);

CREATE TABLE IF NOT EXISTS screen_observations (
    id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL CHECK (market IN ('asx', 'us')),
    provider TEXT NOT NULL,
    ticker TEXT NOT NULL,
    signal_date DATE NOT NULL,
    feature_version INTEGER NOT NULL,
    snapshot_status TEXT NOT NULL DEFAULT 'queued'
        CHECK (snapshot_status IN ('queued', 'complete', 'partial', 'failed')),
    feature_as_of_date DATE,
    technical_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    fundamental_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    first_seen_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at_utc TIMESTAMPTZ,
    UNIQUE (market, provider, ticker, signal_date, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_screen_observations_queue
    ON screen_observations (feature_version, snapshot_status, market, provider, ticker);

CREATE TABLE IF NOT EXISTS screen_observation_outcomes (
    observation_id BIGINT NOT NULL REFERENCES screen_observations(id) ON DELETE CASCADE,
    horizon_days INTEGER NOT NULL,
    measured_at_utc TIMESTAMPTZ NOT NULL,
    price_at_signal DOUBLE PRECISION,
    price_at_horizon DOUBLE PRECISION,
    return_percent DOUBLE PRECISION,
    outcome_date DATE,
    benchmark_ticker TEXT,
    benchmark_return_percent DOUBLE PRECISION,
    benchmark_excess_return_percent DOUBLE PRECISION,
    maximum_gain_percent DOUBLE PRECISION,
    maximum_drawdown_percent DOUBLE PRECISION,
    days_to_maximum_gain INTEGER,
    target_hit BOOLEAN,
    stop_hit BOOLEAN,
    target_hit_at DATE,
    stop_hit_at DATE,
    outcome_version INTEGER NOT NULL,
    quality_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (observation_id, horizon_days)
);

DO $$
DECLARE
    role_name text;
    table_name text;
    all_roles text[] := ARRAY[
        'firebasereader_moneymaker_public',
        'firebasewriter_moneymaker_public',
        'firebaseowner_moneymaker_public'
    ];
    tables text[] := ARRAY['scan_near_misses', 'screen_observations', 'screen_observation_outcomes'];
BEGIN
    FOREACH role_name IN ARRAY all_roles LOOP
        IF to_regrole(role_name) IS NOT NULL THEN
            EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %I', role_name);
            FOREACH table_name IN ARRAY tables LOOP
                EXECUTE format('GRANT SELECT ON TABLE public.%I TO %I', table_name, role_name);
            END LOOP;
        END IF;
    END LOOP;
END $$;
