-- Immutable point-in-time appraisal snapshots and research outputs.

CREATE TABLE IF NOT EXISTS feature_definitions (
    id BIGSERIAL PRIMARY KEY,
    feature_name TEXT NOT NULL,
    feature_version INTEGER NOT NULL,
    category TEXT NOT NULL,
    value_type TEXT NOT NULL CHECK (value_type IN ('numeric', 'boolean', 'categorical')),
    unit TEXT,
    description TEXT NOT NULL,
    formula TEXT NOT NULL,
    required_history_days INTEGER,
    source_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (feature_name, feature_version)
);

CREATE TABLE IF NOT EXISTS pick_feature_snapshots (
    id BIGSERIAL PRIMARY KEY,
    rating_event_id BIGINT NOT NULL REFERENCES rating_events(id) ON DELETE CASCADE,
    origin_event_id BIGINT REFERENCES rating_events(id) ON DELETE SET NULL,
    firebase_uid TEXT NOT NULL,
    market TEXT NOT NULL CHECK (market IN ('asx', 'us')),
    ticker TEXT NOT NULL,
    appraisal_label TEXT NOT NULL,
    appraisal_at_utc TIMESTAMPTZ NOT NULL,
    feature_as_of_date DATE,
    feature_version INTEGER NOT NULL,
    snapshot_status TEXT NOT NULL DEFAULT 'queued'
        CHECK (snapshot_status IN ('queued', 'running', 'complete', 'partial', 'failed')),
    technical_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    fundamental_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at_utc TIMESTAMPTZ,
    UNIQUE (rating_event_id, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_pick_feature_snapshots_cohort
    ON pick_feature_snapshots (market, appraisal_label, feature_as_of_date, snapshot_status);
CREATE INDEX IF NOT EXISTS idx_pick_feature_snapshots_owner
    ON pick_feature_snapshots (firebase_uid, appraisal_at_utc DESC);

CREATE TABLE IF NOT EXISTS pick_feature_values (
    snapshot_id BIGINT NOT NULL REFERENCES pick_feature_snapshots(id) ON DELETE CASCADE,
    feature_definition_id BIGINT NOT NULL REFERENCES feature_definitions(id),
    numeric_value DOUBLE PRECISION,
    boolean_value BOOLEAN,
    categorical_value TEXT,
    is_missing BOOLEAN NOT NULL DEFAULT FALSE,
    missing_reason TEXT,
    source_as_of_utc TIMESTAMPTZ,
    PRIMARY KEY (snapshot_id, feature_definition_id),
    CHECK (
        (is_missing AND num_nonnulls(numeric_value, boolean_value, categorical_value) = 0)
        OR
        (NOT is_missing AND num_nonnulls(numeric_value, boolean_value, categorical_value) = 1)
    )
);

CREATE INDEX IF NOT EXISTS idx_pick_feature_values_numeric
    ON pick_feature_values (feature_definition_id, numeric_value)
    WHERE numeric_value IS NOT NULL;

CREATE TABLE IF NOT EXISTS fundamental_facts (
    id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    source_name TEXT NOT NULL,
    accession_id TEXT NOT NULL,
    fact_name TEXT NOT NULL,
    unit TEXT NOT NULL,
    period_start DATE,
    period_end DATE NOT NULL,
    filed_at_utc TIMESTAMPTZ NOT NULL,
    fiscal_year INTEGER,
    fiscal_period TEXT,
    form_type TEXT,
    numeric_value DOUBLE PRECISION,
    source_fact_key TEXT NOT NULL,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (market, ticker, source_name, source_fact_key)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_facts_point_in_time
    ON fundamental_facts (market, ticker, filed_at_utc DESC, fact_name);

ALTER TABLE rating_outcomes
    ADD COLUMN IF NOT EXISTS outcome_date DATE,
    ADD COLUMN IF NOT EXISTS benchmark_ticker TEXT,
    ADD COLUMN IF NOT EXISTS benchmark_return_percent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS benchmark_excess_return_percent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sector_return_percent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sector_excess_return_percent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS maximum_gain_percent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS maximum_drawdown_percent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS days_to_maximum_gain INTEGER,
    ADD COLUMN IF NOT EXISTS target_hit BOOLEAN,
    ADD COLUMN IF NOT EXISTS stop_hit BOOLEAN,
    ADD COLUMN IF NOT EXISTS target_hit_at DATE,
    ADD COLUMN IF NOT EXISTS stop_hit_at DATE,
    ADD COLUMN IF NOT EXISTS outcome_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS quality_json JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS insight_runs (
    id UUID PRIMARY KEY,
    requested_by_uid TEXT NOT NULL,
    requested_by_email TEXT,
    scope TEXT NOT NULL CHECK (scope IN ('mine', 'team', 'owner')),
    owner_uid TEXT,
    market TEXT NOT NULL CHECK (market IN ('asx', 'us', 'all')),
    horizon_days INTEGER NOT NULL,
    feature_version INTEGER NOT NULL,
    outcome_version INTEGER NOT NULL,
    config_hash TEXT NOT NULL,
    configuration_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    cohort_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    performance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    stage TEXT,
    error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at_utc TIMESTAMPTZ,
    finished_at_utc TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_insight_runs_latest
    ON insight_runs (requested_by_uid, market, horizon_days, created_at_utc DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_insight_runs_active_config
    ON insight_runs (requested_by_uid, config_hash)
    WHERE status IN ('queued', 'running');

CREATE TABLE IF NOT EXISTS insight_findings (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES insight_runs(id) ON DELETE CASCADE,
    finding_id TEXT NOT NULL,
    finding_type TEXT NOT NULL CHECK (finding_type IN ('univariate', 'interaction', 'cluster', 'rule')),
    title TEXT NOT NULL,
    explanation TEXT NOT NULL,
    condition_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    feature_names_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    support_count INTEGER NOT NULL,
    baseline_hit_rate DOUBLE PRECISION,
    finding_hit_rate DOUBLE PRECISION,
    lift DOUBLE PRECISION,
    average_excess_return DOUBLE PRECISION,
    median_excess_return DOUBLE PRECISION,
    drawdown_effect DOUBLE PRECISION,
    confidence_interval_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    adjusted_p_value DOUBLE PRECISION,
    in_sample_metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    out_of_sample_metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    counterexample_count INTEGER NOT NULL DEFAULT 0,
    counterexamples_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('exploratory', 'candidate', 'validated', 'rejected')),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, finding_id)
);

CREATE INDEX IF NOT EXISTS idx_insight_findings_run
    ON insight_findings (run_id, status, finding_type);

CREATE TABLE IF NOT EXISTS insight_rule_sets (
    id UUID PRIMARY KEY,
    owner_uid TEXT NOT NULL,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    condition_json JSONB NOT NULL,
    source_finding_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('draft', 'shadow', 'approved', 'retired')),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at_utc TIMESTAMPTZ,
    UNIQUE (owner_uid, name, version)
);

CREATE INDEX IF NOT EXISTS idx_insight_rule_sets_active
    ON insight_rule_sets (owner_uid, status, created_at_utc DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_runs_active_dedupe_nullsafe
    ON job_runs (job_type, COALESCE(market, ''), dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running');

CREATE TABLE IF NOT EXISTS insight_rule_evaluations (
    id BIGSERIAL PRIMARY KEY,
    rule_set_id UUID NOT NULL REFERENCES insight_rule_sets(id) ON DELETE CASCADE,
    evaluated_from DATE,
    evaluated_to DATE,
    eligible_count INTEGER NOT NULL DEFAULT 0,
    retained_count INTEGER NOT NULL DEFAULT 0,
    removed_count INTEGER NOT NULL DEFAULT 0,
    high_retained_count INTEGER NOT NULL DEFAULT 0,
    high_removed_count INTEGER NOT NULL DEFAULT 0,
    low_removed_count INTEGER NOT NULL DEFAULT 0,
    low_retained_count INTEGER NOT NULL DEFAULT 0,
    baseline_hit_rate DOUBLE PRECISION,
    filtered_hit_rate DOUBLE PRECISION,
    benchmark_excess_change DOUBLE PRECISION,
    drawdown_change DOUBLE PRECISION,
    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
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
    tables text[] := ARRAY[
        'feature_definitions', 'pick_feature_snapshots', 'pick_feature_values',
        'fundamental_facts', 'insight_runs', 'insight_findings',
        'insight_rule_sets', 'insight_rule_evaluations'
    ];
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
