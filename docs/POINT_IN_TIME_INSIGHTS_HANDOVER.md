# MoneyMaker Point-in-Time Insights Implementation Handover

## 1. Objective

Build an Insights workspace inside the existing Analysis page that discovers
which characteristics were present, or absent, when a stock was appraised and
whether those characteristics predicted later performance.

The system must:

1. Reconstruct each stock exactly as it was knowable at the appraisal time.
2. Compare mature high-performing winner appraisals with mature
   underperforming winner appraisals.
3. Search technical, volume, volatility, market, industry, fundamental, event,
   and appraisal-context features for repeatable patterns and interactions.
4. Show evidence, counterexamples, sample size, uncertainty, and
   out-of-sample performance for every finding.
5. Turn accepted findings into versioned shadow rules for a future refined
   winner screen.
6. Never use future information, revised/current fundamentals, or later labels
   as inputs to an earlier appraisal.

This is a research and decision-support feature. It must not claim guaranteed
returns or present an uncalibrated model score as a probability.

## 2. Repository and Current State

Repository:

```text
D:\jmbow1\My Documents\MoneyMaker\stock-filter
```

Current branch at handover time:

```text
codex/minimal-ui-transparent-scans
```

Relevant production architecture:

- `hosting/index.html`: single-file Firebase Hosting UI.
- `functions/src/api.ts`: authenticated Firebase Functions Express API.
- `functions/src/auth.ts`: Firebase Auth and application roles.
- `firebase/worker.py`: long-running Cloud Run worker entry point.
- `cloud_backend/`: PostgreSQL screening and market processing logic.
- `firebase/migrations/`: ordered additive PostgreSQL migrations.
- `firebase/schema.py`: migration runner and `schema_migrations` tracking.
- `price_history`: authoritative daily OHLCV table.
- `weekly_price_history` and `weekly_metrics`: derived screening data.
- `rating_events`: immutable appraisal event history.
- `rating_outcomes`: current fixed-horizon return storage.
- `scan_runs` and `scan_results`: mechanical-screen history and controls.

Existing Analysis API endpoints that must remain backward compatible:

```text
GET /api/analysis/summary
GET /api/analysis/timeseries
GET /api/analysis/insights
GET /api/user/picks
GET /api/user/rating-history
GET /api/chart
```

The existing `/api/analysis/insights` endpoint only reports negative winners
and simple sector, industry, and market-cap aggregates. Extend the feature
without silently changing that response contract.

### Dirty-worktree warning

At handover time, the worktree contains:

```text
M  hosting/index.html
M  us_tickers_nasdaqtrader.txt
?? export_scan_labels.zip
?? hosting/assets/
```

The `hosting/index.html` and `hosting/assets/vendor/lightweight-charts/` changes
are an in-progress, tested TradingView Lightweight Charts integration. Preserve
them and build appraisal replay on top of them.

The ticker file and export ZIP are unrelated user/generated changes. Do not
modify, delete, stage, or revert them.

Do not use `git reset --hard`, `git checkout --`, or any broad cleanup command.

### Worker execution contract

The implementing worker must follow this sequence exactly:

1. Read this entire handover before editing anything.
2. Run `git status --short` and preserve every pre-existing dirty file listed
   above. If the status differs, treat new changes as user-owned until proven
   otherwise.
3. Inspect the current definitions of `rating_events`, `rating_outcomes`,
   `job_runs`, `job_events`, API authentication helpers, worker dispatch, and
   Analysis rendering. Do not implement from this document without reconciling
   names and types with the checked-out code.
4. Implement only Phase 1 first. Do not start Phase 2 until Phase 1 tests pass
   and its diff has been reviewed.
5. Use additive migrations and backward-compatible API changes. Do not rename
   or remove an existing production field or endpoint.
6. Use transactions around related writes and idempotency keys around all
   asynchronous work. A retry must not duplicate snapshots, facts, findings,
   or jobs.
7. Add tests in the same change as each behavior. A phase is not complete when
   code exists; it is complete when its stated tests pass.
8. Stop and ask the owner when a credential, Firebase login, production
   database URL, billing action, secret, or deployment approval is required.
   Do not seek a workaround for missing authorization.
9. Do not deploy from an unreviewed dirty worktree. Produce a focused diff and
   verification report first.
10. After each phase, report changed files, migration impact, tests run,
    failures or omissions, and the exact next phase. Never describe a test as
    passing unless its command actually ran successfully.

Before Phase 1 coding, the worker must write a short implementation map naming:

- The two exact appraisal-write functions/routes it will modify.
- The existing job dispatch and polling helpers it will reuse.
- The migration runner behavior it verified.
- The chart query that will enforce the replay cutoff.
- The test files it will create or extend.

If any of these cannot be identified from the repository, stop that part and
report the missing fact rather than guessing.

## 3. Non-Negotiable Point-in-Time Rules

### 3.1 Appraisal event identity

Treat every row in `rating_events` as a distinct historical decision. Do not
collapse events to the latest label when constructing training records.

For a winner model, the default research event is the exact event where the
user assigned `label = 'winner'`.

If a stock moved from `needs_confirmation` to `winner`, preserve both:

- `origin_event_id`: the first non-empty appraisal event for that user, market,
  and ticker in the relevant appraisal sequence.
- `decision_event_id`: the event where it became a winner.

Default feature timing for winner analysis is `decision_event_id`. Allow a
future UI comparison against initial-selection features, but never mix those
timings in one cohort.

Do not use a later relabel, note, confirmation, or observed price path to alter
an earlier feature snapshot.

### 3.2 Market cutoff

For each event, calculate:

```text
feature_as_of_date = latest available market session where
                     price_date <= appraisal event timestamp/date
```

Only query price bars where `price_date <= feature_as_of_date`.

Do not use the database's newest bar and then trim the response in the browser.
The SQL query itself must enforce the cutoff.

### 3.3 Fundamental cutoff

A fundamental fact is eligible only when:

```text
filed_at_utc <= appraisal event_at_utc
```

Do not use the fiscal period end as the availability date. A quarter ending in
March was not public until its filing date.

Do not backfill historical P/E, market cap, earnings estimates, or margins from
today's `companies.info_json`. That object is a current profile snapshot.

### 3.4 Adjusted-price consistency

All price-derived features in one snapshot must use the same price basis.
Respect the existing `price_history` basis work from migration
`007_price_history_basis.sql`. Never compare an adjusted close with unadjusted
high/low or moving averages.

### 3.5 Outcome maturity

An event is mature for horizon `H` only when market data exists at or after:

```text
feature_as_of_date + H calendar days
```

Use the first market session on or after the target date, with a maximum
seven-calendar-day tolerance. If no eligible bar exists, mark the outcome
unavailable. Do not substitute the latest price for a fixed-horizon outcome.

## 4. Scope and Privacy

All Insights endpoints require Firebase Auth.

Default scope is `mine`, restricted to `firebase_uid = authenticated user.uid`.

Support:

```text
scope=mine
scope=team
owner_uid=<uid>
```

Rules:

- Analysts can use `scope=mine`.
- Admins can use `scope=mine`, `scope=team`, or an explicit owner UID.
- Do not expose another user's notes through Insights.
- Team analysis may aggregate features and outcomes, but raw user notes remain
  private.
- Apply these rules server-side. Hiding a control in the browser is not access
  control.

Do not weaken the existing Analysis-tab access for Brady, Damien, or future
authenticated analysts.

## 5. Database Migration

Create one additive migration:

```text
firebase/migrations/010_point_in_time_insights.sql
```

Do not edit `001_schema.sql` to deploy this feature. Existing databases have
already recorded that migration.

### 5.1 `feature_definitions`

Create a registry so every feature is auditable and agent-readable. Feature
definitions are immutable by version; changing a formula creates a new row:

```sql
CREATE TABLE feature_definitions (
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
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (feature_name, feature_version)
);
```

### 5.2 `pick_feature_snapshots`

One immutable snapshot per rating event and feature-set version:

```sql
CREATE TABLE pick_feature_snapshots (
    id BIGSERIAL PRIMARY KEY,
    rating_event_id BIGINT NOT NULL REFERENCES rating_events(id) ON DELETE CASCADE,
    origin_event_id BIGINT REFERENCES rating_events(id) ON DELETE SET NULL,
    firebase_uid TEXT NOT NULL,
    market TEXT NOT NULL CHECK (market IN ('asx', 'us')),
    ticker TEXT NOT NULL,
    appraisal_label TEXT NOT NULL,
    appraisal_at_utc TIMESTAMPTZ NOT NULL,
    feature_as_of_date DATE NOT NULL,
    feature_version INTEGER NOT NULL,
    snapshot_status TEXT NOT NULL DEFAULT 'queued'
        CHECK (snapshot_status IN ('queued', 'running', 'complete', 'partial', 'failed')),
    technical_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    fundamental_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at_utc TIMESTAMPTZ,
    UNIQUE (rating_event_id, feature_version)
);
```

Indexes:

```sql
CREATE INDEX idx_pick_feature_snapshots_cohort
    ON pick_feature_snapshots (market, appraisal_label, feature_as_of_date, snapshot_status);

CREATE INDEX idx_pick_feature_snapshots_owner
    ON pick_feature_snapshots (firebase_uid, appraisal_at_utc DESC);
```

### 5.3 `pick_feature_values`

Store queryable feature values separately from the audit JSON:

```sql
CREATE TABLE pick_feature_values (
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

CREATE INDEX idx_pick_feature_values_numeric
    ON pick_feature_values (feature_definition_id, numeric_value)
    WHERE numeric_value IS NOT NULL;
```

### 5.4 `fundamental_facts`

Create point-in-time filing facts. This is initially most useful for US SEC
filings:

```sql
CREATE TABLE fundamental_facts (
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
    fetched_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market, ticker, source_name, source_fact_key)
);

CREATE INDEX idx_fundamental_facts_point_in_time
    ON fundamental_facts (market, ticker, filed_at_utc DESC, fact_name);
```

Build `source_fact_key` deterministically from all source identity fields,
including accession, concept, unit, start, end, form, fiscal year/period, and
frame. This prevents valid SEC facts with the same period end from overwriting
one another while keeping ingestion retries idempotent.

Do not invent ASX historical fundamentals. Store unavailable fields as missing
with a reason. Prospective ASX snapshots can begin immediately while a reliable
historical filing source is evaluated.

### 5.5 Extend `rating_outcomes`

Add columns rather than creating a competing outcomes table:

```sql
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
```

Benchmark mapping for the first version:

```text
ASX -> ^AORD
US  -> SPY
```

Both already exist in migration `004_market_benchmarks.sql`.

### 5.6 Insight result tables

Create:

```text
insight_runs
insight_findings
insight_rule_sets
insight_rule_evaluations
```

Minimum fields:

`insight_runs`:

- UUID ID.
- Requested user UID and email.
- Scope and optional owner UID.
- Market and horizon.
- Feature and outcome versions.
- Configuration JSON.
- Cohort counts.
- Training, validation, and holdout date ranges.
- Status, stage, error, timestamps, and performance JSON.

`insight_findings`:

- Run ID and stable finding ID.
- Finding type: `univariate`, `interaction`, `cluster`, or `rule`.
- Human title and deterministic explanation.
- Condition JSON.
- Feature names JSON.
- Support count.
- Baseline and finding hit rates.
- Lift, average excess return, median excess return, and drawdown effect.
- Confidence interval and adjusted p-value where applicable.
- In-sample and out-of-sample metrics JSON.
- Counterexample count and examples JSON.
- Status: `exploratory`, `candidate`, `validated`, or `rejected`.

`insight_rule_sets`:

- Owner UID.
- Name, version, and immutable condition JSON.
- Source finding IDs.
- Status: `draft`, `shadow`, `approved`, `retired`.
- Created and approved timestamps.

`insight_rule_evaluations`:

- Rule-set ID and evaluation date/range.
- Eligible, retained, and removed counts.
- High performers retained and incorrectly removed.
- Underperformers removed and retained.
- Baseline versus filtered hit rate, alpha, and drawdown.
- Full metrics JSON.

Add indexes for latest run lookup, finding lookup by run, and active shadow rules.

Grant the same read/write roles used by the existing application tables. Do not
grant unauthenticated/public access.

## 6. Snapshot Creation Workflow

### 6.1 New appraisals

Modify both rating write paths in `functions/src/api.ts`:

- The legacy-compatible rating path around the existing `rating_events` insert.
- The current `/api/scan/appraisal` path around its `rating_events` insert.

Required transaction sequence:

1. Insert the rating event and obtain its ID.
2. If it is a label action, insert a queued snapshot stub with the fixed event
   timestamp and computed `feature_as_of_date`.
3. Commit the appraisal transaction.
4. Dispatch an idempotent snapshot task after commit.
5. Return the appraisal response immediately. Do not make rating clicks wait for
   SEC or analytical processing.

The unique key `(rating_event_id, feature_version)` makes retries safe.

### 6.2 Existing appraisals

Add an admin-only asynchronous backfill job:

```text
POST /api/admin/insights/backfill-snapshots
```

Parameters:

```json
{
  "market": "all",
  "owner_uid": null,
  "labels": ["winner", "maybe", "bad", "needs_confirmation"],
  "feature_version": 1,
  "resume": true
}
```

The job must:

- Process events in chronological order.
- Skip existing complete snapshots for the same version.
- Retry partial/failed snapshots safely.
- Emit `job_events` progress.
- Report complete, partial, failed, and insufficient-history counts.
- Never rewrite a complete immutable snapshot. A changed formula requires a new
  feature version.

### 6.3 Worker placement

Do not run large backfills or model searches inside a normal HTTP request.

Add worker job types to the existing Cloud Run/job framework:

```text
insight-snapshot-backfill
insight-run
fundamentals-refresh
```

Update all job-type allowlists and stale-job reconciliation deliberately.

## 7. Feature Engineering Version 1

Implement deterministic functions in a new module:

```text
cloud_backend/insights/features.py
```

Do not place hundreds of feature calculations directly in `functions/src/api.ts`.

Each feature function must accept only data already cut off at the appraisal
date. Each returns value, source timestamp, and missing reason.

### 7.1 Price and trend

Implement:

- Returns: 1, 5, 20, 60, 120, and 252 trading days.
- Relative returns versus benchmark for 20, 60, 120, and 252 days.
- Return acceleration: recent 20-day return minus preceding 20-day return.
- Distance percentage from MA 30, 90, 180, 360, and 700.
- MA slope over 4, 13, and 26 weeks where history permits.
- MA slope acceleration.
- MA alignment flags.
- Sessions/weeks since last cross and touch of each MA.
- Distance from 52-week and all-time high.
- Breakout age and prior failed-breakout count.
- Linear trend slope and R-squared over 20, 60, and 120 days.
- Kaufman-style price efficiency ratio over 20 and 60 days.
- Gap percentage and five-day gap retention.

Use prior bars only. Do not include an incomplete future weekly candle.

### 7.2 Volume and liquidity

Implement:

- Relative volume over 1, 5, and 20 days.
- Weekly volume ratio versus the prior 52 completed weeks.
- Number of elevated-volume sessions in the last 5 and 20 sessions.
- Elevated-volume persistence flag.
- Up-day versus down-day volume share.
- Dollar volume averages over 20 and 60 days.
- Dollar-volume trend.
- Turnover where point-in-time shares outstanding is available.
- On-balance volume slope.
- Price-volume divergence flags.
- Amihud-style illiquidity proxy from daily return and dollar volume.
- Volume contraction before the appraisal expansion.

### 7.3 Volatility and risk

Implement:

- ATR percentage over 14 and 60 days.
- Realized volatility over 20, 60, and 120 days.
- Downside volatility.
- Volatility percentile versus the previous year.
- Volatility contraction ratio.
- Maximum drawdown over 60, 120, and 252 days.
- Recovery percentage from the latest drawdown.
- Beta and correlation versus market over 60 and 252 days.
- Idiosyncratic volatility from benchmark residuals.
- Return skew and excess kurtosis where sample size is sufficient.
- Largest positive and negative gap in the preceding 60 sessions.

### 7.4 Market, sector, and industry context

Implement:

- Market return and MA regime at appraisal.
- Market volatility regime.
- Stock relative-strength percentile in its industry and sector.
- Industry median return over 20, 60, and 120 days.
- Industry breadth: percentage of peers above MA 30, 90, and 180.
- Industry dispersion and crowding/correlation measures.
- Company market-cap percentile within its industry.
- Volume-ratio percentile within its industry.

Store peer-universe size with each percentile. Mark industry features missing
when fewer than five comparable stocks exist.

### 7.5 Fundamentals

Initial US point-in-time features:

- Trailing earnings yield and P/E when earnings are positive.
- Price-to-sales, price-to-book, EV-to-sales, and EV-to-EBITDA where inputs exist.
- Free-cash-flow yield.
- Revenue, EPS, operating income, and free-cash-flow year-over-year growth.
- Growth acceleration between the two latest comparable reported periods.
- Gross, operating, and net margins plus their change.
- ROA, ROE, and ROIC where required components exist.
- Debt-to-equity, net debt, interest coverage, and current ratio.
- Cash runway proxy for loss-making companies.
- Operating cash-flow conversion.
- Accruals proxy.
- Shares-outstanding growth and dilution.
- Asset growth and capital-expenditure intensity.
- Industry-relative percentiles for valuation, growth, profitability, and
  balance-sheet measures.

For negative earnings, P/E is missing, not zero. Keep `earnings_negative` as a
separate boolean feature.

Use SEC `data.sec.gov` server-side with a compliant User-Agent. APIs require no
key. Cache raw filing facts in `fundamental_facts`; do not call SEC once per
page render.

Do not block version 1 on perfect fundamental coverage. Expose coverage and
missingness honestly.

### 7.6 Event features

Only implement events backed by point-in-time data:

- Days since latest 10-Q/10-K/8-K or equivalent filing.
- Filing type.
- Recent share issuance/dilution evidence.
- Days to scheduled earnings only when the calendar value was captured before
  appraisal.

Do not manufacture historical analyst revisions, earnings dates, insider data,
short interest, options data, or biotech catalysts from current webpages.
These are future feature-source projects.

### 7.7 Appraisal and scan context

Implement:

- Appraisal owner UID as grouping metadata, not a predictive feature by default.
- Source scan ID, rank, signal date, and configuration hash.
- MA history tier.
- Volume ratio and market cap from the scan result.
- Number of prior appearances in scan results before appraisal.
- Number of prior appraisals for the ticker before this event.
- Days from first `needs_confirmation` event to winner confirmation.

Do not train on free-text notes in version 1.

## 8. Outcome Engine

Extend the existing rating-outcomes worker rather than creating a second
conflicting scheduler.

Required horizons:

```text
28, 56, 84, and 182 calendar days
```

For each mature rating event calculate:

- Raw return.
- Benchmark return and benchmark-excess return.
- Sector return/excess when a reliable sector series exists.
- Maximum favorable excursion.
- Maximum drawdown from appraisal price.
- Days to maximum favorable excursion.
- Whether target was reached before stop.

Default research target/stop configuration:

```json
{
  "target_percent": 15,
  "stop_percent": -12
}
```

Store the configuration/version in `quality_json`. Do not silently change
historical outcome definitions.

The primary model target is benchmark-excess performance, not merely positive
raw return.

Default high/low cohorts for exploratory comparison:

- High performer: top quartile of mature winner benchmark-excess returns within
  the same market and horizon.
- Underperformer: bottom quartile.
- Middle half: retained as controls but not used as extreme labels.

Also report the operational target-before-stop outcome separately.

## 9. Insight Analysis Engine

Create:

```text
cloud_backend/insights/analysis.py
cloud_backend/insights/models.py
cloud_backend/insights/rules.py
cloud_backend/insights/validation.py
```

### 9.1 Minimum-sample behavior

Apply these hard gates:

```text
Fewer than 20 mature winner events:
  Show coverage and individual outcomes only. No pattern claims.

20-49 mature events:
  Descriptive univariate comparisons only, explicitly exploratory.

50-199 mature events:
  Add regularized logistic regression, shallow trees, and limited two-feature
  interactions.

200 or more mature events:
  Allow calibrated gradient boosting and broader interaction discovery.
```

Do not bypass these gates because a model can technically fit the data.

### 9.2 Validation

Use chronological walk-forward validation. Never randomly shuffle stock events
across train and test sets.

Group events from the same scan/date together so market conditions do not leak
between training and test folds.

When horizons overlap, purge/embargo observations around fold boundaries.

Reserve the newest eligible period as an untouched holdout. Do not tune feature
selection, thresholds, or rules on the holdout.

Fit all preprocessing inside each training fold:

- Missing-value imputation.
- Scaling.
- Industry/category encoding.
- Feature selection.
- Probability calibration.

Never compute a full-dataset industry target encoding before splitting.

### 9.3 Analyses

Run these in order:

1. Coverage and missingness report.
2. High versus low cohort descriptive statistics.
3. Quantile outcome tables for every numeric feature.
4. Effect size and uncertainty.
5. Multiple-testing correction using Benjamini-Hochberg FDR.
6. Regularized logistic regression.
7. Shallow decision-tree rules.
8. Limited pairwise interactions selected from stable univariate candidates.
9. Gradient boosting only after the sample gate.
10. Permutation importance on validation folds.
11. Partial-dependence/ICE inspection for stable candidates.
12. Clustering performed separately for high and low performers.
13. Counterexample search.
14. Rule simplification and walk-forward retest.

Do not use in-sample tree feature importance as evidence of a valid pattern.

### 9.4 Finding acceptance

A finding can be `candidate` only when:

- Support is at least 10 mature events and at least 10 percent of its cohort.
- Direction is stable in at least two walk-forward folds.
- Out-of-sample lift is positive.
- It does not depend on one ticker, one industry, or one scan date.
- Removing the largest contributor does not reverse the finding.

A finding can be `validated` only after it also succeeds in the untouched
holdout or a later shadow period.

Always report:

- Support.
- Baseline rate.
- Finding rate and lift.
- Average and median benchmark excess.
- Drawdown effect.
- Confidence interval.
- Out-of-sample result.
- Markets/date ranges tested.
- Counterexamples.
- High performers that the rule would incorrectly remove.

### 9.5 Probability language

Use `score` until calibration is demonstrated.

Only call an output `estimated probability` when:

- It was generated out-of-fold or on holdout data.
- A calibration curve is available.
- Brier/log-loss and reliability bins are reported.
- Sample size is displayed.

## 10. API Contract

Keep the current `GET /api/analysis/insights` response operational.

Add:

### 10.1 Overview

```text
GET /api/analysis/insights/overview
  ?market=asx|us|all
  &scope=mine|team
  &owner_uid=<admin-only>
  &horizon_days=28|56|84|182
  &timing=decision|origin
```

Return:

```json
{
  "ok": true,
  "coverage": {
    "total_events": 0,
    "mature_events": 0,
    "complete_snapshots": 0,
    "partial_snapshots": 0,
    "fundamental_coverage_percent": 0,
    "earliest_appraisal": null,
    "latest_appraisal": null
  },
  "latest_run": null,
  "can_run_models": false,
  "minimum_sample_message": ""
}
```

### 10.2 Appraisal replay

```text
GET /api/analysis/insights/picks/:rating_event_id
```

Return event metadata, feature snapshot, outcome summary, and chart cutoff. Do
not include another user's private record unless admin team scope is authorized.

Extend `GET /api/chart` with optional:

```text
end_date=YYYY-MM-DD
```

The SQL must enforce `price_date <= end_date`. Return the effective cutoff.

### 10.3 Run analysis

```text
POST /api/analysis/insights/run
```

Body:

```json
{
  "market": "us",
  "scope": "mine",
  "owner_uid": null,
  "horizon_days": 84,
  "timing": "decision",
  "feature_version": 1,
  "outcome_version": 1,
  "target_percent": 15,
  "stop_percent": -12
}
```

Return an existing active identical job when one exists. Use a canonical config
hash/dedupe key. Dispatch Cloud Run and return a job ID immediately.

### 10.4 Run result

```text
GET /api/analysis/insights/runs/:run_id
```

Return run metadata, coverage, findings, validation summary, and model-card
information. Support pagination for findings.

### 10.5 Rule laboratory

```text
POST /api/analysis/insights/rules
POST /api/analysis/insights/rules/:id/evaluate
PATCH /api/analysis/insights/rules/:id
GET /api/analysis/insights/rules
```

Only admins may approve a rule. Analysts may create/evaluate their own drafts.

All API failures must return JSON through the existing error middleware. Never
allow an HTML proxy error to reach the browser as if it were JSON.

## 11. Analysis UI

Modify the existing Analysis page in `hosting/index.html`. Do not add a new
marketing page.

Add a compact internal segmented control:

```text
Performance | Insights
```

Switching to Insights should replace the Analysis workspace contents, not append
an enormous section beneath all current tables.

### 11.1 Insights controls

Provide:

- Market: ASX, US, All.
- Scope: My picks; Team only for admin.
- Timing: Winner decision; Initial selection.
- Horizon: 4, 8, 12, 26 weeks.
- `Generate insights` command.
- Latest completed run timestamp.
- Snapshot and fundamental coverage.

### 11.2 Layout order

1. Coverage and maturity strip.
2. Appraisal replay.
3. High-performer signature.
4. Underperformer warning clusters.
5. Feature explorer.
6. Interaction explorer.
7. Candidate pattern cards.
8. Rule laboratory.

Keep the design consistent with the current restrained dark UI. Do not add
gradient cards, oversized headings, decorative blobs, or nested cards.

### 11.3 Appraisal replay

The replay must:

- Select a historical winner event.
- Load a chart ending at `feature_as_of_date`.
- Show no future candles initially.
- Display features exactly as stored in the immutable snapshot.
- Show data-source timestamps and missing reasons.
- Provide `Reveal outcome` to extend only to the selected horizon.
- Clearly distinguish information known then from later outcome information.

Reuse the in-progress `drawCandles`/Lightweight Charts integration. Do not bring
back the previous hand-drawn chart renderer.

### 11.4 Feature explorer

For each feature show:

- High, low, and middle cohort distributions.
- Outcome by feature quintile.
- Sample size and missing count.
- Industry/market controls applied.
- Effect direction and stability by walk-forward fold.

### 11.5 Finding cards

Example content:

```text
Persistent-volume industry leaders

Conditions at appraisal:
  Weekly volume ratio 2.2-4.5
  Elevated volume persisted for at least two weeks
  Industry relative strength above 70th percentile
  MA180 slope positive
  Price 5-22% above MA180

Evidence:
  Support: 47 mature picks
  Baseline 12-week hit rate: 43%
  Walk-forward hit rate: 61%
  Average benchmark excess: +8.2%
  High performers incorrectly removed: 7

Status: Candidate, not yet validated
```

Actions:

- Inspect examples.
- Inspect counterexamples.
- Test as rule.
- Add to draft shadow rule set.

Do not show a finding when sample thresholds are unmet.

### 11.6 Progress

Use the existing operations drawer/job-events polling pattern. Stages:

```text
Queued
Loading point-in-time snapshots
Checking outcome maturity
Building cohorts
Testing individual features
Testing interactions
Validating patterns
Saving findings
Complete or Failed
```

Use indeterminate progress when totals are unknown. Do not invent percentages.

## 12. Refined Winner Screen Preparation

Do not change the production screening rules automatically.

Insight rules first enter `shadow` status. A shadow evaluation runs against
historical base-screen results and later live screens without hiding candidates.

Every evaluation must show:

- Eligible and retained candidates.
- Underperformers removed.
- Underperformers retained.
- High performers retained.
- High performers incorrectly removed.
- Hit-rate lift.
- Benchmark-excess-return change.
- Drawdown change.
- Market, horizon, and date range.

An admin must explicitly approve a rule before a future refined screen can use
it.

## 13. Optional LLM Explanation Layer

Do not make an LLM responsible for calculations, feature extraction, statistical
tests, or rule acceptance.

The deterministic analysis engine saves structured findings first. An optional
server-side LLM step may summarize:

- What the finding says.
- Why it may make economic/market sense.
- Known limitations.
- Counterexamples.
- Questions for the analyst.

Requirements:

- No API key in Hosting JavaScript.
- Store secrets in Google Secret Manager.
- Send structured aggregate findings, not private user notes.
- Save provider, model, prompt version, and generated timestamp.
- Clearly label generated narrative as interpretation, not evidence.
- The Insights feature must remain useful when no LLM key exists.

## 14. Tests

### 14.1 Migration tests

- Apply migrations 001 through 010 to an empty PostgreSQL database.
- Apply them again and confirm no changes/errors.
- Apply 010 to a fixture representing production through 009.
- Confirm existing ratings, users, scans, and outcomes remain intact.
- Confirm constraints and indexes exist.

### 14.2 Point-in-time tests

- No price bar after appraisal cutoff enters a feature.
- Filing with `period_end` before appraisal but `filed_at_utc` after appraisal is
  excluded.
- Later restatement does not replace an earlier snapshot.
- Relabeling does not alter the original snapshot.
- Origin and decision timing remain distinct.
- Negative earnings produce missing P/E plus `earnings_negative = true`.
- Missing fundamentals remain missing rather than zero.
- Adjusted OHLC and moving averages use one basis.

### 14.3 Outcome tests

- Fixed horizons use first trading day on/after target.
- Missing horizon bars produce unavailable outcomes.
- Benchmark return uses the same start/end convention as the stock.
- Maximum drawdown and favorable excursion are calculated only inside the
  selected horizon.
- Target-before-stop order is correct when both levels occur.
- Immature picks never enter mature model cohorts.

### 14.4 Model and validation tests

- No random shuffle is used.
- Same-date/scan events remain in one fold.
- Preprocessing is fitted per training fold.
- Holdout is not used for tuning.
- Sample gates suppress unsupported analyses.
- FDR correction is deterministic.
- Removing one dominant ticker/industry is included in robustness output.
- Seeded synthetic predictive features are found.
- Seeded noise features are not promoted as validated.
- Re-running identical config produces identical finding IDs/results.

### 14.5 Authorization tests

- Signed-out requests return JSON 401.
- Analyst can analyze own events.
- Analyst cannot request team or another owner.
- Admin can request team/owner scope.
- Raw notes never appear in team Insights responses.
- Only admin can approve rules.

### 14.6 UI and Playwright tests

- Analysis Performance and Insights switch correctly.
- No protected API requests before sign-in.
- Empty, loading, insufficient-sample, partial-data, complete, and failed states.
- Appraisal replay contains no future bars before `Reveal outcome`.
- Outcome reveal respects the chosen horizon.
- Feature missing reasons are visible.
- Finding examples/counterexamples open correctly.
- Rule evaluation reports false negatives.
- Job progress resumes after page refresh.
- Keyboard focus, `aria-live`, and reduced motion.
- Screenshots at 1366x768, 1024x768, and 390x844.
- No horizontal overflow.

### 14.7 Regression tests

- Market screening unchanged.
- MA screening mathematics unchanged.
- Ratings still save and persist for Brady and other users.
- Shared Needs Confirmation remains shared.
- Current performance chart remains operational.
- Main and review Lightweight Charts still pan, zoom, reset, and render volume.

## 15. Implementation Order

Do not attempt the entire system in one patch.

### Phase 1: Foundation

1. Add migration 010.
2. Add feature registry and deterministic technical snapshot module.
3. Insert queued snapshot stubs on new rating events.
4. Add idempotent snapshot backfill worker.
5. Add point-in-time chart `end_date` support.
6. Add snapshot/replay API.
7. Test with existing appraisal events.

Deliverable: historical appraisal replay plus technical feature snapshots.

### Phase 2: Outcomes and descriptive insights

1. Extend `rating_outcomes`.
2. Add benchmark-relative outcomes, excursions, and drawdowns.
3. Add coverage/maturity overview.
4. Add deterministic high/low cohorts.
5. Add univariate distributions, quantile tables, effect sizes, and FDR.
6. Build Insights UI states and feature explorer.

Deliverable: evidence-based descriptive Insights without machine-learning
probability claims.

### Phase 3: Fundamentals and context

1. Add SEC facts ingestion and caching for US stocks.
2. Calculate point-in-time valuation, growth, quality, and dilution features.
3. Add industry-relative percentiles and context.
4. Display coverage and missingness.
5. Rebuild snapshots as feature version 2; never overwrite version 1.

Deliverable: comprehensive US point-in-time feature set and honest ASX coverage.

### Phase 4: Pattern models and rules

1. Add chronological validation engine.
2. Add regularized and shallow-tree models subject to sample gates.
3. Add stable interactions, clusters, counterexamples, and model cards.
4. Add rule laboratory and shadow evaluation.
5. Add calibration only when data is sufficient.

Deliverable: candidate refined-screen rules with measured false-negative cost.

### Phase 5: Optional narrative

1. Add server-side LLM summary behind an optional configured secret.
2. Save prompt/model provenance.
3. Verify deterministic Insights work with the LLM disabled.

## 16. Build and Verification Commands

PowerShell script execution is restricted on this machine. Use executable
wrappers such as `npm.cmd`, not `npm`, where required.

Functions:

```powershell
cd "D:\jmbow1\My Documents\MoneyMaker\stock-filter\functions"
npm.cmd install
npm.cmd run build
```

Python syntax:

```powershell
cd "D:\jmbow1\My Documents\MoneyMaker\stock-filter"
python -m compileall cloud_backend firebase src
```

Tests may require installing the repository's Python dependencies because the
current system Python did not have `pytest` at handover time.

Apply migration only when a valid database URL is intentionally provided:

```powershell
$env:MONEYMAKER_DATABASE_URL = "<provided securely by owner>"
python firebase\apply_schema.py
```

Do not ask the user to paste database passwords or API keys into source files,
chat, or Git. Stop and request the owner to complete login/secret configuration
when credentials are required.

## 17. Deployment Gate

Development should be committed and pushed to GitHub in reviewable phases.

Do not deploy during implementation merely to test syntax.

Before production deployment:

1. Migration tested on empty and production-shaped fixtures.
2. Functions build passes.
3. Worker image/job tests pass.
4. Point-in-time leakage tests pass.
5. Firebase Hosting preview tested by owner.
6. Existing market, appraisal, and Analysis workflows pass regression tests.
7. User explicitly approves production deployment.

Use the `fast-firebase-deploy` workflow to deploy only touched Firebase targets.
Cloud Run worker changes are a separate deployment target and must not be
mistaken for a Hosting/Functions-only deploy.

## 18. Definition of Done

The implementation is not complete until all of the following are true:

- A winner appraisal can be replayed with the chart ending on the appraisal
  date.
- Every displayed feature has a definition, version, source timestamp, and
  missing-data explanation.
- No future market or filing data enters a snapshot.
- Mature outcomes include benchmark excess and drawdown, not only raw return.
- High and low cohorts compare equal horizons.
- Insights respect owner/team authorization.
- Small samples produce descriptive warnings, not fabricated model certainty.
- Findings include out-of-sample evidence and counterexamples.
- Candidate rules report both underperformers removed and high performers lost.
- Rules remain shadow-only until explicit admin approval.
- Existing screens, ratings, Needs Confirmation, charts, and Analysis
  performance remain operational.
- The owner has reviewed a Firebase preview before production deployment.

## 19. Prohibited Shortcuts

Do not:

- Use current Yahoo `info` values as historical fundamentals.
- Use future bars and hide them only in the UI.
- Collapse rating history to the latest label for training events.
- Treat missing values as zero.
- Randomly split time-series events.
- Fit encoders/scalers before train-test splitting.
- Promote in-sample correlations as validated rules.
- Run large analysis jobs inside a normal HTTP request.
- Put secrets in Hosting code or Git.
- Automatically alter the production winner screen.
- Overwrite immutable feature snapshots when formulas change.
- Revert the in-progress chart work or unrelated dirty files.
- Deploy without the owner approval gate.
