const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..", "..");
const api = fs.readFileSync(path.join(root, "functions", "src", "api.ts"), "utf8");
const worker = fs.readFileSync(path.join(root, "firebase", "worker.py"), "utf8");
const snapshots = fs.readFileSync(path.join(root, "cloud_backend", "insights", "snapshots.py"), "utf8");
const outcomes = fs.readFileSync(path.join(root, "cloud_backend", "insights", "outcomes.py"), "utf8");
const html = fs.readFileSync(path.join(root, "hosting", "index.html"), "utf8");
const screenMigration = fs.readFileSync(
  path.join(root, "firebase", "migrations", "014_screen_observations.sql"),
  "utf8"
);
const migration = fs.readFileSync(
  path.join(root, "firebase", "migrations", "010_point_in_time_insights.sql"),
  "utf8"
);
const anchorMigration = fs.readFileSync(
  path.join(root, "firebase", "migrations", "013_appraisal_anchor.sql"),
  "utf8"
);
const progressMigration = fs.readFileSync(
  path.join(root, "firebase", "migrations", "011_nullable_job_progress.sql"),
  "utf8"
);

assert.match(migration, /UNIQUE \(feature_name, feature_version\)/);
assert.match(migration, /UNIQUE \(rating_event_id, feature_version\)/);
assert.match(migration, /is_missing AND num_nonnulls\(numeric_value, boolean_value, categorical_value\) = 0/);
assert.match(migration, /filed_at_utc TIMESTAMPTZ NOT NULL/);
assert.match(migration, /source_fact_key TEXT NOT NULL/);
assert.match(progressMigration, /ALTER COLUMN percent DROP NOT NULL/);

assert.match(api, /apiApp\.post\("\/api\/analysis\/pick"/);
assert.match(api, /apiApp\.post\("\/api\/label"/);
assert.equal((api.match(/insertSnapshotStub\(client, ratingEventId\)/g) || []).length, 2);
assert.match(api, /apiApp\.get\("\/api\/analysis\/insights\/picks\/:ratingEventId"/);
assert.match(api, /String\(event\.firebase_uid \?\? ""\) !== user\.uid/);
assert.match(api, /AND \(\$4::date IS NULL OR price_date <= \$4::date\)/);
assert.match(api, /effective_end_date/);
assert.match(api, /apiApp\.post\("\/api\/admin\/insights\/backfill-snapshots"/);
assert.match(api, /apiApp\.get\("\/api\/analysis\/insights\/overview"/);
assert.match(api, /apiApp\.post\("\/api\/analysis\/insights\/run"/);
assert.match(api, /apiApp\.get\("\/api\/analysis\/insights\/runs\/:runId"/);
assert.match(api, /apiApp\.post\("\/api\/analysis\/insights\/rules"/);
assert.match(api, /apiApp\.post\("\/api\/analysis\/insights\/rules\/:ruleId\/evaluate"/);
assert.match(api, /Only an admin may approve a rule/);
assert.match(api, /high_removed_count: removed\.filter\(isHigh\)\.length/);
assert.match(api, /row\.eventAt >= ruleCreatedAt/);
assert.match(api, /requireAdmin\(user\)/);

assert.match(worker, /def run_insight_snapshot_backfill/);
assert.match(worker, /elif kind == "insight-snapshot-backfill"/);
assert.match(worker, /process_snapshot\(conn, snapshot_id, definition_ids\)/);
assert.match(outcomes, /%\(remeasure\)s/);
assert.match(api, /remeasure: true/);
assert.match(outcomes, /ph\.price_date <= a\.anchor_date \+ %\(horizon\)s::int \+ \{OUTCOME_HORIZON_TOLERANCE_DAYS\}/);
assert.match(outcomes, /appraisal_cutoff_date\(market, event_at_utc\) AS cutoff_day/);
assert.match(outcomes, /'horizon_status', horizon_status/);
assert.match(outcomes, /same_day_target_stop_policy', 'stop_first'/);
assert.match(worker, /from cloud_backend\.insights\.outcomes import OUTCOME_VERSION, measure_rating_outcomes/);
assert.match(worker, /refresh_screen_observations\(/);
assert.match(screenMigration, /CREATE TABLE IF NOT EXISTS scan_near_misses/);
assert.match(screenMigration, /CREATE TABLE IF NOT EXISTS screen_observation_outcomes/);
assert.match(api, /apiApp\.get\("\/api\/analysis\/selection-comparison"/);
assert.match(html, /id="selectionComparisonBody"/);
assert.match(html, /\/api\/analysis\/selection-comparison\?market=/);
assert.match(snapshots, /price_date <= appraisal_cutoff_date\(%s, %s::timestamptz\)/);
assert.match(anchorMigration, /CREATE OR REPLACE FUNCTION appraisal_cutoff_date/);
assert.doesNotMatch(api, /feature_version: 2\b|feature_version = 2\b/);
assert.match(worker, /benchmark_excess_return_percent/);
assert.match(worker, /def run_insight_analysis/);
assert.match(snapshots, /WHEN provider = 'yfinance' THEN 1/);
assert.match(snapshots, /"appraisal_source_provider": event_provider or None/);

console.log("point-in-time insights foundation tests passed");
