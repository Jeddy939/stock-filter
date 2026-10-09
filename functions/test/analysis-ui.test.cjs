const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const html = fs.readFileSync(path.join(__dirname, "..", "..", "hosting", "index.html"), "utf8");

const authInitialization = html.slice(
  html.indexOf("async function initializeFirebaseAuth()"),
  html.indexOf("async function snapshotJson(")
);
assert.doesNotMatch(authInitialization, /loadCurrentSnapshot\(/, "Firebase auth initialization must not wait on an API call that waits for authReady");
assert.ok(
  html.indexOf('firebaseFirestoreMethods.getDoc(') < html.indexOf('payload = await api("/api/user/profile")'),
  "Signed-in startup should read the Firestore profile before using the SQL-backed API fallback"
);
assert.match(
  html,
  /await loadCurrentUserProfile\(\);\s*startWeeklyRunPolling\(\);\s*await loadCurrentSnapshot\(\)\.catch/,
  "Authenticated startup should resolve auth before loading the protected snapshot"
);
assert.match(html, /id="weeklyRunStatus"[^>]*aria-live="polite"/);
assert.match(html, /collection\(firebaseFirestore, "weekly_runs"\)/);
assert.match(html, /const WEEKLY_RUN_POLL_MS = 20_000/);
assert.match(html, /progressFill\.classList\.toggle\("indeterminate", isActive\)/);
for (const phase of ["starting_database", "refreshing_asx", "refreshing_us", "screening", "publishing", "complete", "failed"]) {
  assert.match(html, new RegExp(`${phase}:`));
}

assert.match(html, /<h3 id="analysisReviewHeading">Needs confirmation<\/h3>/);
assert.match(html, /id="analysisReviewChart"/);
assert.match(html, /label=needs_confirmation&scope=team&horizon=0&limit=5000/);
assert.match(html, /<th>Owner<\/th>/);
assert.match(html, /data-review-owner=/);
assert.match(html, /target_uid:\s*tableRow\.dataset\.reviewOwner/);
assert.ok(html.indexOf('id="analysisReviewHeading"') < html.indexOf('id="analysisChartHeading"'));
assert.match(html, /id="analysisManualTicker"/);
assert.match(html, /id="analysisAddNeeds"/);
assert.match(html, /data-confirm-label="winner"/);
assert.match(html, /data-confirm-label="bad"/);
assert.match(html, /id="analysisWinnerBody"/);
assert.match(html, /id="analysisMaybeBody"/);
assert.match(html, /id="analysisBadBody"/);
assert.match(html, /id="boughtPortfolioHeading">Bought portfolio/);
assert.match(html, /id="boughtPortfolioBody"/);
assert.match(html, /id="purchaseModal"/);
assert.match(html, /data-purchase-ticker=/);
assert.match(html, /collection\(firebaseFirestore, "purchases"\)/);
assert.match(html, /function saveLivePurchase/);
assert.match(html, /function deleteLivePurchase/);
assert.match(html, /Team purchases.*equal-weight performance/);
assert.doesNotMatch(html, /Winner and potential-winner trends/);
assert.doesNotMatch(html, /Potential Winner/);
assert.match(html, /id="analysisPerformanceMode"/);
assert.match(html, /id="analysisInsightsMode"/);
assert.match(html, /id="analysisInsightsWorkspace"/);
assert.match(html, /id="insightsReplayPick"/);
assert.match(html, /id="generateInsights"/);
assert.match(html, /id="insightsReplayChart"/);
assert.match(html, /id="insightsRevealOutcome"/);
assert.match(html, /id="insightsRuleForm"/);
assert.match(html, /id="insightsRuleBody"/);
assert.match(html, /\/api\/analysis\/insights\/overview/);
assert.match(html, /\/api\/analysis\/insights\/runs\//);
assert.match(html, /Exploratory only: treat as a lead, not a pattern\./);
assert.match(html, /Candidate: survived multiple-testing correction/);
assert.match(html, /Shadow test on appraisals made after this rule was saved/);
assert.match(html, /function analysisEntryPrice\(row\)/);
assert.match(html, /incorrectly removed.*high performers/);
assert.match(html, /\.analysis-picks thead\s*\{\s*position:\s*sticky/);
assert.match(html, /scrollbar-gutter:\s*stable/);

for (const key of [
  "market",
  "ticker",
  "event_at_utc",
  "signal_price",
  "latest_price",
  "return_percent",
  "latest_date"
]) {
  assert.match(html, new RegExp(`data-sort-key="${key}"`));
}

for (const group of ["winner", "maybe", "bad"]) {
  assert.match(html, new RegExp(`data-analysis-group="${group}"`));
}

const migration = fs.readFileSync(
  path.join(__dirname, "..", "..", "firebase", "migrations", "009_simplify_appraisal_categories.sql"),
  "utf8"
);
assert.match(migration, /SET label = 'maybe' WHERE label = 'potential_winner'/);
assert.match(migration, /SET label = 'winner' WHERE label = 'confirmed'/);

const firestoreRules = fs.readFileSync(path.join(__dirname, "..", "..", "firestore.rules"), "utf8");
assert.match(firestoreRules, /match \/purchases\/\{purchaseId\}/);
assert.match(firestoreRules, /request\.resource\.data\.owner_uid == request\.auth\.uid/);
assert.match(firestoreRules, /resource\.data\.owner_uid == request\.auth\.uid/);
assert.match(firestoreRules, /request\.resource\.data\.bought_price > 0/);
assert.match(firestoreRules, /request\.resource\.data\.bought_at <= request\.time/);

console.log("analysis UI structure tests passed");
