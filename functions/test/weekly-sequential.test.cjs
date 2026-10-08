const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {advanceWeeklyState, leaseAllowed, recordLaunchFailure} = require("../lib/weekly.js");

function fakeActions() {
  const calls = [];
  const statuses = new Map();
  let nextId = 1;
  return {
    calls,
    statuses,
    mark(id, status, error) {
      statuses.set(String(id), {status, error});
    },
    isDatabaseRunning: async () => ({running: true, state: "RUNNABLE", activationPolicy: "ALWAYS"}),
    startDatabase: async () => calls.push("startDatabase"),
    stopDatabase: async () => calls.push("stopDatabase"),
    startAsxRefresh: async () => {
      calls.push("startAsxRefresh");
      const id = `asx-refresh-${nextId++}`;
      return {job: {id}};
    },
    startUsRefresh: async () => {
      calls.push("startUsRefresh");
      const id = `us-refresh-${nextId++}`;
      return {job: {id}};
    },
    startAsxScan: async () => {
      calls.push("startAsxScan");
      const id = `asx-scan-${nextId++}`;
      return {job: {id}};
    },
    startUsScan: async () => {
      calls.push("startUsScan");
      const id = `us-scan-${nextId++}`;
      return {job: {id}};
    },
    startPublish: async () => {
      calls.push("startPublish");
      const id = `publish-${nextId++}`;
      return {job: {id}};
    },
    jobStatus: async (id) => {
      const entry = statuses.get(String(id));
      if (!entry) return null;
      return {id: String(id), status: entry.status, stage: "test", error: entry.error};
    }
  };
}

async function advance(state, actions, count = 1) {
  let current = state;
  for (let i = 0; i < count; i += 1) {
    const result = await advanceWeeklyState(current, actions);
    current = result.state;
  }
  return current;
}

async function testSequence() {
  const actions = fakeActions();
  let state = await advance({}, actions);
  assert.equal(state.phase, "refreshing_asx");

  actions.mark("asx-refresh-1", "succeeded");
  state = await advance(state, actions);
  assert.equal(state.phase, "screening_asx");

  actions.mark("asx-scan-2", "succeeded");
  state = await advance(state, actions);
  assert.equal(state.phase, "refreshing_us");

  actions.mark("us-refresh-3", "succeeded");
  state = await advance(state, actions);
  assert.equal(state.phase, "screening_us");

  actions.mark("us-scan-4", "succeeded");
  state = await advance(state, actions);
  assert.equal(state.phase, "publishing");

  actions.mark("publish-5", "succeeded");
  state = await advance(state, actions);
  assert.equal(state.phase, "complete");

  assert.deepEqual(actions.calls, [
    "startAsxRefresh",
    "startAsxScan",
    "startUsRefresh",
    "startUsScan",
    "startPublish",
    "stopDatabase"
  ]);
  console.log("sequential workflow: sequence and completion passed");
}

async function testResume() {
  const actions = fakeActions();
  actions.mark("old-scan", "succeeded");
  const state = await advance(
    {phase: "screening_asx", asx_job_id: "old-refresh", asx_scan_job_id: "old-scan", asx_refreshed_at: "2026-08-11"},
    actions
  );
  assert.equal(state.phase, "refreshing_us");
  assert.ok(!actions.calls.includes("startAsxRefresh"), "resume must not restart ASX refresh");
  assert.ok(!actions.calls.includes("startAsxScan"), "resume must not restart the ASX scan");
  assert.ok(actions.calls.includes("startUsRefresh"), "resume must start the US refresh");
  console.log("sequential workflow: resume passed");
}

async function testRetryAndBounds() {
  const actions = fakeActions();
  actions.mark("u1", "failed", "quota exceeded");
  let state = await advance({phase: "refreshing_us", us_job_id: "u1"}, actions);
  assert.equal(state.phase, "refreshing_us");
  assert.equal(Number(state.us_refresh_retry_count), 1);
  assert.equal(state.us_job_id, "us-refresh-1");

  actions.mark("us-refresh-1", "failed", "quota exceeded");
  state = await advance(state, actions);
  assert.equal(Number(state.us_refresh_retry_count), 2);
  assert.equal(state.us_job_id, "us-refresh-2");

  actions.mark("us-refresh-2", "failed", "quota exceeded");
  await assert.rejects(
    () => advanceWeeklyState(state, actions),
    /exceeded 2 retries/
  );
  console.log("sequential workflow: bounded retries passed");
}

async function testLegacyMigration() {
  const actions = fakeActions();
  actions.mark("u1", "succeeded");
  let state = await advance({phase: "refreshing_us", us_job_id: "u1"}, actions);
  assert.equal(state.phase, "screening_asx");
  assert.equal(state.legacy_order, true);
  assert.ok(actions.calls.includes("startAsxScan"), "legacy US refresh must still run the ASX scan");
  assert.ok(!actions.calls.includes("startUsRefresh"), "legacy US refresh must not restart");

  const parallel = fakeActions();
  parallel.mark("a1", "succeeded");
  parallel.mark("u1", "succeeded");
  state = await advance({phase: "screening", asx_scan_job_id: "a1", us_scan_job_id: "u1"}, parallel);
  assert.equal(state.phase, "publishing");
  assert.equal(state.legacy_order, true);

  const noIds = fakeActions();
  state = await advance({phase: "screening"}, noIds);
  assert.equal(state.phase, "screening_asx");
  assert.ok(noIds.calls.includes("startAsxScan"));
  console.log("sequential workflow: legacy migration passed");
}

async function testLeaseDeduplication() {
  const now = 1_752_000_000_000;
  const ttl = 600_000;
  assert.equal(leaseAllowed({}, now, ttl).ok, true, "unowned run should be claimable");
  assert.equal(
    leaseAllowed({phase: "refreshing_asx", lease_owner: "other", lease_until_ms: now + ttl}, now, ttl).ok,
    false,
    "active lease must block duplicate coordinators"
  );
  assert.equal(
    leaseAllowed({phase: "refreshing_asx", lease_owner: "other", lease_until_ms: now - 1}, now, ttl).ok,
    true,
    "expired lease must be claimable"
  );
  assert.equal(
    leaseAllowed({phase: "complete"}, now, ttl).ok,
    false,
    "finished runs must not be claimed"
  );
  console.log("sequential workflow: duplicate lease behavior passed");
}

async function testDeployEnvPersistence() {
  const script = fs.readFileSync(
    path.join(__dirname, "../../scripts/deploy_firebase_native.ps1"),
    "utf8"
  );
  const envExample = fs.readFileSync(path.join(__dirname, "../.env.example"), "utf8");
  assert.match(script, /MONEYMAKER_USE_TASK_QUEUE/);
  assert.match(script, /MONEYMAKER_USE_TASK_QUEUE must be 1, true, or yes/);
  assert.match(script, /"scheduledratingoutcomes"/);
  assert.match(envExample, /MONEYMAKER_USE_TASK_QUEUE=true/);
  console.log("sequential workflow: deploy env persistence passed");
}

async function testMissingPersistedJobRetries() {
  const actions = fakeActions();
  let state = await advance({phase: "refreshing_us", us_job_id: "missing"}, actions);
  assert.equal(state.us_refresh_retry_count, 1);
  assert.equal(state.us_job_id, "us-refresh-1");
  assert.ok(actions.calls.includes("startUsRefresh"));
  console.log("sequential workflow: missing persisted job retry passed");
}

async function testBoundedLaunchFailures() {
  let state = {phase: "screening_asx"};
  let failure = recordLaunchFailure(state, "temporary quota error");
  assert.equal(failure.retry, true);
  state = failure.state;
  failure = recordLaunchFailure(state, "temporary quota error");
  assert.equal(failure.retry, true);
  state = failure.state;
  failure = recordLaunchFailure(state, "temporary quota error");
  assert.equal(failure.retry, false);
  assert.equal(failure.state.launch_retry_counts.screening_asx, 3);
  console.log("sequential workflow: bounded launch failures passed");
}

async function main() {
  await testSequence();
  await testResume();
  await testRetryAndBounds();
  await testLegacyMigration();
  await testLeaseDeduplication();
  await testDeployEnvPersistence();
  await testMissingPersistedJobRetries();
  await testBoundedLaunchFailures();
  console.log("weekly sequential tests passed");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
