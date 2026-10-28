# MoneyMaker Trigger Levels and Loading Speed Handover

Handover for continuing development of the work done on branch
`agent/trigger-levels`, October 2026. It covers four changes:

1. **Trigger levels**: a Triggers tab that tracks Brady's long-term moving
   average buy floors.
2. **Live database controls**: Ops buttons, and an idle shutdown that counts app use.
3. **Loading speed**: the app is fast while Cloud SQL is stopped.
4. **Trigger chart**: an in-app chart for each trigger.

Read section 2 first. Most design decisions here follow from those constraints.

## 1. Repository and State

```text
Repository: D:\jmbow1\My Documents\MoneyMaker\stock-filter
Branch:     agent/trigger-levels (pushed to origin)
Based on:   fc0ec15 (the commit that was live before this work)
Main:       main (this branch is NOT merged; no pull request exists yet)
```

Commits on the branch, oldest first:

| Commit | Content | Deployed |
|---|---|---|
| `f128a19` | Trigger levels tab, scheduled scan, Ops > Live database controls | Yes, 2026-10-23 |
| `67686a4` | Loading speed fixes; batched trigger writes; stepped scans | Yes, 2026-10-28 |
| next commit | In-app trigger chart, plus this document | See section 9 |

Files that are always left out of commits:

- `us_tickers_nasdaqtrader.txt` (a large, locally modified ticker list)
- `export_scan_labels.zip`

The owner works with Brady, who supplied the trigger list. Commit, push and
deploy only when the owner asks.

## 2. Constraints That Shape the Design

- **Cloud SQL is stopped most of the week.** Production Postgres
  (`moneymaker-db`) runs only during the Tuesday weekly pipeline and when an
  admin starts it. Anything that must work every day cannot depend on it. The
  weekly pipeline publishes a gzip JSON snapshot to Cloud Storage for read-only
  browsing.
- **Firestore is in `nam5` (US), while Functions run in `australia-southeast1`.**
  Each Firestore round trip from a function costs roughly 150–350 ms. Reading
  or writing documents one at a time in a loop is very slow (200 sequential
  reads took over a minute). Always batch: use `getAll(...refs)` and
  `bulkWriter()`. The browser reads Firestore directly and is not affected in
  the same way.
- **Firebase Hosting cuts off a function rewrite after 60 seconds** and returns
  a Google 502 HTML page, even though the function keeps running. The page
  shows this as `Server returned 502 text/html`. Every request made from the
  page must finish well within 60 s. Longer work is split into steps that the
  page drives (see `/api/triggers/scan-batch`).
- **Cold starts.** The `api` function (1 vCPU, concurrency 80, min instances 0)
  takes 2–5 s to start after being idle. Avoid requiring it on page load where
  Firestore or a static file will do.
- **Firebase Storage is not linked to the snapshot bucket**
  `moneymaker-aedf7-cache`. No Storage rules release exists, and `storage.rules`
  is not deployed by `firebase.json`. Browsers therefore cannot read the bucket
  directly; snapshot files go through the API.

## 3. Trigger Levels

### Rules (agreed with the owner)

- Each stock has one simple moving average of closes: a length plus a timeframe
  (`daily`, `weekly` or `monthly`), e.g. `MCD 700 Weekly`.
- **Trigger:** any bar whose **low** is at or below that bar's MA. Open and
  close are not good enough.
- A triggered stock stays flagged for **6 months after the last touch** (measured
  from the end of the touching bar, or from today for the current bar), then goes
  back into the pool and can trigger again.
- **Short history:** with fewer bars than the MA length, the MA averages every
  bar available, but only from 50 bars onwards (or from the MA length, if that
  is shorter than 50). Below that, the status is "Not enough data".
- The MA is always recalculated from fresh prices, never stored as a fixed value.
- Flagging is in the app only; there are no emails or push alerts.
- On the first scan, a touch in the last 6 months counts, so stocks that
  already bounced show as triggered.

### Code

| File | Role |
|---|---|
| `functions/src/trigger-levels.ts` | Pure logic: parsing, validation, Yahoo symbols, bar building, `evaluateTrigger`, `triggerMaSeries`, `touchStarts`, `fetchYahooBars` |
| `functions/src/trigger-seed.ts` | Brady's starting list, generated from `Trigger levels (2).xlsx` |
| `functions/src/triggers.ts` | Firestore storage, batched scanning, Express router, scheduled function |
| `functions/test/trigger-levels.test.cjs` | Tests for all of the above |
| `hosting/index.html` | Triggers tab (search for `Trigger levels:` and `triggerChartState`) |

### Prices

Prices come directly from Yahoo's v8 chart API (`fetchYahooBars`), not from
Cloud SQL. There are three quirks the code relies on:

- `range=max` silently returns coarser bars (e.g. 3-month bars for MCD).
  Explicit `period1`/`period2` are used instead.
- Weekly bars align to the weekday of `period1`. `period1 = -259200`
  (Monday 1969-12-29) makes them start on Monday.
- Yahoo appends the live quote as an extra element inside the current period.
  `barsFromYahoo` merges bars that share a period, so the current bar keeps
  its full low.

Market symbols: ASX gets a `.AX` suffix, crypto gets `-USD`, and in US
tickers `.` becomes `-`. Daily requests only fetch `dailyHistoryDays(maPeriod)`
calendar days. Charts fetch 5 extra years.

### Data (Firestore)

- `trigger_levels/{market}__{TICKER}`: `ticker`, `market` (`us`/`asx`/`crypto`),
  `ma_period`, `timeframe`, `yahoo_symbol`, `created_*`, `updated_*`, and `scan`.
  `scan` holds the latest result: status, price, MA value, MA length used,
  distance, touch dates, active-until date and error. A scan counts only while
  `scan.config_key === "<ma_period>:<timeframe>"`, so editing a trigger makes
  its old scan stale.
- `system/trigger_scan`: last full-scan status and counts.
- Rules: members can read `trigger_levels`, and all writes go through the API
  (`firestore.rules`).
- Live state: 200 entries loaded on 2026-10-27. Duplicates in the spreadsheet
  merge by document ID.

### API (all in `triggers.ts`; analyst role or higher for writes)

| Route | Purpose |
|---|---|
| `GET /api/triggers` | List and status (fallback; the page reads Firestore directly) |
| `POST /api/triggers` | Add or edit one trigger and scan it immediately |
| `POST /api/triggers/bulk` | Paste lines (`MCD 700 Weekly`, spreadsheet rows read as US/ASX/Crypto columns); returns `scan_ids` without scanning |
| `POST /api/triggers/scan-batch` | One scan step: at most 60 IDs, stops starting new stocks after 30 s, returns `items` plus `remaining`. With `full_scan.final`, it records `system/trigger_scan` |
| `POST /api/triggers/scan` | Old one-shot full scan; kept only for cached old pages |
| `POST /api/triggers/seed` | Admin only: adds Brady's list without overwriting existing entries; returns `scan_ids` |
| `DELETE /api/triggers/:id` | Remove an entry |
| `GET /api/triggers/chart` | Query: `market`, `ticker`, `ma_period`, `timeframe`. Returns candles, the trigger MA series, touch starts, evaluation and name. Cached 10 minutes per instance |

Scheduled scan: `scheduledTriggerScan` runs at 07:30 and 16:30
Australia/Brisbane, with a 540 s timeout and a full scan under a 15-minute
lock. It needs no database secret or Cloud SQL connection.

### Page behaviour

- The list is read straight from Firestore (falling back to the API). It is
  sorted triggered first, then by distance to the MA.
- Scan now, Load list and Paste a list run through `scanTriggerIds()`, which
  loops over `scan-batch` in chunks of 30 and updates the table and a progress
  count after each one.
- Clicking a ticker opens the chart dialog (`openTriggerChart`). It shows the
  trigger MA in bold green, arrows at the first bar of each dip, optional
  30/90/180/360/700 MAs (computed client-side) and 1Y/3Y/5Y/10Y/All ranges.
  It reuses `drawCandles`, which gained `lineStyles` and `markers` options.

## 4. Live Database Controls (`f128a19`)

- Ops > Live database has start, stop and status buttons, using the existing
  `/api/admin/{start,stop}-database` and `/api/admin/database-status` routes.
- Signed-in API calls record `system/database_activity.last_app_activity_at`
  in Firestore, at most once a minute per instance
  (`recordAppActivity` in `database-lifecycle.ts`). The hourly
  `scheduledDatabaseIdleShutdown` stops Cloud SQL only after 30 minutes with
  neither job activity nor app activity. `/admin/database-status` and
  `/triggers*` are not counted.

## 5. Loading Speed (`67686a4`)

The causes, measured from production request logs before the fix:

- Snapshot chart files took a median of 2.65 s each and were sent
  uncompressed (337 KB). `/api/snapshot-object` downloaded each gzip file,
  decompressed and parsed it, then re-sent it as JSON with only a 5-minute
  cache.
- `current.json` (2 KB) took a median of 1.6 s, and `/api/auth-config` 1.75 s.
  Both are mostly cold starts.
- Every API call forced an ID-token refresh (`getIdToken(user, true)`).
- The Analysis tab re-read the whole `team_appraisals` collection for every
  pick's chart.
- A bug: the Bought portfolio asked for snapshot kind `weekly_metrics`, which
  does not exist, so prices were always missing. Latest daily prices live in
  `screen_flags`.

The fixes:

- `GET /api/snapshot-file` (`api.ts` plus `functions/src/snapshot-files.ts`)
  sends the stored gzip bytes unchanged (`Content-Encoding: gzip`). Files under
  `snapshots/versions/` get `Cache-Control: private, max-age=31536000, immutable`
  and an in-instance LRU of up to 96 MB, so the browser keeps them all week.
  `/api/snapshot-object` remains as a fallback.
- The manifest is read from Firestore `system/current_snapshot`, which the
  publisher writes alongside `snapshots/current.json`.
- The sign-in config comes from the static `hosting/auth-config.json`. Hosting's
  `/__/firebase/init.json` uses a different API key, so it is not a drop-in
  replacement. If the Firebase web config changes, update this file along with
  `scripts/deploy_firebase_native.ps1`.
- `getIdToken(user)` reuses the cached token.
- `prefetchOtherViews()` warms the Triggers list and Analysis charts after the
  Market tab loads.

Measured locally against the real bucket: 3.6–4.4× fewer bytes. On a repeat
visit the Market tab rendered in 0.24 s and 10 charts in 47 ms from the browser
cache.

## 6. Deploying

Normal path, from `stock-filter`:

```powershell
# Only when firestore.rules changed:
firebase deploy --only firestore:rules --project moneymaker-aedf7 --non-interactive
# Functions, then Hosting (order matters when the page needs new routes):
& .\scripts\deploy_firebase_native.ps1            # both
& .\scripts\deploy_firebase_native.ps1 -Hosting   # Hosting only
```

Points to watch:

- **Do not redirect the script's output** (`*>`, `2>&1`). In PowerShell 5.1,
  under `$ErrorActionPreference = "Stop"`, gcloud's progress output on stderr
  becomes a terminating error. The script then stops after the Functions deploy
  and never deploys Hosting.
- After any Functions deploy, check that each service kept `FUNCTION_TARGET`,
  the database secret and its environment variables (an earlier incident wiped
  them). For example:
  `gcloud run services describe api --region australia-southeast1 --project moneymaker-aedf7`.
  `scheduledtriggerscan` is intentionally not in the script's update list and
  needs none of those variables.
- The script deploys all functions, so `scheduledTriggerScan` is created and
  updated with everything else.

## 7. Testing

- Unit tests: `cd functions && npm test` (build plus 15 suites, including
  `trigger-levels` and `snapshot-files`).
- Emulator run, with no sign-in and no production writes:
  1. Create `functions/.env.local` (git-ignored) containing
     `MONEYMAKER_REQUIRE_AUTH=false` and
     `MONEYMAKER_DATABASE_URL=postgres://127.0.0.1:1/none`.
  2. Run `firebase emulators:start --project demo-moneymaker --only functions,firestore,hosting`.
  3. Open `http://127.0.0.1:5000`. The page shows the sign-in screen; in the
     console, set `currentUserProfile = {user: {role: "admin"}}`, then call
     `showAuthenticated()` and `activateView("triggers")`.
  4. Yahoo is real and Firestore is emulated.
  5. Delete `.env.local` afterwards.
- The `demo-` project cannot read the real snapshot bucket. To test snapshot
  routes, load `functions/lib/api.js` in a small Node script, setting
  `MONEYMAKER_REQUIRE_AUTH=false`,
  `MONEYMAKER_STORAGE_BUCKET=moneymaker-aedf7-cache` and
  `GOOGLE_CLOUD_PROJECT=moneymaker-aedf7`, and serve `hosting/` next to it.
  This reads production Storage, read-only.
- To stop emulators, stop only the processes listening on the emulator ports.
  Do not kill every `java.exe`.
- Production API request logs (latency and response size per route):
  `gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="api"' --project moneymaker-aedf7`.

## 8. Open Items and Suggested Next Steps

1. **Check gzip pass-through in production.** After a signed-in session,
   check the request logs: the `responseSize` for `/api/snapshot-file` should
   be close to the stored compressed size (e.g. about 100 KB for an ASX chart,
   not about 375 KB). This has not yet been confirmed through Hosting.
2. **Merge the branch.** `agent/trigger-levels` is live but not merged to
   `main`. Open a pull request when the owner is ready.
3. **Remaining failures while the database is off.** Insights calls and the
   admin feedback inbox (`/api/admin/feedback`, loaded at startup for admins)
   need Cloud SQL and return 500 while it is stopped. Options: skip or explain
   them while the database is off (the page can tell from
   `/api/admin/database-status` or the first failure), or move feedback to
   Firestore.
4. **Activity tracking scope.** `APP_ACTIVITY_IGNORED_PATHS` in `api.ts` does
   not exclude `/snapshot-file` or `/snapshot-object`, so browsing snapshots
   keeps a running database awake. Consider excluding them.
5. **Cold starts.** The first visit after each weekly publication still waits
   for a cold `api` instance. Options: set `minInstances: 1` on `api` (a
   monthly cost; confirm with the owner first), or link Firebase Storage to
   the bucket and deploy Storage rules, so the browser reads snapshot files
   directly.
6. **Yahoo resilience.** Prices depend on Yahoo's unofficial endpoint. On a
   fetch failure, a scan keeps the last good result and shows "Last update
   failed". If rate limiting appears, reduce `SCAN_CONCURRENCY` in
   `triggers.ts` or add a fallback provider.
7. **Possible trigger features.** None of these has been asked for yet:
   - a history of past trigger episodes;
   - an alert when a stock newly triggers (the owner chose in-app only for now);
   - CSV export of the list;
   - links from a trigger to the Market tab chart.
8. **Seed file.** `trigger-seed.ts` is a one-off import. Live edits happen
   through the app and are stored in Firestore, not in this file.

## 9. Final Commit Contents

The commit that adds this document also adds:

- `GET /api/triggers/chart`;
- `triggerMaSeries`, `touchStarts` and `dailyHistoryDays`;
- volume on bars;
- the chart dialog in `hosting/index.html`;
- tests for the MA series and touch runs.

It requires a Functions and Hosting deploy. No Firestore rules change.
