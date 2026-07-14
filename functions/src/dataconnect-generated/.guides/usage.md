# Basic Usage

Always prioritize using a supported framework over using the generated SDK
directly. Supported frameworks simplify the developer experience and help ensure
best practices are followed.





## Advanced Usage
If a user is not using a supported framework, they can use the generated SDK directly.

Here's an example of how to use it with the first 5 operations:

```js
import { upsertMyAppraisal, clearMyAppraisal, upsertMyNote, listRecentScans, getMarketStatus, getLatestRefreshJobs, getWeeklyMetricSnapshot, getScanResults, getChartBars, getCompanyProfile } from '@firebasegen/admin-connector';


// Operation UpsertMyAppraisal:  For variables, look at type UpsertMyAppraisalVars in ../index.d.ts
const { data } = await UpsertMyAppraisal(dataConnect, upsertMyAppraisalVars);

// Operation ClearMyAppraisal:  For variables, look at type ClearMyAppraisalVars in ../index.d.ts
const { data } = await ClearMyAppraisal(dataConnect, clearMyAppraisalVars);

// Operation UpsertMyNote:  For variables, look at type UpsertMyNoteVars in ../index.d.ts
const { data } = await UpsertMyNote(dataConnect, upsertMyNoteVars);

// Operation ListRecentScans:  For variables, look at type ListRecentScansVars in ../index.d.ts
const { data } = await ListRecentScans(dataConnect, listRecentScansVars);

// Operation GetMarketStatus:  For variables, look at type GetMarketStatusVars in ../index.d.ts
const { data } = await GetMarketStatus(dataConnect, getMarketStatusVars);

// Operation GetLatestRefreshJobs:  For variables, look at type GetLatestRefreshJobsVars in ../index.d.ts
const { data } = await GetLatestRefreshJobs(dataConnect, getLatestRefreshJobsVars);

// Operation GetWeeklyMetricSnapshot:  For variables, look at type GetWeeklyMetricSnapshotVars in ../index.d.ts
const { data } = await GetWeeklyMetricSnapshot(dataConnect, getWeeklyMetricSnapshotVars);

// Operation GetScanResults:  For variables, look at type GetScanResultsVars in ../index.d.ts
const { data } = await GetScanResults(dataConnect, getScanResultsVars);

// Operation GetChartBars:  For variables, look at type GetChartBarsVars in ../index.d.ts
const { data } = await GetChartBars(dataConnect, getChartBarsVars);

// Operation GetCompanyProfile:  For variables, look at type GetCompanyProfileVars in ../index.d.ts
const { data } = await GetCompanyProfile(dataConnect, getCompanyProfileVars);


```