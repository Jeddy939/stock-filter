# Generated TypeScript README
This README will guide you through the process of using the generated JavaScript SDK package for the connector `admin`. It will also provide examples on how to use your generated SDK to call your Data Connect queries and mutations.

***NOTE:** This README is generated alongside the generated SDK. If you make changes to this file, they will be overwritten when the SDK is regenerated.*

# Table of Contents
- [**Overview**](#generated-javascript-readme)
- [**Accessing the connector**](#accessing-the-connector)
  - [*Connecting to the local Emulator*](#connecting-to-the-local-emulator)
- [**Queries**](#queries)
  - [*ListRecentScans*](#listrecentscans)
  - [*GetMarketStatus*](#getmarketstatus)
  - [*GetLatestRefreshJobs*](#getlatestrefreshjobs)
  - [*GetWeeklyMetricSnapshot*](#getweeklymetricsnapshot)
  - [*GetScanResults*](#getscanresults)
  - [*GetChartBars*](#getchartbars)
  - [*GetCompanyProfile*](#getcompanyprofile)
  - [*GetMyAppraisals*](#getmyappraisals)
  - [*GetMySavedPicks*](#getmysavedpicks)
  - [*GetMyRatingHistory*](#getmyratinghistory)
  - [*GetMyNotes*](#getmynotes)
- [**Mutations**](#mutations)
  - [*UpsertMyAppraisal*](#upsertmyappraisal)
  - [*ClearMyAppraisal*](#clearmyappraisal)
  - [*UpsertMyNote*](#upsertmynote)

# Accessing the connector
A connector is a collection of Queries and Mutations. One SDK is generated for each connector - this SDK is generated for the connector `admin`. You can find more information about connectors in the [Data Connect documentation](https://firebase.google.com/docs/data-connect#how-does).

You can use this generated SDK by importing from the package `@firebasegen/admin-connector` as shown below. Both CommonJS and ESM imports are supported.

You can also follow the instructions from the [Data Connect documentation](https://firebase.google.com/docs/data-connect/web-sdk#set-client).

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig } from '@firebasegen/admin-connector';

const dataConnect = getDataConnect(connectorConfig);
```

## Connecting to the local Emulator
By default, the connector will connect to the production service.

To connect to the emulator, you can use the following code.
You can also follow the emulator instructions from the [Data Connect documentation](https://firebase.google.com/docs/data-connect/web-sdk#instrument-clients).

```typescript
import { connectDataConnectEmulator, getDataConnect } from 'firebase/data-connect';
import { connectorConfig } from '@firebasegen/admin-connector';

const dataConnect = getDataConnect(connectorConfig);
connectDataConnectEmulator(dataConnect, 'localhost', 9399);
```

After it's initialized, you can call your Data Connect [queries](#queries) and [mutations](#mutations) from your generated SDK.

# Queries

There are two ways to execute a Data Connect Query using the generated Web SDK:
- Using a Query Reference function, which returns a `QueryRef`
  - The `QueryRef` can be used as an argument to `executeQuery()`, which will execute the Query and return a `QueryPromise`
- Using an action shortcut function, which returns a `QueryPromise`
  - Calling the action shortcut function will execute the Query and return a `QueryPromise`

The following is true for both the action shortcut function and the `QueryRef` function:
- The `QueryPromise` returned will resolve to the result of the Query once it has finished executing
- If the Query accepts arguments, both the action shortcut function and the `QueryRef` function accept a single argument: an object that contains all the required variables (and the optional variables) for the Query
- Both functions can be called with or without passing in a `DataConnect` instance as an argument. If no `DataConnect` argument is passed in, then the generated SDK will call `getDataConnect(connectorConfig)` behind the scenes for you.

Below are examples of how to use the `admin` connector's generated functions to execute each query. You can also follow the examples from the [Data Connect documentation](https://firebase.google.com/docs/data-connect/web-sdk#using-queries).

## ListRecentScans
You can execute the `ListRecentScans` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
listRecentScans(vars: ListRecentScansVariables, options?: ExecuteQueryOptions): QueryPromise<ListRecentScansData, ListRecentScansVariables>;

interface ListRecentScansRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: ListRecentScansVariables): QueryRef<ListRecentScansData, ListRecentScansVariables>;
}
export const listRecentScansRef: ListRecentScansRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
listRecentScans(dc: DataConnect, vars: ListRecentScansVariables, options?: ExecuteQueryOptions): QueryPromise<ListRecentScansData, ListRecentScansVariables>;

interface ListRecentScansRef {
  ...
  (dc: DataConnect, vars: ListRecentScansVariables): QueryRef<ListRecentScansData, ListRecentScansVariables>;
}
export const listRecentScansRef: ListRecentScansRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the listRecentScansRef:
```typescript
const name = listRecentScansRef.operationName;
console.log(name);
```

### Variables
The `ListRecentScans` query requires an argument of type `ListRecentScansVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface ListRecentScansVariables {
  market: string;
  limit: number;
}
```
### Return Type
Recall that executing the `ListRecentScans` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `ListRecentScansData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface ListRecentScansData {
  scans?: unknown[] | null;
}
```
### Using `ListRecentScans`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, listRecentScans, ListRecentScansVariables } from '@firebasegen/admin-connector';

// The `ListRecentScans` query requires an argument of type `ListRecentScansVariables`:
const listRecentScansVars: ListRecentScansVariables = {
  market: ..., 
  limit: ..., 
};

// Call the `listRecentScans()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await listRecentScans(listRecentScansVars);
// Variables can be defined inline as well.
const { data } = await listRecentScans({ market: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await listRecentScans(dataConnect, listRecentScansVars);

console.log(data.scans);

// Or, you can use the `Promise` API.
listRecentScans(listRecentScansVars).then((response) => {
  const data = response.data;
  console.log(data.scans);
});
```

### Using `ListRecentScans`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, listRecentScansRef, ListRecentScansVariables } from '@firebasegen/admin-connector';

// The `ListRecentScans` query requires an argument of type `ListRecentScansVariables`:
const listRecentScansVars: ListRecentScansVariables = {
  market: ..., 
  limit: ..., 
};

// Call the `listRecentScansRef()` function to get a reference to the query.
const ref = listRecentScansRef(listRecentScansVars);
// Variables can be defined inline as well.
const ref = listRecentScansRef({ market: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = listRecentScansRef(dataConnect, listRecentScansVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.scans);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.scans);
});
```

## GetMarketStatus
You can execute the `GetMarketStatus` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getMarketStatus(vars: GetMarketStatusVariables, options?: ExecuteQueryOptions): QueryPromise<GetMarketStatusData, GetMarketStatusVariables>;

interface GetMarketStatusRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMarketStatusVariables): QueryRef<GetMarketStatusData, GetMarketStatusVariables>;
}
export const getMarketStatusRef: GetMarketStatusRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getMarketStatus(dc: DataConnect, vars: GetMarketStatusVariables, options?: ExecuteQueryOptions): QueryPromise<GetMarketStatusData, GetMarketStatusVariables>;

interface GetMarketStatusRef {
  ...
  (dc: DataConnect, vars: GetMarketStatusVariables): QueryRef<GetMarketStatusData, GetMarketStatusVariables>;
}
export const getMarketStatusRef: GetMarketStatusRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getMarketStatusRef:
```typescript
const name = getMarketStatusRef.operationName;
console.log(name);
```

### Variables
The `GetMarketStatus` query requires an argument of type `GetMarketStatusVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetMarketStatusVariables {
  market: string;
}
```
### Return Type
Recall that executing the `GetMarketStatus` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetMarketStatusData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetMarketStatusData {
  status?: unknown | null;
}
```
### Using `GetMarketStatus`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getMarketStatus, GetMarketStatusVariables } from '@firebasegen/admin-connector';

// The `GetMarketStatus` query requires an argument of type `GetMarketStatusVariables`:
const getMarketStatusVars: GetMarketStatusVariables = {
  market: ..., 
};

// Call the `getMarketStatus()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getMarketStatus(getMarketStatusVars);
// Variables can be defined inline as well.
const { data } = await getMarketStatus({ market: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getMarketStatus(dataConnect, getMarketStatusVars);

console.log(data.status);

// Or, you can use the `Promise` API.
getMarketStatus(getMarketStatusVars).then((response) => {
  const data = response.data;
  console.log(data.status);
});
```

### Using `GetMarketStatus`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getMarketStatusRef, GetMarketStatusVariables } from '@firebasegen/admin-connector';

// The `GetMarketStatus` query requires an argument of type `GetMarketStatusVariables`:
const getMarketStatusVars: GetMarketStatusVariables = {
  market: ..., 
};

// Call the `getMarketStatusRef()` function to get a reference to the query.
const ref = getMarketStatusRef(getMarketStatusVars);
// Variables can be defined inline as well.
const ref = getMarketStatusRef({ market: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getMarketStatusRef(dataConnect, getMarketStatusVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.status);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.status);
});
```

## GetLatestRefreshJobs
You can execute the `GetLatestRefreshJobs` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getLatestRefreshJobs(vars: GetLatestRefreshJobsVariables, options?: ExecuteQueryOptions): QueryPromise<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;

interface GetLatestRefreshJobsRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetLatestRefreshJobsVariables): QueryRef<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;
}
export const getLatestRefreshJobsRef: GetLatestRefreshJobsRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getLatestRefreshJobs(dc: DataConnect, vars: GetLatestRefreshJobsVariables, options?: ExecuteQueryOptions): QueryPromise<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;

interface GetLatestRefreshJobsRef {
  ...
  (dc: DataConnect, vars: GetLatestRefreshJobsVariables): QueryRef<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;
}
export const getLatestRefreshJobsRef: GetLatestRefreshJobsRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getLatestRefreshJobsRef:
```typescript
const name = getLatestRefreshJobsRef.operationName;
console.log(name);
```

### Variables
The `GetLatestRefreshJobs` query requires an argument of type `GetLatestRefreshJobsVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetLatestRefreshJobsVariables {
  market: string;
  limit: number;
}
```
### Return Type
Recall that executing the `GetLatestRefreshJobs` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetLatestRefreshJobsData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetLatestRefreshJobsData {
  jobs?: unknown[] | null;
}
```
### Using `GetLatestRefreshJobs`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getLatestRefreshJobs, GetLatestRefreshJobsVariables } from '@firebasegen/admin-connector';

// The `GetLatestRefreshJobs` query requires an argument of type `GetLatestRefreshJobsVariables`:
const getLatestRefreshJobsVars: GetLatestRefreshJobsVariables = {
  market: ..., 
  limit: ..., 
};

// Call the `getLatestRefreshJobs()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getLatestRefreshJobs(getLatestRefreshJobsVars);
// Variables can be defined inline as well.
const { data } = await getLatestRefreshJobs({ market: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getLatestRefreshJobs(dataConnect, getLatestRefreshJobsVars);

console.log(data.jobs);

// Or, you can use the `Promise` API.
getLatestRefreshJobs(getLatestRefreshJobsVars).then((response) => {
  const data = response.data;
  console.log(data.jobs);
});
```

### Using `GetLatestRefreshJobs`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getLatestRefreshJobsRef, GetLatestRefreshJobsVariables } from '@firebasegen/admin-connector';

// The `GetLatestRefreshJobs` query requires an argument of type `GetLatestRefreshJobsVariables`:
const getLatestRefreshJobsVars: GetLatestRefreshJobsVariables = {
  market: ..., 
  limit: ..., 
};

// Call the `getLatestRefreshJobsRef()` function to get a reference to the query.
const ref = getLatestRefreshJobsRef(getLatestRefreshJobsVars);
// Variables can be defined inline as well.
const ref = getLatestRefreshJobsRef({ market: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getLatestRefreshJobsRef(dataConnect, getLatestRefreshJobsVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.jobs);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.jobs);
});
```

## GetWeeklyMetricSnapshot
You can execute the `GetWeeklyMetricSnapshot` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getWeeklyMetricSnapshot(vars: GetWeeklyMetricSnapshotVariables, options?: ExecuteQueryOptions): QueryPromise<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;

interface GetWeeklyMetricSnapshotRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetWeeklyMetricSnapshotVariables): QueryRef<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;
}
export const getWeeklyMetricSnapshotRef: GetWeeklyMetricSnapshotRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getWeeklyMetricSnapshot(dc: DataConnect, vars: GetWeeklyMetricSnapshotVariables, options?: ExecuteQueryOptions): QueryPromise<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;

interface GetWeeklyMetricSnapshotRef {
  ...
  (dc: DataConnect, vars: GetWeeklyMetricSnapshotVariables): QueryRef<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;
}
export const getWeeklyMetricSnapshotRef: GetWeeklyMetricSnapshotRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getWeeklyMetricSnapshotRef:
```typescript
const name = getWeeklyMetricSnapshotRef.operationName;
console.log(name);
```

### Variables
The `GetWeeklyMetricSnapshot` query requires an argument of type `GetWeeklyMetricSnapshotVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetWeeklyMetricSnapshotVariables {
  market: string;
  provider: string;
  ticker: string;
  limit: number;
}
```
### Return Type
Recall that executing the `GetWeeklyMetricSnapshot` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetWeeklyMetricSnapshotData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetWeeklyMetricSnapshotData {
  metrics?: unknown[] | null;
}
```
### Using `GetWeeklyMetricSnapshot`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getWeeklyMetricSnapshot, GetWeeklyMetricSnapshotVariables } from '@firebasegen/admin-connector';

// The `GetWeeklyMetricSnapshot` query requires an argument of type `GetWeeklyMetricSnapshotVariables`:
const getWeeklyMetricSnapshotVars: GetWeeklyMetricSnapshotVariables = {
  market: ..., 
  provider: ..., 
  ticker: ..., 
  limit: ..., 
};

// Call the `getWeeklyMetricSnapshot()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getWeeklyMetricSnapshot(getWeeklyMetricSnapshotVars);
// Variables can be defined inline as well.
const { data } = await getWeeklyMetricSnapshot({ market: ..., provider: ..., ticker: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getWeeklyMetricSnapshot(dataConnect, getWeeklyMetricSnapshotVars);

console.log(data.metrics);

// Or, you can use the `Promise` API.
getWeeklyMetricSnapshot(getWeeklyMetricSnapshotVars).then((response) => {
  const data = response.data;
  console.log(data.metrics);
});
```

### Using `GetWeeklyMetricSnapshot`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getWeeklyMetricSnapshotRef, GetWeeklyMetricSnapshotVariables } from '@firebasegen/admin-connector';

// The `GetWeeklyMetricSnapshot` query requires an argument of type `GetWeeklyMetricSnapshotVariables`:
const getWeeklyMetricSnapshotVars: GetWeeklyMetricSnapshotVariables = {
  market: ..., 
  provider: ..., 
  ticker: ..., 
  limit: ..., 
};

// Call the `getWeeklyMetricSnapshotRef()` function to get a reference to the query.
const ref = getWeeklyMetricSnapshotRef(getWeeklyMetricSnapshotVars);
// Variables can be defined inline as well.
const ref = getWeeklyMetricSnapshotRef({ market: ..., provider: ..., ticker: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getWeeklyMetricSnapshotRef(dataConnect, getWeeklyMetricSnapshotVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.metrics);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.metrics);
});
```

## GetScanResults
You can execute the `GetScanResults` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getScanResults(vars: GetScanResultsVariables, options?: ExecuteQueryOptions): QueryPromise<GetScanResultsData, GetScanResultsVariables>;

interface GetScanResultsRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetScanResultsVariables): QueryRef<GetScanResultsData, GetScanResultsVariables>;
}
export const getScanResultsRef: GetScanResultsRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getScanResults(dc: DataConnect, vars: GetScanResultsVariables, options?: ExecuteQueryOptions): QueryPromise<GetScanResultsData, GetScanResultsVariables>;

interface GetScanResultsRef {
  ...
  (dc: DataConnect, vars: GetScanResultsVariables): QueryRef<GetScanResultsData, GetScanResultsVariables>;
}
export const getScanResultsRef: GetScanResultsRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getScanResultsRef:
```typescript
const name = getScanResultsRef.operationName;
console.log(name);
```

### Variables
The `GetScanResults` query requires an argument of type `GetScanResultsVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetScanResultsVariables {
  scanId: Int64String;
}
```
### Return Type
Recall that executing the `GetScanResults` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetScanResultsData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetScanResultsData {
  results?: unknown[] | null;
}
```
### Using `GetScanResults`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getScanResults, GetScanResultsVariables } from '@firebasegen/admin-connector';

// The `GetScanResults` query requires an argument of type `GetScanResultsVariables`:
const getScanResultsVars: GetScanResultsVariables = {
  scanId: ..., 
};

// Call the `getScanResults()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getScanResults(getScanResultsVars);
// Variables can be defined inline as well.
const { data } = await getScanResults({ scanId: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getScanResults(dataConnect, getScanResultsVars);

console.log(data.results);

// Or, you can use the `Promise` API.
getScanResults(getScanResultsVars).then((response) => {
  const data = response.data;
  console.log(data.results);
});
```

### Using `GetScanResults`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getScanResultsRef, GetScanResultsVariables } from '@firebasegen/admin-connector';

// The `GetScanResults` query requires an argument of type `GetScanResultsVariables`:
const getScanResultsVars: GetScanResultsVariables = {
  scanId: ..., 
};

// Call the `getScanResultsRef()` function to get a reference to the query.
const ref = getScanResultsRef(getScanResultsVars);
// Variables can be defined inline as well.
const ref = getScanResultsRef({ scanId: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getScanResultsRef(dataConnect, getScanResultsVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.results);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.results);
});
```

## GetChartBars
You can execute the `GetChartBars` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getChartBars(vars: GetChartBarsVariables, options?: ExecuteQueryOptions): QueryPromise<GetChartBarsData, GetChartBarsVariables>;

interface GetChartBarsRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetChartBarsVariables): QueryRef<GetChartBarsData, GetChartBarsVariables>;
}
export const getChartBarsRef: GetChartBarsRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getChartBars(dc: DataConnect, vars: GetChartBarsVariables, options?: ExecuteQueryOptions): QueryPromise<GetChartBarsData, GetChartBarsVariables>;

interface GetChartBarsRef {
  ...
  (dc: DataConnect, vars: GetChartBarsVariables): QueryRef<GetChartBarsData, GetChartBarsVariables>;
}
export const getChartBarsRef: GetChartBarsRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getChartBarsRef:
```typescript
const name = getChartBarsRef.operationName;
console.log(name);
```

### Variables
The `GetChartBars` query requires an argument of type `GetChartBarsVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetChartBarsVariables {
  market: string;
  provider: string;
  ticker: string;
  startDate: DateString;
}
```
### Return Type
Recall that executing the `GetChartBars` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetChartBarsData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetChartBarsData {
  bars?: unknown[] | null;
}
```
### Using `GetChartBars`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getChartBars, GetChartBarsVariables } from '@firebasegen/admin-connector';

// The `GetChartBars` query requires an argument of type `GetChartBarsVariables`:
const getChartBarsVars: GetChartBarsVariables = {
  market: ..., 
  provider: ..., 
  ticker: ..., 
  startDate: ..., 
};

// Call the `getChartBars()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getChartBars(getChartBarsVars);
// Variables can be defined inline as well.
const { data } = await getChartBars({ market: ..., provider: ..., ticker: ..., startDate: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getChartBars(dataConnect, getChartBarsVars);

console.log(data.bars);

// Or, you can use the `Promise` API.
getChartBars(getChartBarsVars).then((response) => {
  const data = response.data;
  console.log(data.bars);
});
```

### Using `GetChartBars`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getChartBarsRef, GetChartBarsVariables } from '@firebasegen/admin-connector';

// The `GetChartBars` query requires an argument of type `GetChartBarsVariables`:
const getChartBarsVars: GetChartBarsVariables = {
  market: ..., 
  provider: ..., 
  ticker: ..., 
  startDate: ..., 
};

// Call the `getChartBarsRef()` function to get a reference to the query.
const ref = getChartBarsRef(getChartBarsVars);
// Variables can be defined inline as well.
const ref = getChartBarsRef({ market: ..., provider: ..., ticker: ..., startDate: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getChartBarsRef(dataConnect, getChartBarsVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.bars);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.bars);
});
```

## GetCompanyProfile
You can execute the `GetCompanyProfile` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getCompanyProfile(vars: GetCompanyProfileVariables, options?: ExecuteQueryOptions): QueryPromise<GetCompanyProfileData, GetCompanyProfileVariables>;

interface GetCompanyProfileRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetCompanyProfileVariables): QueryRef<GetCompanyProfileData, GetCompanyProfileVariables>;
}
export const getCompanyProfileRef: GetCompanyProfileRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getCompanyProfile(dc: DataConnect, vars: GetCompanyProfileVariables, options?: ExecuteQueryOptions): QueryPromise<GetCompanyProfileData, GetCompanyProfileVariables>;

interface GetCompanyProfileRef {
  ...
  (dc: DataConnect, vars: GetCompanyProfileVariables): QueryRef<GetCompanyProfileData, GetCompanyProfileVariables>;
}
export const getCompanyProfileRef: GetCompanyProfileRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getCompanyProfileRef:
```typescript
const name = getCompanyProfileRef.operationName;
console.log(name);
```

### Variables
The `GetCompanyProfile` query requires an argument of type `GetCompanyProfileVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetCompanyProfileVariables {
  market: string;
  ticker: string;
}
```
### Return Type
Recall that executing the `GetCompanyProfile` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetCompanyProfileData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetCompanyProfileData {
  company?: unknown | null;
}
```
### Using `GetCompanyProfile`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getCompanyProfile, GetCompanyProfileVariables } from '@firebasegen/admin-connector';

// The `GetCompanyProfile` query requires an argument of type `GetCompanyProfileVariables`:
const getCompanyProfileVars: GetCompanyProfileVariables = {
  market: ..., 
  ticker: ..., 
};

// Call the `getCompanyProfile()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getCompanyProfile(getCompanyProfileVars);
// Variables can be defined inline as well.
const { data } = await getCompanyProfile({ market: ..., ticker: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getCompanyProfile(dataConnect, getCompanyProfileVars);

console.log(data.company);

// Or, you can use the `Promise` API.
getCompanyProfile(getCompanyProfileVars).then((response) => {
  const data = response.data;
  console.log(data.company);
});
```

### Using `GetCompanyProfile`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getCompanyProfileRef, GetCompanyProfileVariables } from '@firebasegen/admin-connector';

// The `GetCompanyProfile` query requires an argument of type `GetCompanyProfileVariables`:
const getCompanyProfileVars: GetCompanyProfileVariables = {
  market: ..., 
  ticker: ..., 
};

// Call the `getCompanyProfileRef()` function to get a reference to the query.
const ref = getCompanyProfileRef(getCompanyProfileVars);
// Variables can be defined inline as well.
const ref = getCompanyProfileRef({ market: ..., ticker: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getCompanyProfileRef(dataConnect, getCompanyProfileVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.company);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.company);
});
```

## GetMyAppraisals
You can execute the `GetMyAppraisals` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getMyAppraisals(vars: GetMyAppraisalsVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyAppraisalsData, GetMyAppraisalsVariables>;

interface GetMyAppraisalsRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMyAppraisalsVariables): QueryRef<GetMyAppraisalsData, GetMyAppraisalsVariables>;
}
export const getMyAppraisalsRef: GetMyAppraisalsRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getMyAppraisals(dc: DataConnect, vars: GetMyAppraisalsVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyAppraisalsData, GetMyAppraisalsVariables>;

interface GetMyAppraisalsRef {
  ...
  (dc: DataConnect, vars: GetMyAppraisalsVariables): QueryRef<GetMyAppraisalsData, GetMyAppraisalsVariables>;
}
export const getMyAppraisalsRef: GetMyAppraisalsRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getMyAppraisalsRef:
```typescript
const name = getMyAppraisalsRef.operationName;
console.log(name);
```

### Variables
The `GetMyAppraisals` query requires an argument of type `GetMyAppraisalsVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetMyAppraisalsVariables {
  scanId: Int64String;
}
```
### Return Type
Recall that executing the `GetMyAppraisals` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetMyAppraisalsData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetMyAppraisalsData {
  appraisals?: unknown[] | null;
}
```
### Using `GetMyAppraisals`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getMyAppraisals, GetMyAppraisalsVariables } from '@firebasegen/admin-connector';

// The `GetMyAppraisals` query requires an argument of type `GetMyAppraisalsVariables`:
const getMyAppraisalsVars: GetMyAppraisalsVariables = {
  scanId: ..., 
};

// Call the `getMyAppraisals()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getMyAppraisals(getMyAppraisalsVars);
// Variables can be defined inline as well.
const { data } = await getMyAppraisals({ scanId: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getMyAppraisals(dataConnect, getMyAppraisalsVars);

console.log(data.appraisals);

// Or, you can use the `Promise` API.
getMyAppraisals(getMyAppraisalsVars).then((response) => {
  const data = response.data;
  console.log(data.appraisals);
});
```

### Using `GetMyAppraisals`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getMyAppraisalsRef, GetMyAppraisalsVariables } from '@firebasegen/admin-connector';

// The `GetMyAppraisals` query requires an argument of type `GetMyAppraisalsVariables`:
const getMyAppraisalsVars: GetMyAppraisalsVariables = {
  scanId: ..., 
};

// Call the `getMyAppraisalsRef()` function to get a reference to the query.
const ref = getMyAppraisalsRef(getMyAppraisalsVars);
// Variables can be defined inline as well.
const ref = getMyAppraisalsRef({ scanId: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getMyAppraisalsRef(dataConnect, getMyAppraisalsVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.appraisals);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.appraisals);
});
```

## GetMySavedPicks
You can execute the `GetMySavedPicks` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getMySavedPicks(vars: GetMySavedPicksVariables, options?: ExecuteQueryOptions): QueryPromise<GetMySavedPicksData, GetMySavedPicksVariables>;

interface GetMySavedPicksRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMySavedPicksVariables): QueryRef<GetMySavedPicksData, GetMySavedPicksVariables>;
}
export const getMySavedPicksRef: GetMySavedPicksRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getMySavedPicks(dc: DataConnect, vars: GetMySavedPicksVariables, options?: ExecuteQueryOptions): QueryPromise<GetMySavedPicksData, GetMySavedPicksVariables>;

interface GetMySavedPicksRef {
  ...
  (dc: DataConnect, vars: GetMySavedPicksVariables): QueryRef<GetMySavedPicksData, GetMySavedPicksVariables>;
}
export const getMySavedPicksRef: GetMySavedPicksRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getMySavedPicksRef:
```typescript
const name = getMySavedPicksRef.operationName;
console.log(name);
```

### Variables
The `GetMySavedPicks` query requires an argument of type `GetMySavedPicksVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetMySavedPicksVariables {
  market?: string | null;
  limit: number;
}
```
### Return Type
Recall that executing the `GetMySavedPicks` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetMySavedPicksData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetMySavedPicksData {
  picks?: unknown[] | null;
}
```
### Using `GetMySavedPicks`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getMySavedPicks, GetMySavedPicksVariables } from '@firebasegen/admin-connector';

// The `GetMySavedPicks` query requires an argument of type `GetMySavedPicksVariables`:
const getMySavedPicksVars: GetMySavedPicksVariables = {
  market: ..., // optional
  limit: ..., 
};

// Call the `getMySavedPicks()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getMySavedPicks(getMySavedPicksVars);
// Variables can be defined inline as well.
const { data } = await getMySavedPicks({ market: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getMySavedPicks(dataConnect, getMySavedPicksVars);

console.log(data.picks);

// Or, you can use the `Promise` API.
getMySavedPicks(getMySavedPicksVars).then((response) => {
  const data = response.data;
  console.log(data.picks);
});
```

### Using `GetMySavedPicks`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getMySavedPicksRef, GetMySavedPicksVariables } from '@firebasegen/admin-connector';

// The `GetMySavedPicks` query requires an argument of type `GetMySavedPicksVariables`:
const getMySavedPicksVars: GetMySavedPicksVariables = {
  market: ..., // optional
  limit: ..., 
};

// Call the `getMySavedPicksRef()` function to get a reference to the query.
const ref = getMySavedPicksRef(getMySavedPicksVars);
// Variables can be defined inline as well.
const ref = getMySavedPicksRef({ market: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getMySavedPicksRef(dataConnect, getMySavedPicksVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.picks);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.picks);
});
```

## GetMyRatingHistory
You can execute the `GetMyRatingHistory` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getMyRatingHistory(vars: GetMyRatingHistoryVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;

interface GetMyRatingHistoryRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMyRatingHistoryVariables): QueryRef<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;
}
export const getMyRatingHistoryRef: GetMyRatingHistoryRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getMyRatingHistory(dc: DataConnect, vars: GetMyRatingHistoryVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;

interface GetMyRatingHistoryRef {
  ...
  (dc: DataConnect, vars: GetMyRatingHistoryVariables): QueryRef<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;
}
export const getMyRatingHistoryRef: GetMyRatingHistoryRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getMyRatingHistoryRef:
```typescript
const name = getMyRatingHistoryRef.operationName;
console.log(name);
```

### Variables
The `GetMyRatingHistory` query requires an argument of type `GetMyRatingHistoryVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetMyRatingHistoryVariables {
  ticker?: string | null;
  limit: number;
}
```
### Return Type
Recall that executing the `GetMyRatingHistory` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetMyRatingHistoryData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetMyRatingHistoryData {
  events?: unknown[] | null;
}
```
### Using `GetMyRatingHistory`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getMyRatingHistory, GetMyRatingHistoryVariables } from '@firebasegen/admin-connector';

// The `GetMyRatingHistory` query requires an argument of type `GetMyRatingHistoryVariables`:
const getMyRatingHistoryVars: GetMyRatingHistoryVariables = {
  ticker: ..., // optional
  limit: ..., 
};

// Call the `getMyRatingHistory()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getMyRatingHistory(getMyRatingHistoryVars);
// Variables can be defined inline as well.
const { data } = await getMyRatingHistory({ ticker: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getMyRatingHistory(dataConnect, getMyRatingHistoryVars);

console.log(data.events);

// Or, you can use the `Promise` API.
getMyRatingHistory(getMyRatingHistoryVars).then((response) => {
  const data = response.data;
  console.log(data.events);
});
```

### Using `GetMyRatingHistory`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getMyRatingHistoryRef, GetMyRatingHistoryVariables } from '@firebasegen/admin-connector';

// The `GetMyRatingHistory` query requires an argument of type `GetMyRatingHistoryVariables`:
const getMyRatingHistoryVars: GetMyRatingHistoryVariables = {
  ticker: ..., // optional
  limit: ..., 
};

// Call the `getMyRatingHistoryRef()` function to get a reference to the query.
const ref = getMyRatingHistoryRef(getMyRatingHistoryVars);
// Variables can be defined inline as well.
const ref = getMyRatingHistoryRef({ ticker: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getMyRatingHistoryRef(dataConnect, getMyRatingHistoryVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.events);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.events);
});
```

## GetMyNotes
You can execute the `GetMyNotes` query using the following action shortcut function, or by calling `executeQuery()` after calling the following `QueryRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
getMyNotes(vars: GetMyNotesVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyNotesData, GetMyNotesVariables>;

interface GetMyNotesRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMyNotesVariables): QueryRef<GetMyNotesData, GetMyNotesVariables>;
}
export const getMyNotesRef: GetMyNotesRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `QueryRef` function.
```typescript
getMyNotes(dc: DataConnect, vars: GetMyNotesVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyNotesData, GetMyNotesVariables>;

interface GetMyNotesRef {
  ...
  (dc: DataConnect, vars: GetMyNotesVariables): QueryRef<GetMyNotesData, GetMyNotesVariables>;
}
export const getMyNotesRef: GetMyNotesRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the getMyNotesRef:
```typescript
const name = getMyNotesRef.operationName;
console.log(name);
```

### Variables
The `GetMyNotes` query requires an argument of type `GetMyNotesVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface GetMyNotesVariables {
  market?: string | null;
  ticker?: string | null;
  limit: number;
}
```
### Return Type
Recall that executing the `GetMyNotes` query returns a `QueryPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `GetMyNotesData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface GetMyNotesData {
  notes?: unknown[] | null;
}
```
### Using `GetMyNotes`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, getMyNotes, GetMyNotesVariables } from '@firebasegen/admin-connector';

// The `GetMyNotes` query requires an argument of type `GetMyNotesVariables`:
const getMyNotesVars: GetMyNotesVariables = {
  market: ..., // optional
  ticker: ..., // optional
  limit: ..., 
};

// Call the `getMyNotes()` function to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await getMyNotes(getMyNotesVars);
// Variables can be defined inline as well.
const { data } = await getMyNotes({ market: ..., ticker: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await getMyNotes(dataConnect, getMyNotesVars);

console.log(data.notes);

// Or, you can use the `Promise` API.
getMyNotes(getMyNotesVars).then((response) => {
  const data = response.data;
  console.log(data.notes);
});
```

### Using `GetMyNotes`'s `QueryRef` function

```typescript
import { getDataConnect, executeQuery } from 'firebase/data-connect';
import { connectorConfig, getMyNotesRef, GetMyNotesVariables } from '@firebasegen/admin-connector';

// The `GetMyNotes` query requires an argument of type `GetMyNotesVariables`:
const getMyNotesVars: GetMyNotesVariables = {
  market: ..., // optional
  ticker: ..., // optional
  limit: ..., 
};

// Call the `getMyNotesRef()` function to get a reference to the query.
const ref = getMyNotesRef(getMyNotesVars);
// Variables can be defined inline as well.
const ref = getMyNotesRef({ market: ..., ticker: ..., limit: ..., });

// You can also pass in a `DataConnect` instance to the `QueryRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = getMyNotesRef(dataConnect, getMyNotesVars);

// Call `executeQuery()` on the reference to execute the query.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeQuery(ref);

console.log(data.notes);

// Or, you can use the `Promise` API.
executeQuery(ref).then((response) => {
  const data = response.data;
  console.log(data.notes);
});
```

# Mutations

There are two ways to execute a Data Connect Mutation using the generated Web SDK:
- Using a Mutation Reference function, which returns a `MutationRef`
  - The `MutationRef` can be used as an argument to `executeMutation()`, which will execute the Mutation and return a `MutationPromise`
- Using an action shortcut function, which returns a `MutationPromise`
  - Calling the action shortcut function will execute the Mutation and return a `MutationPromise`

The following is true for both the action shortcut function and the `MutationRef` function:
- The `MutationPromise` returned will resolve to the result of the Mutation once it has finished executing
- If the Mutation accepts arguments, both the action shortcut function and the `MutationRef` function accept a single argument: an object that contains all the required variables (and the optional variables) for the Mutation
- Both functions can be called with or without passing in a `DataConnect` instance as an argument. If no `DataConnect` argument is passed in, then the generated SDK will call `getDataConnect(connectorConfig)` behind the scenes for you.

Below are examples of how to use the `admin` connector's generated functions to execute each mutation. You can also follow the examples from the [Data Connect documentation](https://firebase.google.com/docs/data-connect/web-sdk#using-mutations).

## UpsertMyAppraisal
You can execute the `UpsertMyAppraisal` mutation using the following action shortcut function, or by calling `executeMutation()` after calling the following `MutationRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
upsertMyAppraisal(vars: UpsertMyAppraisalVariables): MutationPromise<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;

interface UpsertMyAppraisalRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: UpsertMyAppraisalVariables): MutationRef<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;
}
export const upsertMyAppraisalRef: UpsertMyAppraisalRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `MutationRef` function.
```typescript
upsertMyAppraisal(dc: DataConnect, vars: UpsertMyAppraisalVariables): MutationPromise<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;

interface UpsertMyAppraisalRef {
  ...
  (dc: DataConnect, vars: UpsertMyAppraisalVariables): MutationRef<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;
}
export const upsertMyAppraisalRef: UpsertMyAppraisalRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the upsertMyAppraisalRef:
```typescript
const name = upsertMyAppraisalRef.operationName;
console.log(name);
```

### Variables
The `UpsertMyAppraisal` mutation requires an argument of type `UpsertMyAppraisalVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface UpsertMyAppraisalVariables {
  scanId: Int64String;
  sourceId: Int64String;
  market: string;
  ticker: string;
  label: string;
  status?: string | null;
}
```
### Return Type
Recall that executing the `UpsertMyAppraisal` mutation returns a `MutationPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `UpsertMyAppraisalData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface UpsertMyAppraisalData {
  appraisal?: unknown | null;
}
```
### Using `UpsertMyAppraisal`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, upsertMyAppraisal, UpsertMyAppraisalVariables } from '@firebasegen/admin-connector';

// The `UpsertMyAppraisal` mutation requires an argument of type `UpsertMyAppraisalVariables`:
const upsertMyAppraisalVars: UpsertMyAppraisalVariables = {
  scanId: ..., 
  sourceId: ..., 
  market: ..., 
  ticker: ..., 
  label: ..., 
  status: ..., // optional
};

// Call the `upsertMyAppraisal()` function to execute the mutation.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await upsertMyAppraisal(upsertMyAppraisalVars);
// Variables can be defined inline as well.
const { data } = await upsertMyAppraisal({ scanId: ..., sourceId: ..., market: ..., ticker: ..., label: ..., status: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await upsertMyAppraisal(dataConnect, upsertMyAppraisalVars);

console.log(data.appraisal);

// Or, you can use the `Promise` API.
upsertMyAppraisal(upsertMyAppraisalVars).then((response) => {
  const data = response.data;
  console.log(data.appraisal);
});
```

### Using `UpsertMyAppraisal`'s `MutationRef` function

```typescript
import { getDataConnect, executeMutation } from 'firebase/data-connect';
import { connectorConfig, upsertMyAppraisalRef, UpsertMyAppraisalVariables } from '@firebasegen/admin-connector';

// The `UpsertMyAppraisal` mutation requires an argument of type `UpsertMyAppraisalVariables`:
const upsertMyAppraisalVars: UpsertMyAppraisalVariables = {
  scanId: ..., 
  sourceId: ..., 
  market: ..., 
  ticker: ..., 
  label: ..., 
  status: ..., // optional
};

// Call the `upsertMyAppraisalRef()` function to get a reference to the mutation.
const ref = upsertMyAppraisalRef(upsertMyAppraisalVars);
// Variables can be defined inline as well.
const ref = upsertMyAppraisalRef({ scanId: ..., sourceId: ..., market: ..., ticker: ..., label: ..., status: ..., });

// You can also pass in a `DataConnect` instance to the `MutationRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = upsertMyAppraisalRef(dataConnect, upsertMyAppraisalVars);

// Call `executeMutation()` on the reference to execute the mutation.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeMutation(ref);

console.log(data.appraisal);

// Or, you can use the `Promise` API.
executeMutation(ref).then((response) => {
  const data = response.data;
  console.log(data.appraisal);
});
```

## ClearMyAppraisal
You can execute the `ClearMyAppraisal` mutation using the following action shortcut function, or by calling `executeMutation()` after calling the following `MutationRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
clearMyAppraisal(vars: ClearMyAppraisalVariables): MutationPromise<ClearMyAppraisalData, ClearMyAppraisalVariables>;

interface ClearMyAppraisalRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: ClearMyAppraisalVariables): MutationRef<ClearMyAppraisalData, ClearMyAppraisalVariables>;
}
export const clearMyAppraisalRef: ClearMyAppraisalRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `MutationRef` function.
```typescript
clearMyAppraisal(dc: DataConnect, vars: ClearMyAppraisalVariables): MutationPromise<ClearMyAppraisalData, ClearMyAppraisalVariables>;

interface ClearMyAppraisalRef {
  ...
  (dc: DataConnect, vars: ClearMyAppraisalVariables): MutationRef<ClearMyAppraisalData, ClearMyAppraisalVariables>;
}
export const clearMyAppraisalRef: ClearMyAppraisalRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the clearMyAppraisalRef:
```typescript
const name = clearMyAppraisalRef.operationName;
console.log(name);
```

### Variables
The `ClearMyAppraisal` mutation requires an argument of type `ClearMyAppraisalVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface ClearMyAppraisalVariables {
  scanId: Int64String;
  sourceId: Int64String;
  ticker: string;
}
```
### Return Type
Recall that executing the `ClearMyAppraisal` mutation returns a `MutationPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `ClearMyAppraisalData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface ClearMyAppraisalData {
  cleared?: number | null;
}
```
### Using `ClearMyAppraisal`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, clearMyAppraisal, ClearMyAppraisalVariables } from '@firebasegen/admin-connector';

// The `ClearMyAppraisal` mutation requires an argument of type `ClearMyAppraisalVariables`:
const clearMyAppraisalVars: ClearMyAppraisalVariables = {
  scanId: ..., 
  sourceId: ..., 
  ticker: ..., 
};

// Call the `clearMyAppraisal()` function to execute the mutation.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await clearMyAppraisal(clearMyAppraisalVars);
// Variables can be defined inline as well.
const { data } = await clearMyAppraisal({ scanId: ..., sourceId: ..., ticker: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await clearMyAppraisal(dataConnect, clearMyAppraisalVars);

console.log(data.cleared);

// Or, you can use the `Promise` API.
clearMyAppraisal(clearMyAppraisalVars).then((response) => {
  const data = response.data;
  console.log(data.cleared);
});
```

### Using `ClearMyAppraisal`'s `MutationRef` function

```typescript
import { getDataConnect, executeMutation } from 'firebase/data-connect';
import { connectorConfig, clearMyAppraisalRef, ClearMyAppraisalVariables } from '@firebasegen/admin-connector';

// The `ClearMyAppraisal` mutation requires an argument of type `ClearMyAppraisalVariables`:
const clearMyAppraisalVars: ClearMyAppraisalVariables = {
  scanId: ..., 
  sourceId: ..., 
  ticker: ..., 
};

// Call the `clearMyAppraisalRef()` function to get a reference to the mutation.
const ref = clearMyAppraisalRef(clearMyAppraisalVars);
// Variables can be defined inline as well.
const ref = clearMyAppraisalRef({ scanId: ..., sourceId: ..., ticker: ..., });

// You can also pass in a `DataConnect` instance to the `MutationRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = clearMyAppraisalRef(dataConnect, clearMyAppraisalVars);

// Call `executeMutation()` on the reference to execute the mutation.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeMutation(ref);

console.log(data.cleared);

// Or, you can use the `Promise` API.
executeMutation(ref).then((response) => {
  const data = response.data;
  console.log(data.cleared);
});
```

## UpsertMyNote
You can execute the `UpsertMyNote` mutation using the following action shortcut function, or by calling `executeMutation()` after calling the following `MutationRef` function, both of which are defined in [dataconnect-generated/index.d.ts](./index.d.ts):
```typescript
upsertMyNote(vars: UpsertMyNoteVariables): MutationPromise<UpsertMyNoteData, UpsertMyNoteVariables>;

interface UpsertMyNoteRef {
  ...
  /* Allow users to create refs without passing in DataConnect */
  (vars: UpsertMyNoteVariables): MutationRef<UpsertMyNoteData, UpsertMyNoteVariables>;
}
export const upsertMyNoteRef: UpsertMyNoteRef;
```
You can also pass in a `DataConnect` instance to the action shortcut function or `MutationRef` function.
```typescript
upsertMyNote(dc: DataConnect, vars: UpsertMyNoteVariables): MutationPromise<UpsertMyNoteData, UpsertMyNoteVariables>;

interface UpsertMyNoteRef {
  ...
  (dc: DataConnect, vars: UpsertMyNoteVariables): MutationRef<UpsertMyNoteData, UpsertMyNoteVariables>;
}
export const upsertMyNoteRef: UpsertMyNoteRef;
```

If you need the name of the operation without creating a ref, you can retrieve the operation name by calling the `operationName` property on the upsertMyNoteRef:
```typescript
const name = upsertMyNoteRef.operationName;
console.log(name);
```

### Variables
The `UpsertMyNote` mutation requires an argument of type `UpsertMyNoteVariables`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:

```typescript
export interface UpsertMyNoteVariables {
  scanId: Int64String;
  sourceId: Int64String;
  market: string;
  ticker: string;
  note: string;
}
```
### Return Type
Recall that executing the `UpsertMyNote` mutation returns a `MutationPromise` that resolves to an object with a `data` property.

The `data` property is an object of type `UpsertMyNoteData`, which is defined in [dataconnect-generated/index.d.ts](./index.d.ts). It has the following fields:
```typescript
export interface UpsertMyNoteData {
  note?: unknown | null;
}
```
### Using `UpsertMyNote`'s action shortcut function

```typescript
import { getDataConnect } from 'firebase/data-connect';
import { connectorConfig, upsertMyNote, UpsertMyNoteVariables } from '@firebasegen/admin-connector';

// The `UpsertMyNote` mutation requires an argument of type `UpsertMyNoteVariables`:
const upsertMyNoteVars: UpsertMyNoteVariables = {
  scanId: ..., 
  sourceId: ..., 
  market: ..., 
  ticker: ..., 
  note: ..., 
};

// Call the `upsertMyNote()` function to execute the mutation.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await upsertMyNote(upsertMyNoteVars);
// Variables can be defined inline as well.
const { data } = await upsertMyNote({ scanId: ..., sourceId: ..., market: ..., ticker: ..., note: ..., });

// You can also pass in a `DataConnect` instance to the action shortcut function.
const dataConnect = getDataConnect(connectorConfig);
const { data } = await upsertMyNote(dataConnect, upsertMyNoteVars);

console.log(data.note);

// Or, you can use the `Promise` API.
upsertMyNote(upsertMyNoteVars).then((response) => {
  const data = response.data;
  console.log(data.note);
});
```

### Using `UpsertMyNote`'s `MutationRef` function

```typescript
import { getDataConnect, executeMutation } from 'firebase/data-connect';
import { connectorConfig, upsertMyNoteRef, UpsertMyNoteVariables } from '@firebasegen/admin-connector';

// The `UpsertMyNote` mutation requires an argument of type `UpsertMyNoteVariables`:
const upsertMyNoteVars: UpsertMyNoteVariables = {
  scanId: ..., 
  sourceId: ..., 
  market: ..., 
  ticker: ..., 
  note: ..., 
};

// Call the `upsertMyNoteRef()` function to get a reference to the mutation.
const ref = upsertMyNoteRef(upsertMyNoteVars);
// Variables can be defined inline as well.
const ref = upsertMyNoteRef({ scanId: ..., sourceId: ..., market: ..., ticker: ..., note: ..., });

// You can also pass in a `DataConnect` instance to the `MutationRef` function.
const dataConnect = getDataConnect(connectorConfig);
const ref = upsertMyNoteRef(dataConnect, upsertMyNoteVars);

// Call `executeMutation()` on the reference to execute the mutation.
// You can use the `await` keyword to wait for the promise to resolve.
const { data } = await executeMutation(ref);

console.log(data.note);

// Or, you can use the `Promise` API.
executeMutation(ref).then((response) => {
  const data = response.data;
  console.log(data.note);
});
```

