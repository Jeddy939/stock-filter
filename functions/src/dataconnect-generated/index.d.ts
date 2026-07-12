import { ConnectorConfig, DataConnect, QueryRef, QueryPromise, ExecuteQueryOptions, MutationRef, MutationPromise } from 'firebase/data-connect';

export const connectorConfig: ConnectorConfig;

export type TimestampString = string;
export type UUIDString = string;
export type Int64String = string;
export type DateString = string;




export interface BackendMetadata_Key {
  key: string;
  __typename?: 'BackendMetadata_Key';
}

export interface ClearMyAppraisalData {
  cleared?: number | null;
}

export interface ClearMyAppraisalVariables {
  scanId: Int64String;
  sourceId: Int64String;
  ticker: string;
}

export interface GetChartBarsData {
  bars?: unknown[] | null;
}

export interface GetChartBarsVariables {
  market: string;
  provider: string;
  ticker: string;
  startDate: DateString;
}

export interface GetCompanyProfileData {
  company?: unknown | null;
}

export interface GetCompanyProfileVariables {
  market: string;
  ticker: string;
}

export interface GetLatestRefreshJobsData {
  jobs?: unknown[] | null;
}

export interface GetLatestRefreshJobsVariables {
  market: string;
  limit: number;
}

export interface GetMarketStatusData {
  status?: unknown | null;
}

export interface GetMarketStatusVariables {
  market: string;
}

export interface GetMyAppraisalsData {
  appraisals?: unknown[] | null;
}

export interface GetMyAppraisalsVariables {
  scanId: Int64String;
}

export interface GetMyNotesData {
  notes?: unknown[] | null;
}

export interface GetMyNotesVariables {
  market?: string | null;
  ticker?: string | null;
  limit: number;
}

export interface GetMyRatingHistoryData {
  events?: unknown[] | null;
}

export interface GetMyRatingHistoryVariables {
  ticker?: string | null;
  limit: number;
}

export interface GetMySavedPicksData {
  picks?: unknown[] | null;
}

export interface GetMySavedPicksVariables {
  market?: string | null;
  limit: number;
}

export interface GetScanResultsData {
  results?: unknown[] | null;
}

export interface GetScanResultsVariables {
  scanId: Int64String;
}

export interface GetWeeklyMetricSnapshotData {
  metrics?: unknown[] | null;
}

export interface GetWeeklyMetricSnapshotVariables {
  market: string;
  provider: string;
  ticker: string;
  limit: number;
}

export interface ListRecentScansData {
  scans?: unknown[] | null;
}

export interface ListRecentScansVariables {
  market: string;
  limit: number;
}

export interface UpsertMyAppraisalData {
  appraisal?: unknown | null;
}

export interface UpsertMyAppraisalVariables {
  scanId: Int64String;
  sourceId: Int64String;
  market: string;
  ticker: string;
  label: string;
  status?: string | null;
}

export interface UpsertMyNoteData {
  note?: unknown | null;
}

export interface UpsertMyNoteVariables {
  scanId: Int64String;
  sourceId: Int64String;
  market: string;
  ticker: string;
  note: string;
}

interface UpsertMyAppraisalRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: UpsertMyAppraisalVariables): MutationRef<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: UpsertMyAppraisalVariables): MutationRef<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;
  operationName: string;
}
export const upsertMyAppraisalRef: UpsertMyAppraisalRef;

export function upsertMyAppraisal(vars: UpsertMyAppraisalVariables): MutationPromise<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;
export function upsertMyAppraisal(dc: DataConnect, vars: UpsertMyAppraisalVariables): MutationPromise<UpsertMyAppraisalData, UpsertMyAppraisalVariables>;

interface ClearMyAppraisalRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: ClearMyAppraisalVariables): MutationRef<ClearMyAppraisalData, ClearMyAppraisalVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: ClearMyAppraisalVariables): MutationRef<ClearMyAppraisalData, ClearMyAppraisalVariables>;
  operationName: string;
}
export const clearMyAppraisalRef: ClearMyAppraisalRef;

export function clearMyAppraisal(vars: ClearMyAppraisalVariables): MutationPromise<ClearMyAppraisalData, ClearMyAppraisalVariables>;
export function clearMyAppraisal(dc: DataConnect, vars: ClearMyAppraisalVariables): MutationPromise<ClearMyAppraisalData, ClearMyAppraisalVariables>;

interface UpsertMyNoteRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: UpsertMyNoteVariables): MutationRef<UpsertMyNoteData, UpsertMyNoteVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: UpsertMyNoteVariables): MutationRef<UpsertMyNoteData, UpsertMyNoteVariables>;
  operationName: string;
}
export const upsertMyNoteRef: UpsertMyNoteRef;

export function upsertMyNote(vars: UpsertMyNoteVariables): MutationPromise<UpsertMyNoteData, UpsertMyNoteVariables>;
export function upsertMyNote(dc: DataConnect, vars: UpsertMyNoteVariables): MutationPromise<UpsertMyNoteData, UpsertMyNoteVariables>;

interface ListRecentScansRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: ListRecentScansVariables): QueryRef<ListRecentScansData, ListRecentScansVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: ListRecentScansVariables): QueryRef<ListRecentScansData, ListRecentScansVariables>;
  operationName: string;
}
export const listRecentScansRef: ListRecentScansRef;

export function listRecentScans(vars: ListRecentScansVariables, options?: ExecuteQueryOptions): QueryPromise<ListRecentScansData, ListRecentScansVariables>;
export function listRecentScans(dc: DataConnect, vars: ListRecentScansVariables, options?: ExecuteQueryOptions): QueryPromise<ListRecentScansData, ListRecentScansVariables>;

interface GetMarketStatusRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMarketStatusVariables): QueryRef<GetMarketStatusData, GetMarketStatusVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetMarketStatusVariables): QueryRef<GetMarketStatusData, GetMarketStatusVariables>;
  operationName: string;
}
export const getMarketStatusRef: GetMarketStatusRef;

export function getMarketStatus(vars: GetMarketStatusVariables, options?: ExecuteQueryOptions): QueryPromise<GetMarketStatusData, GetMarketStatusVariables>;
export function getMarketStatus(dc: DataConnect, vars: GetMarketStatusVariables, options?: ExecuteQueryOptions): QueryPromise<GetMarketStatusData, GetMarketStatusVariables>;

interface GetLatestRefreshJobsRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetLatestRefreshJobsVariables): QueryRef<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetLatestRefreshJobsVariables): QueryRef<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;
  operationName: string;
}
export const getLatestRefreshJobsRef: GetLatestRefreshJobsRef;

export function getLatestRefreshJobs(vars: GetLatestRefreshJobsVariables, options?: ExecuteQueryOptions): QueryPromise<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;
export function getLatestRefreshJobs(dc: DataConnect, vars: GetLatestRefreshJobsVariables, options?: ExecuteQueryOptions): QueryPromise<GetLatestRefreshJobsData, GetLatestRefreshJobsVariables>;

interface GetWeeklyMetricSnapshotRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetWeeklyMetricSnapshotVariables): QueryRef<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetWeeklyMetricSnapshotVariables): QueryRef<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;
  operationName: string;
}
export const getWeeklyMetricSnapshotRef: GetWeeklyMetricSnapshotRef;

export function getWeeklyMetricSnapshot(vars: GetWeeklyMetricSnapshotVariables, options?: ExecuteQueryOptions): QueryPromise<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;
export function getWeeklyMetricSnapshot(dc: DataConnect, vars: GetWeeklyMetricSnapshotVariables, options?: ExecuteQueryOptions): QueryPromise<GetWeeklyMetricSnapshotData, GetWeeklyMetricSnapshotVariables>;

interface GetScanResultsRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetScanResultsVariables): QueryRef<GetScanResultsData, GetScanResultsVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetScanResultsVariables): QueryRef<GetScanResultsData, GetScanResultsVariables>;
  operationName: string;
}
export const getScanResultsRef: GetScanResultsRef;

export function getScanResults(vars: GetScanResultsVariables, options?: ExecuteQueryOptions): QueryPromise<GetScanResultsData, GetScanResultsVariables>;
export function getScanResults(dc: DataConnect, vars: GetScanResultsVariables, options?: ExecuteQueryOptions): QueryPromise<GetScanResultsData, GetScanResultsVariables>;

interface GetChartBarsRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetChartBarsVariables): QueryRef<GetChartBarsData, GetChartBarsVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetChartBarsVariables): QueryRef<GetChartBarsData, GetChartBarsVariables>;
  operationName: string;
}
export const getChartBarsRef: GetChartBarsRef;

export function getChartBars(vars: GetChartBarsVariables, options?: ExecuteQueryOptions): QueryPromise<GetChartBarsData, GetChartBarsVariables>;
export function getChartBars(dc: DataConnect, vars: GetChartBarsVariables, options?: ExecuteQueryOptions): QueryPromise<GetChartBarsData, GetChartBarsVariables>;

interface GetCompanyProfileRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetCompanyProfileVariables): QueryRef<GetCompanyProfileData, GetCompanyProfileVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetCompanyProfileVariables): QueryRef<GetCompanyProfileData, GetCompanyProfileVariables>;
  operationName: string;
}
export const getCompanyProfileRef: GetCompanyProfileRef;

export function getCompanyProfile(vars: GetCompanyProfileVariables, options?: ExecuteQueryOptions): QueryPromise<GetCompanyProfileData, GetCompanyProfileVariables>;
export function getCompanyProfile(dc: DataConnect, vars: GetCompanyProfileVariables, options?: ExecuteQueryOptions): QueryPromise<GetCompanyProfileData, GetCompanyProfileVariables>;

interface GetMyAppraisalsRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMyAppraisalsVariables): QueryRef<GetMyAppraisalsData, GetMyAppraisalsVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetMyAppraisalsVariables): QueryRef<GetMyAppraisalsData, GetMyAppraisalsVariables>;
  operationName: string;
}
export const getMyAppraisalsRef: GetMyAppraisalsRef;

export function getMyAppraisals(vars: GetMyAppraisalsVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyAppraisalsData, GetMyAppraisalsVariables>;
export function getMyAppraisals(dc: DataConnect, vars: GetMyAppraisalsVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyAppraisalsData, GetMyAppraisalsVariables>;

interface GetMySavedPicksRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMySavedPicksVariables): QueryRef<GetMySavedPicksData, GetMySavedPicksVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetMySavedPicksVariables): QueryRef<GetMySavedPicksData, GetMySavedPicksVariables>;
  operationName: string;
}
export const getMySavedPicksRef: GetMySavedPicksRef;

export function getMySavedPicks(vars: GetMySavedPicksVariables, options?: ExecuteQueryOptions): QueryPromise<GetMySavedPicksData, GetMySavedPicksVariables>;
export function getMySavedPicks(dc: DataConnect, vars: GetMySavedPicksVariables, options?: ExecuteQueryOptions): QueryPromise<GetMySavedPicksData, GetMySavedPicksVariables>;

interface GetMyRatingHistoryRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMyRatingHistoryVariables): QueryRef<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetMyRatingHistoryVariables): QueryRef<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;
  operationName: string;
}
export const getMyRatingHistoryRef: GetMyRatingHistoryRef;

export function getMyRatingHistory(vars: GetMyRatingHistoryVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;
export function getMyRatingHistory(dc: DataConnect, vars: GetMyRatingHistoryVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyRatingHistoryData, GetMyRatingHistoryVariables>;

interface GetMyNotesRef {
  /* Allow users to create refs without passing in DataConnect */
  (vars: GetMyNotesVariables): QueryRef<GetMyNotesData, GetMyNotesVariables>;
  /* Allow users to pass in custom DataConnect instances */
  (dc: DataConnect, vars: GetMyNotesVariables): QueryRef<GetMyNotesData, GetMyNotesVariables>;
  operationName: string;
}
export const getMyNotesRef: GetMyNotesRef;

export function getMyNotes(vars: GetMyNotesVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyNotesData, GetMyNotesVariables>;
export function getMyNotes(dc: DataConnect, vars: GetMyNotesVariables, options?: ExecuteQueryOptions): QueryPromise<GetMyNotesData, GetMyNotesVariables>;

