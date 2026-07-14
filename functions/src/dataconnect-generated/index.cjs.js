const { queryRef, executeQuery, validateArgsWithOptions, mutationRef, executeMutation, validateArgs } = require('firebase/data-connect');

const connectorConfig = {
  connector: 'admin',
  service: 'moneymaker',
  location: 'australia-southeast1'
};
exports.connectorConfig = connectorConfig;

const upsertMyAppraisalRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return mutationRef(dcInstance, 'UpsertMyAppraisal', inputVars);
}
upsertMyAppraisalRef.operationName = 'UpsertMyAppraisal';
exports.upsertMyAppraisalRef = upsertMyAppraisalRef;

exports.upsertMyAppraisal = function upsertMyAppraisal(dcOrVars, vars) {
  const { dc: dcInstance, vars: inputVars } = validateArgs(connectorConfig, dcOrVars, vars, true);
  return executeMutation(upsertMyAppraisalRef(dcInstance, inputVars));
}
;

const clearMyAppraisalRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return mutationRef(dcInstance, 'ClearMyAppraisal', inputVars);
}
clearMyAppraisalRef.operationName = 'ClearMyAppraisal';
exports.clearMyAppraisalRef = clearMyAppraisalRef;

exports.clearMyAppraisal = function clearMyAppraisal(dcOrVars, vars) {
  const { dc: dcInstance, vars: inputVars } = validateArgs(connectorConfig, dcOrVars, vars, true);
  return executeMutation(clearMyAppraisalRef(dcInstance, inputVars));
}
;

const upsertMyNoteRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return mutationRef(dcInstance, 'UpsertMyNote', inputVars);
}
upsertMyNoteRef.operationName = 'UpsertMyNote';
exports.upsertMyNoteRef = upsertMyNoteRef;

exports.upsertMyNote = function upsertMyNote(dcOrVars, vars) {
  const { dc: dcInstance, vars: inputVars } = validateArgs(connectorConfig, dcOrVars, vars, true);
  return executeMutation(upsertMyNoteRef(dcInstance, inputVars));
}
;

const listRecentScansRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'ListRecentScans', inputVars);
}
listRecentScansRef.operationName = 'ListRecentScans';
exports.listRecentScansRef = listRecentScansRef;

exports.listRecentScans = function listRecentScans(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(listRecentScansRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getMarketStatusRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetMarketStatus', inputVars);
}
getMarketStatusRef.operationName = 'GetMarketStatus';
exports.getMarketStatusRef = getMarketStatusRef;

exports.getMarketStatus = function getMarketStatus(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getMarketStatusRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getLatestRefreshJobsRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetLatestRefreshJobs', inputVars);
}
getLatestRefreshJobsRef.operationName = 'GetLatestRefreshJobs';
exports.getLatestRefreshJobsRef = getLatestRefreshJobsRef;

exports.getLatestRefreshJobs = function getLatestRefreshJobs(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getLatestRefreshJobsRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getWeeklyMetricSnapshotRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetWeeklyMetricSnapshot', inputVars);
}
getWeeklyMetricSnapshotRef.operationName = 'GetWeeklyMetricSnapshot';
exports.getWeeklyMetricSnapshotRef = getWeeklyMetricSnapshotRef;

exports.getWeeklyMetricSnapshot = function getWeeklyMetricSnapshot(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getWeeklyMetricSnapshotRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getScanResultsRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetScanResults', inputVars);
}
getScanResultsRef.operationName = 'GetScanResults';
exports.getScanResultsRef = getScanResultsRef;

exports.getScanResults = function getScanResults(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getScanResultsRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getChartBarsRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetChartBars', inputVars);
}
getChartBarsRef.operationName = 'GetChartBars';
exports.getChartBarsRef = getChartBarsRef;

exports.getChartBars = function getChartBars(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getChartBarsRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getCompanyProfileRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetCompanyProfile', inputVars);
}
getCompanyProfileRef.operationName = 'GetCompanyProfile';
exports.getCompanyProfileRef = getCompanyProfileRef;

exports.getCompanyProfile = function getCompanyProfile(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getCompanyProfileRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getMyAppraisalsRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetMyAppraisals', inputVars);
}
getMyAppraisalsRef.operationName = 'GetMyAppraisals';
exports.getMyAppraisalsRef = getMyAppraisalsRef;

exports.getMyAppraisals = function getMyAppraisals(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getMyAppraisalsRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getMySavedPicksRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetMySavedPicks', inputVars);
}
getMySavedPicksRef.operationName = 'GetMySavedPicks';
exports.getMySavedPicksRef = getMySavedPicksRef;

exports.getMySavedPicks = function getMySavedPicks(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getMySavedPicksRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getMyRatingHistoryRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetMyRatingHistory', inputVars);
}
getMyRatingHistoryRef.operationName = 'GetMyRatingHistory';
exports.getMyRatingHistoryRef = getMyRatingHistoryRef;

exports.getMyRatingHistory = function getMyRatingHistory(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getMyRatingHistoryRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;

const getMyNotesRef = (dcOrVars, vars) => {
  const { dc: dcInstance, vars: inputVars} = validateArgs(connectorConfig, dcOrVars, vars, true);
  dcInstance._useGeneratedSdk();
  return queryRef(dcInstance, 'GetMyNotes', inputVars);
}
getMyNotesRef.operationName = 'GetMyNotes';
exports.getMyNotesRef = getMyNotesRef;

exports.getMyNotes = function getMyNotes(dcOrVars, varsOrOptions, options) {
  
  const { dc: dcInstance, vars: inputVars, options: inputOpts } = validateArgsWithOptions(connectorConfig, dcOrVars, varsOrOptions, options, true, true);
  return executeQuery(getMyNotesRef(dcInstance, inputVars), inputOpts && { fetchPolicy: inputOpts.fetchPolicy });
}
;
