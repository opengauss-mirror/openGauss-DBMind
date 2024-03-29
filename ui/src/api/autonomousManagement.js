import { get, post } from './request';
// RiskAnalysis
export const getSearchMetricInterface = () => {
  return get('/list/metric');
};
export const getForecastInterface = (data) => {
  return get(`/toolkit/risk-analysis/${data.metric_name}?instance=${data.instance_name}&warning_hours=${data.warning_hours}&upper=${data.upper}&lower=${data.lower}&labels=${data.labels}`);
};
// ----Alarms
export const getHistoryAlarmsInterface = (data) => {
  return get('/alarm/history', data);
};
export const getHistoryAlarmsInterfaceCount = (data) => {
  return get('/alarm/history_count', data);
};
export const getRegularInspections = (data) => {
  return get('/summary/correlation_result', data);
};
export const getTopQueryInterface = (data) => {
  return get('/query/top', data);
};
//---- Active SQL Statements
export const getKillSlowQueryInterface = (data) => {
  return get('/query/slow/killed', data);
};
export const getKillSlowQueryInterfaceCount = (data) => {
  return get('/query/slow/killed_count', data);
};
export const getActiveSQLDataInterface = () => {
  return get('/query/active');
};
export const getKillData = (data) => {
  return get(`/app/kill/${data}`);
};
export const getDetailsData = (data) => {
  return get(`/app/query/wait_status`,data);
};
export const getLockingQueryInterface = () => {
  return get('/query/locking');
};
export const getCommonMetric = (data) => {
  return get(`/latest-sequence/${data.label}?latest_minutes=${data.minutes}&fetch_all=${data.fetch}&instance=${data.instance}`);
};
export const getStorageData = (data) => {
  return get(`/latest-sequence/${data.label}?latest_minutes=${data.minutes}&fetch_all=${data.fetch}&instance=${data.instance}&regrex=True&regrex_labels=device=/.*`);
};
export const getMemoryData = (data) => {
  return get(`/latest-sequence/${data.label}?latest_minutes=${data.minutes}&fetch_all=${data.fetch}&instance=${data.instance}&regrex=True`);
};
export const getServiceCapabilityData = (data) => {
  return get(`/latest-sequence/${data.label}?latest_minutes=${data.minutes}&fetch_all=${data.fetch}&instance=${data.instance}&regrex=False`);
};
export const getDBMemoryData = (data) => {
  return get(`/latest-sequence/pg_total_memory_detail_mbytes?latest_minutes=${data.minutes}&labels=type=${data.label}&fetch_all=False&instance=${data.instance}&regrex=False`);
};
export const getSelfhealingface = (data) => {
  return post(`/anomaly_detection/detectors/${data}/view`);
};
export const getSelfhealingSetting = () => {
  return get(`/anomaly_detection/defaults`);
};
export const getSelfhealingSubmit = (data) => {
  return post(`/anomaly_detection/detectors/${data.name}/addition`, data.detectors_info);
};
export const getSelfhealingDelete = (data) => {
  return post(`/anomaly_detection/detectors/${data}/deletion`);
};
export const getSelfhealingPause = (data) => {
  return post(`/anomaly_detection/detectors/${data}/pause`);
};
export const getSelfhealingResumption = (data) => {
  return post(`/anomaly_detection/detectors/${data}/resumption`);
};
export const getTreeDetails = (data) => {
  return get(`/app/query/wait_tree?sessionid=${data}`);
};
export const getExecutionPlan = (data) => {
  return post(`/app/query/get-plan`,data);
};