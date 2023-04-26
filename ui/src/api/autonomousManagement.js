import { get, post } from './request';

// ----Workload Forecasting
export const getWorkloadForecastInterface = (data) => {
  return get(`/workload_forecasting/sequence/${data.name}`, data.time);
};
export const getForecastChartInterface = (data) => {
  return get(`/workload_forecasting/sequence/forecast/${data.name}`, data.para);
}
// search metric
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
export const getFutureAlarmsInterface = (data) => {
  return get('/alarm/future', data);
};
export const getFutureAlarmsInterfaceCount = (data) => {
  return get('/alarm/future_count', data);
};
export const getSelfHealingRecordsInterface = (data) => {
  return get('/alarm/healing', data);
};
export const getSelfHealingRecordsInterfaceCount = (data) => {
  return get('/alarm/healing_count', data);
};
// ----Slow/Top Query
export const getRecentSlowQueryInterface = (data) => {
  return get('/query/slow/recent', data);
};
export const getRecentSlowQueryInterfaceCount = (data) => {
  return get('/query/slow/recent_count', data);
};
export const getTopQueryInterface = (data) => {
  return get('/query/top', data);
};
export const getIntelligentSqlAnalysisInterface = (data) => {
  return post(`/toolkit/predict/query?database=${data.database}&sql=${data.sql}`)
};
export const getItemsListInterface = () => {
  return get('/list/database');
};
export const getKillSlowQueryInterface = (data) => {
  return get('/query/slow/killed', data);
};
export const getKillSlowQueryInterfaceCount = (data) => {
  return get('/query/slow/killed_count', data);
};
//---- Active SQL Statements
export const getActiveSQLDataInterface = () => {
  return get('/query/active');
};
export const getLockingQueryInterface = () => {
  return get('/query/locking');
};

// ----Log Information
export const getLogSummaryInterface = () => {
  return get('/summary/log')
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