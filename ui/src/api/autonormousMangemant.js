import { get, post } from './request';

// ----Workload Forecasting
export const getWorkloadForcastInterface = (data) => {
  return get(`/workload_forecasting/sequence/${data.name}`, data.time);
};
export const getForecastChartInterface = (data) => {
  return get(`/workload_forecasting/sequence/forecast/${data.name}`, data.para);
}
// search metric
export const getSearchMetricInterface = () => {
  return get('/list/metric');
};
export const getForcastInterface = (data) => {
  return get(`/toolkit/risk_analysis?instance_name=${data.instance_name}&metric_name=${data.metric_name}&filter_name=${data.labels}&warning_hours=${data.warning_hours}&upper=${data.upper}&lower=${data.lower}`);
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
export const getLogSummaryTnterface = () => {
  return get('/summary/log')
}
