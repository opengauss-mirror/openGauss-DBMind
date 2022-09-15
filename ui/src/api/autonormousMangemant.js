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

// ----Alarms
export const getHistoryAlarmsInterface = (data) => {
  return get('/alarm/history', data);
};
export const getFutureAlarmsInterface = (data) => {
  return get('/alarm/future', data);
};
export const getSelfHealingRecordsInterface = (data) => {
  return get('/alarm/healing', data);
};

// ----Slow/Top Query
export const getRecentSlowQueryInterface = (data) => {
  return get('/query/slow/recent', data);
};
export const getTopQueryInterface = () => {
  return get('/query/top');
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
