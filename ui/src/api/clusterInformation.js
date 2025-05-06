import { get } from './request';

// -------Instance
export const getstatusNodeInterface = () => {
  return get('/status/node');
};

// Alert
export const getAlertInterface = () => {
  return get('/status/alert');
};

// -------Host
export const getHostListInterface = (data) => {
  return get('/status/instance', data);
};

// -------Statistics
export const getClusterSummaryInterface = () => {
  return get('/summary/cluster');
};
export const getMetricStatisticInterface = (data) => {
  return get('/summary/metric_statistic', data);
};
export const getMetricStatisticInterfaceCount = (data) => {
  return get('/summary/metric_statistic_count', data);
};
export const getRegularInspectionsInterface = (data) => {
  return get('/summary/regular_inspections', data);
};