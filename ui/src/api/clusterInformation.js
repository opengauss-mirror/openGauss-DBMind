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
export const getHostListInterface = () => {
  return get('/status/host');
};

// -------Statistics
export const getClusterSummaryInterface = () => {
  return get('/summary/cluster');
};
export const getMetricStatisticInterface = () => {
  return get('/summary/metric_statistic');
};
export const getRegularInspectionsInterface = (data) => {
  return get('/summary/regular_inspections', data);
};