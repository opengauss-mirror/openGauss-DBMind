import { get } from './request';

// Transaction State
export const getTransactionStateInterface = () => {
  return get('/status/transaction');
};

// Cluster Information
export const getClusterInformationInterface = () => {
  return get('/summary/cluster');
};

// Running Status
export const getRunningStatusInterface = () => {
  return get('/status/running');
};

// Alert
export const getAlertInterface = () => {
  return get('/status/alert');
};
export const getQpsInterface = (data) => {
  return get(`/sequence/${data.name}`, data.time);
};
