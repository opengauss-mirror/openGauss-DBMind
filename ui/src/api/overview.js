import { get, post } from './request';

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

export const getInterface = () => {
  return get('/overview');
};

export const getResponseTime = (data) => {
  return get(`/latest-sequence/${data.label}?latest_minutes=3&instance=${data.instance}`);
};

export const getConnection = (data) => {
  return get(`/latest-sequence/${data.label}?&latest_minutes=3&instance=${data.instance}`);
};

export const getProxy = () => {
  return get('/agent/status');
};

export const getDistribution = (data) => {
  return get(`/latest-sequence/${data.label}?&latest_minutes=0&instance=${data.instance}`);
};

export const getTransaction = (data) => {
  return get(`/latest-sequence/${data.label}?latest_minutes=0&fetch_all=True&instance=${data.instance}`);
};

export const getDatabaseSize = (data) => {
  return get(`/latest-sequence/${data.label}?latest_minutes=0&fetch_all=True&instance=${data.instance}`);
};

export const getCollectionTable = () => {
  return get('/collection/status');
};
export const getNode = () => {
  return get('/instance/status');
};

export const getDataDisk = (data) => {
  return get(`/data-directory/status?latest_minutes=3&instance=${data}`);
};

export const getTimedTaskStatus = () => {
  return get('/app/timed-task/status');
};

export const getStopTimed = (data) => {
  return post(`/app/stop_timed_task?funcname=${data}`)
};

export const getStartTimed = (data) => {
  return post(`/app/start_timed_task?funcname=${data}`)
};

export const getResetInterval = (data) => {
  return post(`/app/reset_interval?funcname=${data.funcname}&seconds=${data.seconds}`)
};