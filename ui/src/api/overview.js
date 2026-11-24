/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-DBMind is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

import { get, post } from './request';

export const getInterface = () => {
  return get('/status/overview');
};

export const getResponseTime = (data) => {
  return get(`/summary/metrics/${data.label}?latest_minutes=3&instance=${data.instance}`);
};

export const getConnection = (data) => {
  return get(`/summary/metrics/${data.label}?&latest_minutes=3&instance=${data.instance}`);
};

export const getProxy = () => {
  return get('/agent/status');
};

export const getDistribution = (data) => {
  return get(`/summary/metrics/${data.label}?&latest_minutes=0&instance=${data.instance}`);
};

export const getTransaction = (data) => {
  return get(`/summary/metrics/${data.label}?latest_minutes=0&fetch_all=True&instance=${data.instance}`);
};

export const getDatabaseSize = (data) => {
  return get(`/summary/metrics/${data.label}?latest_minutes=0&fetch_all=True&instance=${data.instance}`);
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
  return post(`/app/reset_qinterval?funcname=${data.funcname}&seconds=${data.seconds}`)
};