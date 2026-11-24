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

import { get } from "./request";

export const getTaskSubmit = (data) => {
  return get(
    `/summary/real-time-inspection/exec?inspection_type=${data.inspectionType}&start_time=${data.startTime}&end_time=${data.endTime}&select_metrics=${data.selectMetrics}`
  );
};
export const getRealtimeInspectionList = () => {
  return get(`/summary/real-time-inspection/list`);
};
export const getTaskReport = (data) => {
  return get(`/summary/real-time-inspection/report?spec_id=${data}`);
};
export const deleteTask = (data) => {
  return get(`/summary/real-time-inspection/delete?spec_id=${data}`);
};
