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

import { get,post } from './request';

export const getSettingListInterface = () => {
  return get('/setting/list');
};
export const putSettingDetailInterface = (data) => {
  return get('/setting/set', data);
};
export const updateSetting = (data) => {
  return post('/setting/update_dynamic_config', data);
};
export const getSettingDefaults = (data) => {
  return get('/default_values', data);
};
export const getSettingCurrentValue = (data) => {
  return get('/values', data);
};