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

import {
  post,
  get
} from './request';

export const getItemListInterface = () => {
  return get('/summary/database-list');
};
export const getUserItemListInterface = () => {
  return get('/list/users');
};
export const getListIndexAdvisorInterface = (data) => {
  return post(`/app/index-recommendation?database=${data.database}&instance=${data.instance}&max_index_num=${data.max_index_num}&max_index_storage=${data.max_index_storage}&current=${data.current}&pagesize=${data.pagesize}`, data.textareaVal);
};
export const getQueryTuningInterface = (data) => {
  return post(`/toolkit/advise/query?database=${data.database}&instance=${data.instance}&sql=${data.sql}&use_rewrite=${data.use_rewrite}&use_hinter=${data.use_hinter}&use_materialized=${data.use_materialized}`);
};
export const getIntelligentSqlAnalysisInterface = (data) => {
  return get('/app/slow-sql-rca', data)
};
