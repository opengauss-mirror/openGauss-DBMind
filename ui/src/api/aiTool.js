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
