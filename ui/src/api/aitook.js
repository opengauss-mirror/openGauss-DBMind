import {
  post,
  get
} from './request';

export const getItemListInterface = () => {
  return get('/list/database');
};
export const getListIndexAdvisorInterface = (data) => {
  return post(`/toolkit/advise/index?database=${data.database}`, data.textareaVal);
};
export const getQueryTuningInterface = (data) => {
  return post(`/toolkit/advise/query?database=${data.database}&sql=${data.sql}&use_rewrite=${data.use_rewrite}&use_hinter=${data.use_hinter}&use_materialized=${data.use_materialized}`);
};
export const getIntelligentSqlAnalysisInterface = (data) => {
  return post('/toolkit/slow_sql_rca', data)
};
