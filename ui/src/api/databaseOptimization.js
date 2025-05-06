import { get } from './request';

// ----Index Tuning
export const getIndexTuningInterface = (data) => {
  return get('/summary/index_advisor', data);
};
export const getPositiveSql = (data) => {
  return get('/summary/get_positive_sql', data);
};
export const getPositiveSqlCount = () => {
  return get('/summary/positive_sql_count');
};
export const getExistingIndexes = (data) => {
  return get('/summary/get_existing_indexes', data);
};
export const getExistingIndexesCount = () => {
  return get('/summary/existing_indexes_count');
};

// ---Database Tuning
export const getDatabaseTuningInterface = (data) => {
  return get('/summary/knob_tuning', data);
};
export const getKnobRecommendationSnapshot = (data) => {
  return get('/summary/get_knob_recommendation_snapshot', data);
};
export const getKnobRecommendationSnapshotCount = () => {
  return get('/summary/knob_recommendation_snapshot_count');
};
export const getKnobRecommendationWarnings = (data) => {
  return get('/summary/get_knob_recommendation_warnings', data);
};
export const getKnobRecommendationWarningsCount = () => {
  return get('/summary/knob_recommendation_warnings_count');
};
export const getKnobRecommendation = (data) => {
  return get('summary/get_knob_recommendation', data);
};
export const getKnobRecommendationCount = () => {
  return get('/summary/knob_recommendation_count');
};

// ---Slow Query Analysis
export const getSlowQueryAnalysisInterface = (data) => {
  return get('/summary/slow_query', data);
};
export const getSlowQueryRecent = (data) => {
  return get('/query/slow/recent', data);
};
export const getSlowQueryRecentCount = () => {
  return get('/query/slow/recent_count');
};
export const getIntelligentSqlCondition = (data) => {
  return get(`/workloads/collect?data_source=${data.data_source}&databases=${data.databases}&start_time=${data.start_time}&end_time=${data.end_time}&db_users=${data.db_users}&sql_types=${data.sql_types}&duration=${data.duration}`);
};