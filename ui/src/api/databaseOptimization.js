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
  return get('/summary/knob-recommendation/snapshots', data);
};
export const getKnobRecommendationSnapshotCount = () => {
  return get('/summary/knob-recommendation/snapshots/count');
};
export const getKnobRecommendationWarnings = (data) => {
  return get('/summary/knob-recommendation/warnings', data);
};
export const getKnobRecommendationWarningsCount = () => {
  return get('/summary/knob-recommendation/warnings/count');
};
export const getKnobRecommendation = (data) => {
  return get('/summary/knob-recommendation/details', data);
};
export const getKnobRecommendationCount = () => {
  return get('/summary/knob-recommendation/details/count');
};

// ---Slow Query Analysis
export const getSlowQueryAnalysisInterface = (data) => {
  return get('/summary/sql/slow', data);
};
export const getSlowQueryRecent = (data) => {
  // 获取最近慢 SQL 列表数据
  return get('/summary/sql/slow/latest', data);
};
export const getSlowQueryRecentCount = () => {
  // 获取最近慢 SQL 列表总数
  return get('/summary/sql/slow/latest/count');
};
export const getIntelligentSqlCondition = (data) => {
  return get(`/app/workload-collection`,data);
};
export const getLabelData = (data) => {
  return get(`/sequence/${data.label}?from_timestamp=${data.from_timestamp}&to_timestamp=${data.to_timestamp}&fetch_all=${data.fetch}&instance=${data.instance}`);
};
