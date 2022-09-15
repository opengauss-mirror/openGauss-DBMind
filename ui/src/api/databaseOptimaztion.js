import { get } from './request';

// ----Index Tuning
export const getIndexTuningInterface = () => {
  return get('/summary/index_advisor');
};

// ---Database Tuning
export const getDatabaseTuningInterface = () => {
  return get('/summary/knob_tuning');
};

// ---Slow Query Analysis
export const getSlowQueryAnalysisInterface = () => {
  return get('/summary/slow_query');
};