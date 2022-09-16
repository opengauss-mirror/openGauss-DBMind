import { get } from './request';

export const getDetectedRiskInterface = (data) => {
  return get('/summary/security', data);
};