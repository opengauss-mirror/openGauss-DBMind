import { qspost, get } from './request';

export const loginInterface = data => {
  return qspost('/token', data);
};
export const getAgentListInterface = () => {
  return get('/list/agent');
};