import { post, get } from './request';

export const loginInterface = data => {
  return post('/token', data);
};
export const getAgentListInterface = () => {
  return get('/list/agent');
};