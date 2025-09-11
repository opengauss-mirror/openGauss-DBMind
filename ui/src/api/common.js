import { get, postNoRetry } from './request';

export const loginInterface = data => {
  return postNoRetry('/token', data);  // 使用无重试的 POST 方法
};
export const getAgentListInterface = () => {
  return get('/agents');
};