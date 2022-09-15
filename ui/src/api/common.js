import { qspost } from './request';

export const loginInterface = data => {
  return qspost('/token', data
  );
};