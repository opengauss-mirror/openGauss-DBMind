import { get } from './request';

export const getSettingListInterface = () => {
  return get('/setting/list');
};
export const putSettingDetailInterface = (data) => {
  return get('/setting/set', data);
};