import { get,post } from './request';

export const getSettingListInterface = () => {
  return get('/setting/list');
};
export const putSettingDetailInterface = (data) => {
  return get('/setting/set', data);
};
export const updateSetting = (data) => {
  return post('/setting/update_dynamic_config', data);
};
export const getSettingDefaults = (data) => {
  return get('/default_values', data);
};
export const getSettingCurrentValue = (data) => {
  return get('/values', data);
};