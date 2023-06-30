import { get } from './request';

export const getRegularInspectionsInterface = (data) => {
  return get('/summary/regular_inspections', data);
};