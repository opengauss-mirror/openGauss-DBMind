import { get } from "./request";

export const getTaskSubmit = (data) => {
  return get(
    `/summary/real-time-inspection/exec?inspection_type=${data.inspectionType}&start_time=${data.startTime}&end_time=${data.endTime}&select_metrics=${data.selectMetrics}`
  );
};
export const getRealtimeInspectionList = () => {
  return get(`/summary/real-time-inspection/list`);
};
export const getTaskReport = (data) => {
  return get(`/summary/real-time-inspection/report?spec_id=${data}`);
};
export const deleteTask = (data) => {
  return get(`/summary/real-time-inspection/delete?spec_id=${data}`);
};
