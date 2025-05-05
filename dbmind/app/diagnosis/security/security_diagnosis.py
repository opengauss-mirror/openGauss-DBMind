# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
"""
util for checking security metrics
"""
import datetime
import logging
import math
import os
import time
from itertools import compress

from dbmind import global_vars
from dbmind.app.diagnosis.security.security_scenarios import load_scenarios_yaml_definitions, \
    get_list_of_required_metrics
from dbmind.app.diagnosis.security.security_scenarios_alarms import add_scenarios_alarms
from dbmind.common.algorithm.anomaly_detection import ForecastingAnomalyDetector
from dbmind.common.types import Sequence
from dbmind.common.utils.checking import prepare_ip, split_ip_port
from dbmind.constants import SCENARIO_YAML_FILE_NAME, PORT_SUFFIX
from dbmind.app.diagnosis.security.security_metrics_settings import get_security_metrics_settings
from dbmind.metadatabase.dao.forecast_calibration_info import ModelCalibrationData
from dbmind.metadatabase.dao.security_anomalies import get_calibration_model_age_in_minutes, add_abnormal_value
from dbmind.service import dai
from dbmind.service.dai import calculate_default_step, is_sequence_valid

SECURITY_METRICS_DEBUG_OUTPUT_FILE = "security_metrics"  # folder name to store debug csv files

#  Load self secured configuration:
calibration_training_in_minutes = global_vars.dynamic_configs.get_int_or_float('security_metrics',
                                                                               'calibration_training_in_minute',
                                                                               fallback=5880)
calibration_forecasting_in_minutes = global_vars.dynamic_configs.get_int_or_float('security_metrics',
                                                                                  'calibration_forecasting_in_minutes',
                                                                                  fallback=840)
re_calibrate_period = global_vars.dynamic_configs.get_int_or_float('security_metrics',
                                                                   're_calibrate_period',
                                                                   fallback=5880)
detection_training_in_minutes = global_vars.dynamic_configs.get_int_or_float('security_metrics',
                                                                             'detection_training_in_minutes',
                                                                             fallback=840)
detection_forecasting_in_minutes = global_vars.dynamic_configs.get_int_or_float('security_metrics',
                                                                                'detection_forecasting_in_minutes',
                                                                                fallback=30)
model_min_forecast_length_in_minute = global_vars.dynamic_configs.get_int_or_float(
    'security_metrics', 'model_min_forecast_length_in_minute', fallback=15)


last_security_scenarios_file_ts = 0
security_scenarios_list = []


def calibrate_security_metric(metric_name, host, calibration_training, calibration_forecasting,
                              metric_sequence, model_min_forecast_length):
    """
    calibrate single metric
    @param metric_name: metric name
    @param host: str
    @param calibration_training: int, minutes
    @param calibration_forecasting: int, minutes
    @param metric_sequence: sequence
    @param model_min_forecast_length: int the number of mite to forecast chunks with, minutes
    @return: tuple metric_name, host, ForecastingAnomalyDetector calibration_model that has the data to save
    """
    if metric_sequence.name is None:
        logging.error("The sequence can not be calibrated because of lacking metric name. "
                      "The expected metric name is: %s", metric_name)
        return None
    minutes_back_to_calibrate = calibration_training + calibration_forecasting
    logging.info("Starting calibration of %s on host %s minutes_back_to_calibrate: %s",
                 metric_name, host, minutes_back_to_calibrate)
    if metric_sequence.timestamps[-1] - metric_sequence.timestamps[0] + 60 < minutes_back_to_calibrate * 60:
        logging.error("Not enough data to calibrate metric: %s %s - got %s expecting %s",
                      metric_name, host, metric_sequence.timestamps[-1] - metric_sequence.timestamps[0],
                      minutes_back_to_calibrate * 60)
        return None
    logging.info("Starting calibration of %s data length: %s calibration_forecasting_in_minutes: %s",
                 metric_name, len(metric_sequence), calibration_forecasting)
    forecast_length = calculate_forecast_length(metric_sequence, model_min_forecast_length)
    z_score_conf_factor = get_security_metrics_settings('z_score_conf_factor')
    ratio_conf_factor = get_security_metrics_settings('ratio_conf_factor')
    calibration_model = ForecastingAnomalyDetector(side="positive",
                                                   detection_area_in_minutes=calibration_forecasting,
                                                   min_forecast_length=forecast_length,
                                                   z_score_conf_factor=z_score_conf_factor,
                                                   ratio_conf_factor=ratio_conf_factor)
    try:
        calibration_model.fit_predict(metric_sequence)
    except Exception as e:
        logging.error("Failed to fit or predict metric: %s", metric_name)
        logging.debug(e)
        return None

    logging.info("Calibration of %s on host %s is done and found get_std_errors: %s "
                 "get_avg_errors: %s get_max_ratio: %s",
                 metric_name, host, calibration_model.get_std_errors(),
                 calibration_model.get_avg_errors(),
                 calibration_model.get_max_ratio())
    return metric_name, host, calibration_model, metric_sequence.step


def calculate_forecast_length(metric_sequence: Sequence, model_min_forecast_length: int) -> int:
    """
    Calculates the forecast length for anomaly detection of a sequence
    @param metric_sequence: metric name
    @param model_min_forecast_length: the number of minutes to forecast each chunk with, minutes
    @return: int with the number of data points to forecast
    """
    # step is in milliseconds:
    min_forecast_length = model_min_forecast_length * (60 / (metric_sequence.step / 1000))
    min_forecast_length = math.ceil(min_forecast_length)
    return min_forecast_length


def detect_security_metric(metric_name, host, data, calibration_data, detection_forecasting, model_min_forecast_length):
    """
    Detect anomalies in the last period of security metric and save the abnormal values in the database
    @param metric_name: string
    @param host: string
    @param data: sequence the metric data
    @param calibration_data: ModelCalibrationData with the calibration data for the given metric
    @param detection_forecasting: int, minutes
    @param model_min_forecast_length: int the number of mite to forecast chunks with, minutes
    @return: list of tuples with anomalies (metric name , anomaly value, anomaly time stamp, host)
    """
    try:
        logging.info("Starting detect_security_metric metric_name: %s", metric_name)
        if calibration_data is None:
            logging.error("detect_security_metric for %s failed retrieving metric params.", metric_name)
            return []
        data_sequence = data

        data_length = len(data_sequence)
        forecast_length = calculate_forecast_length(data_sequence, model_min_forecast_length)
        logging.info("Using calibration_data: %s for %s detection_forecasting_in_minutes of : %s "
                     "forecast_length of %s",
                     calibration_data, metric_name, detection_forecasting, forecast_length)
        z_score_conf_factor = get_security_metrics_settings('z_score_conf_factor')
        ratio_conf_factor = get_security_metrics_settings('ratio_conf_factor')
        model = ForecastingAnomalyDetector(side="positive",
                                           detection_area_in_minutes=detection_forecasting,
                                           min_forecast_length=forecast_length,
                                           z_score_conf_factor=z_score_conf_factor,
                                           ratio_conf_factor=ratio_conf_factor,
                                           **calibration_data)
        anomalies = model.fit_predict(data_sequence)
        skip_anomalies_under_low_bound(anomalies, data_sequence)
        anomalies_1_0 = [1 if i else 0 for i in anomalies]
        anomalies_count = sum(a == 1 for a in anomalies_1_0)
        step_in_seconds = int(data_sequence.step / 1000)
        if step_in_seconds == 0:
            logging.error("Error sequence.step is zero in detect_security_metric for metric: %s", metric_name)
            return []
        if anomalies_count > 0:
            logging.info("Found %s anomalies for %s on host %s.", anomalies_count, metric_name, host)
        else:
            logging.info("No anomalies found %s", metric_name)

        return _get_metric_anomalies(anomalies, data_length, data_sequence, metric_name, host)
    except Exception as error:
        logging.error("detect_security_metric failed metric_name: %s on host %s", metric_name, host)
        logging.exception(error)
        return []


def skip_anomalies_under_low_bound(anomalies, data_sequence):
    """
    do not trigger anomalies below configured number per metric
    @param anomalies: list of anomalies
    @param data_sequence: the original sequence
    """
    try:
        min_anomaly = get_security_metrics_settings(f"{data_sequence.name}_lower_bound")
    except KeyError:
        min_anomaly = None  # if the value does not exist we can ignore it

    if min_anomaly is not None:
        indices = _get_true_indices(anomalies)
        detection_values = data_sequence.values[-len(anomalies):]
        for index in indices:
            if detection_values[index] < min_anomaly:
                anomalies[index] = False


def _get_metric_anomalies(anomalies, data_length, data_sequence, metric_name, host):
    """
    Generates list of tuples with anomalies values
    @param anomalies: list of anomalies
    @param data_length: int the data length
    @param data_sequence: Sequence
    @param metric_name: string
    @param host: string
    @return: list of tuples with anomalies (metric name, host, anomaly vale, anomaly time stamp)
    """
    result = list()
    for index, value in enumerate(anomalies):
        if value:
            data_index = data_length - len(anomalies) + index
            anomaly_value = data_sequence.values[data_index]
            anomaly_time = data_sequence.timestamps[data_index]
            logging.info("Found abnormal value in %s, anomaly_value: %s, anomaly_time: %s",
                         metric_name, anomaly_value, anomaly_time)
            result.append((metric_name, host, anomaly_value, anomaly_time))
    return result


def current_ts():
    """
    @return: int with current timestamp in milliseconds
    """
    return round(time.time() * 1000)


def _current_date_human():
    """
    @return: str the date in human readable format yyyy-mm-dd hh:mm:ss
    """
    return f'{datetime.datetime.now():%Y-%m-%d_%H:%M:%S}'


def _get_true_indices(t: list) -> list:
    return list(compress(range(len(t)), t))


def load_security_scenarios_list():
    """
    load security scenarios from local yaml file
    """
    global security_scenarios_list
    global last_security_scenarios_file_ts
    try:
        scenario_file_path = os.path.join(global_vars.confpath, SCENARIO_YAML_FILE_NAME)
        security_scenarios_file_ts = os.path.getmtime(scenario_file_path)
        # if scenarios were not loaded yet, or the file changed since last time, reload it
        if len(security_scenarios_list) == 0 or last_security_scenarios_file_ts < security_scenarios_file_ts:
            default_security_metrics_scenario_low_alert = global_vars.dynamic_configs.get_int_or_float(
                'security_metrics', 'scenario_low_alert', fallback=0.2)
            default_medium_threshold = global_vars.dynamic_configs.get_int_or_float('security_metrics',
                                                                                    'scenario_medium_alert',
                                                                                    fallback=0.6)
            default_high_threshold = global_vars.dynamic_configs.get_int_or_float('security_metrics',
                                                                                  'scenario_high_alert',
                                                                                  fallback=0.8)
            security_scenarios_list = load_scenarios_yaml_definitions(scenario_file_path,
                                                                      default_security_metrics_scenario_low_alert,
                                                                      default_medium_threshold, default_high_threshold)
            last_security_scenarios_file_ts = security_scenarios_file_ts
            logging.info("Security scenarios loaded: %s",
                         [str(scenario) for scenario in security_scenarios_list])
        else:
            logging.info("Security scenarios loaded from history data: %s",
                         [str(scenario) for scenario in security_scenarios_list])
    except Exception as error:
        logging.error("load_security_scenarios_list failed due to exception")
        logging.exception(error)


def get_metrics_that_need_calibration(minutes_back):
    """
    get the list of metrics that need calibration
    @param minutes_back: int minutes back to fetch data from the TS database
    @return: list of tuples (host, metric_name, sequence) for metrics that need calibration, empty list means
    no calibration is needed
    """
    load_security_scenarios_list()
    security_metrics_list = get_list_of_required_metrics(security_scenarios_list)
    logging.info("security_metrics_list is %s", security_metrics_list)
    metric_hosts_tuple = []
    for metric_name in security_metrics_list:
        # do not want to ask for too many data points from the server:
        step = calculate_default_step(minutes_back)
        logging.info("Calibrating %s by step %s", metric_name, step)
        agent_list = global_vars.agent_proxy.agent_get_all().values()
        all_agents = [agent for agents in agent_list for agent in agents]
        for agent in all_agents:
            model_age = get_calibration_model_age_in_minutes(metric_name, agent)
            logging.info("agent: %s, model_age: %s", agent, model_age)
            if model_age == 0 or model_age > re_calibrate_period:
                logging.info("Calibration is needed for metric: %s on host: %s "
                             "since model age is %s, minutes_back: %s, step: %s",
                             metric_name, agent, model_age, minutes_back, step)
                if metric_name.startswith('opengauss_log_'):
                    host_like = prepare_ip(split_ip_port(agent)[0]) + PORT_SUFFIX
                    sequences = dai.get_latest_metric_sequence(
                        metric_name,
                        minutes=minutes_back,
                        step=step
                    ).from_server_like(host_like).fetchall()
                else:
                    sequences = dai.get_latest_metric_sequence(
                        metric_name,
                        minutes=minutes_back,
                        step=step
                    ).from_server(agent).fetchall()

                for seq in sequences:
                    metric_hosts_tuple.append((agent, metric_name, seq))

    return metric_hosts_tuple


def find_anomalies_in_security_metrics():
    """
    Find anomalies in security metrics and write it into the metadata database
    """
    scenario_from_ts = (current_ts()) - detection_forecasting_in_minutes * 60 * 1000
    scenario_to_ts = current_ts()
    minutes_back = detection_training_in_minutes + detection_forecasting_in_minutes
    metrics_information = list()
    calibration_data_fetcher = ModelCalibrationData()
    agent_list = global_vars.agent_proxy.agent_get_all().values()
    all_agents = [agent for agents in agent_list for agent in agents]
    security_metrics_list = get_list_of_required_metrics(security_scenarios_list)
    for host in all_agents:
        # get the data from the TS for each metric
        for metric_name in security_metrics_list:
            calibration_data = calibration_data_fetcher.load_from_database(metric_name, host)
            if calibration_data is None:
                logging.info("skip finding anomalies - no calibration data for %s on host: %s ...",
                             metric_name, host)
                continue

            if metric_name.startswith('opengauss_log_'):
                host_like = prepare_ip(split_ip_port(host)[0]) + PORT_SUFFIX
                sequence = dai.get_latest_metric_sequence(
                    metric_name,
                    minutes=minutes_back,
                    step=calibration_data["step"]
                ).from_server_like(host_like).fetchall()
            else:
                sequence = dai.get_latest_metric_sequence(
                    metric_name,
                    minutes=minutes_back,
                    step=calibration_data["step"]
                ).from_server(host).fetchall()

            del calibration_data["step"]  # no need after use
            logging.debug("calibration_data = %s", calibration_data)
            if not is_sequence_valid(sequence):
                logging.warning("failed to find sequence for %s", metric_name)
                continue

            metric_data = {
                "metric_name": metric_name,
                "host": host,
                "sequence": sequence[0],
                "calibration_data": calibration_data
            }
            metrics_information.append(metric_data)

    abnormal_values = list()
    new_anomalies = global_vars.worker.parallel_execute(
        detect_security_metric,
        ((metric_data["metric_name"], metric_data["host"], metric_data["sequence"], metric_data["calibration_data"],
          detection_forecasting_in_minutes, model_min_forecast_length_in_minute)
         for metric_data in metrics_information)) or []
    abnormal_values.append(new_anomalies)
    anomalies_count = 0
    for item in abnormal_values[0]:
        if not item:
            continue

        for anomaly in item:
            anomalies_count += 1
            add_abnormal_value(*anomaly)
    if anomalies_count > 0:
        logging.info("Found %s anomalies for security scenarios.", anomalies_count)
        for host_2_check in all_agents:
            add_scenarios_alarms(security_scenarios_list, scenario_from_ts, scenario_to_ts, host_2_check)


def calibrate_security_metrics_serial(metric_list):
    """
    get the list of metrics that need calibration
    @param metric_list: list of tuples (host, metric_name, sequence) for metrics that need calibration
    @return: None
    """
    try:
        logging.info("Calibrating security metrics for metrics: %s",
                     [(item[0], item[1]) for item in metric_list])
        for metric_tuple in metric_list:
            calibration_result = calibrate_security_metric(metric_tuple[1], metric_tuple[0],
                                                           calibration_training_in_minutes,
                                                           calibration_forecasting_in_minutes, metric_tuple[2],
                                                           model_min_forecast_length_in_minute)

            save_calibration_result_to_db(calibration_result)
    except Exception as error:
        logging.error("_calibrate_all_security_metrics failed due to exception")
        logging.exception(error)

    logging.info("Calibrating security metrics is done")


def save_calibration_result_to_db(calibration_result: tuple):
    """
    save calibration result to db
    """
    if not isinstance(calibration_result, tuple) or len(calibration_result) < 1:
        logging.error("bad result for calibration calibration_result: %s.", calibration_result)
        return
    if calibration_result[2] is None:
        logging.error("calibration failed for: %s", calibration_result[0])
        return
    calibration_data = ModelCalibrationData()
    calibration_data.save_to_database(calibration_result[0], calibration_result[1], calibration_result[2],
                                      calibration_result[3])
