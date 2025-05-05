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
import csv
import os

import pytest

from dbmind import global_vars
from dbmind.app.diagnosis.security import security_metrics_settings, security_diagnosis
from dbmind.cmd.configs.configurators import DynamicConfig
from dbmind.common.algorithm.anomaly_detection import ForecastingAnomalyDetector
from dbmind.common.types.sequence import Sequence

STEP_IN_SECONDS = 15  # make timestamps 15 seconds jump in ms
find_result = tuple()


def calibrate_metric(file_name: str, learning_period: int, detection_length: int = 240, window_size: int = 10,
                     field_name: str = "actual") -> ForecastingAnomalyDetector:
    """
    calibrates a dataset
    @param file_name: name of the csv with the dataset - the dataset should have one field called "actual" with the data
    @param learning_period: how many records to use for learning
    @param detection_length: how many data points should be used for the detection within the learning_period
    @param window_size: the windows size of arima
    @param field_name: the name of the field with the value
    @return model used for calibration
    """
    file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", file_name)
    with open(file_path, 'r') as input_file:
        csv_dict_reader = csv.DictReader(input_file)
        values = [float(row[field_name]) for row in csv_dict_reader]
        calibration_values = values[:learning_period]

    calibration_model = ForecastingAnomalyDetector(
        side="positive",
        detection_area_in_minutes=detection_length * STEP_IN_SECONDS // 60,
        min_forecast_length=window_size
    )
    calibration_sequence = Sequence(
        timestamps=[item * STEP_IN_SECONDS * 1000 for item in range(learning_period)],
        values=calibration_values
    )
    calibration_model.fit_predict(calibration_sequence)
    return calibration_model


def _execute_model(file_name: str, max_z_score, std_errors, avg_error, max_ratio, learn_from_index: int,
                   detection_from_index: int, detection_to_index: int, window_size: int = 60) -> list:
    """
    looking for anomalies in a dataset
    @param file_name: name of the csv with the dataset - the dataset should have one field called "actual" with the data
    @param max_z_score
    @param std_errors
    @param avg_error
    @param max_ratio
    @param learn_from_index: start learning index
    @param detection_from_index: where tpo start detect for anomalies
    @param detection_to_index: stop learning index
    @param window_size: the windows size of arima
    @return list of anomalies (true false) of the detection area
    """
    file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", file_name)

    with open(file_path, 'r') as input_file:
        csv_dict_reader = csv.DictReader(input_file)
        values = [float(row["actual"]) for row in csv_dict_reader]
        calibration_values = values[learn_from_index:detection_to_index]
        step_in_seconds = 15
        timestamps = [item * step_in_seconds * 1000 for item in range(detection_to_index - learn_from_index)]
        detection_area_in_minutes = (detection_to_index - detection_from_index) / (60 / step_in_seconds)
        detection_sequence = Sequence(timestamps, values=calibration_values)
        detection_model = ForecastingAnomalyDetector(side="positive",
                                                     detection_area_in_minutes=int(detection_area_in_minutes),
                                                     min_forecast_length=window_size,
                                                     max_z_score=max_z_score,
                                                     std_errors=std_errors,
                                                     avg_errors=avg_error,
                                                     max_ratio=max_ratio)

        model_anomalies = detection_model.fit_predict(detection_sequence)
        return model_anomalies


def test_calibration_1():
    model = calibrate_metric("calibration_1.csv", 500)
    anomalies = _execute_model("calibration_1.csv", model.get_max_ratio(), model.get_std_errors(),
                               model.get_avg_errors(), model.get_max_ratio(), 0, 600, 650)
    expected_anomalies = ([False] * 27 + [True] * 11 + [False] * 10,
                          [False] * 26 + [True] * 12 + [False] * 10)
    assert anomalies in expected_anomalies


def test_zeros_no_anomaly():
    model = calibrate_metric("detect_1.csv", 5000)
    anomalies = _execute_model("detect_1.csv", model.get_max_z_score(), model.get_avg_errors(), model.get_std_errors(),
                               model.get_max_ratio(), 1, 5761, 6000)
    expected_anomalies = [False] * 144 + [True] * 20 + [False] * 72
    assert anomalies == expected_anomalies


def test_end_spike():
    file_name = "sequence_with_spike.csv"
    data_sequence = read_sequence_from_file(file_name)

    max_z_score = 10.25490586843348
    std_errors = 3.690263380354088
    avg_errors = 2.0585256595893346
    max_ratio = 6.316789473684211
    detection_forecasting_in_minutes = 30
    forecast_length = 5
    model = ForecastingAnomalyDetector(side="positive",
                                       detection_area_in_minutes=detection_forecasting_in_minutes,
                                       min_forecast_length=forecast_length, z_score_conf_factor=3.5,
                                       ratio_conf_factor=3.5, max_z_score=max_z_score, std_errors=std_errors,
                                       avg_errors=avg_errors, max_ratio=max_ratio)
    anomalies = model.fit_predict(data_sequence)
    assert anomalies == [False, False, False, False, False, False, False, True, True]


def read_sequence_from_file(file_name):
    file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", file_name)
    with open(file_path, 'r') as input_file:
        csv_dict_reader = csv.DictReader(input_file)
        data = [(float(row["actual"]), int(row["timestamp"])) for row in csv_dict_reader]
        values = [item[0] for item in data]
        timestamps = [item[1] for item in data]

        data_sequence = Sequence(timestamps=timestamps, values=values)
        return data_sequence


@pytest.fixture
def mock_get_dynamic_param(monkeypatch):
    monkeypatch.setattr(security_metrics_settings,
                        "get_dynamic_param", lambda x, y: y)

    def mock_cast_to(value):
        if value == "nan":
            return float('nan')
        elif value == "on":
            return -1
        elif value == "model_min_forecast_length_in_minute":
            return 70

    monkeypatch.setattr(security_metrics_settings,
                        "cast_to_int_or_float", mock_cast_to)


def test_security_metrics_settings(mock_get_dynamic_param):
    actual_result = list()
    actual_result.append(
        str(security_metrics_settings.get_security_metrics_settings("nan")))
    actual_result.append(
        security_metrics_settings.get_security_metrics_settings("on"))
    actual_result.append(security_metrics_settings.get_security_metrics_settings(
        "model_min_forecast_length_in_minute"))
    assert actual_result == ["0", 0, 15]


def create_metric_list():
    timestamps = list(range(1611790000, 1612830000))
    values = [0, 0, 0, 1, 1] * 208000
    sequence = Sequence(timestamps, values, "opengauss_log_errors_rate")
    metric_list = [("localhost", "opengauss_log_errors_rate", sequence)]
    return metric_list


@pytest.fixture
def mock_security_anomalies(monkeypatch):
    monkeypatch.setattr(security_diagnosis, "current_ts", lambda: 1612830000)
    monkeypatch.setattr(
        security_diagnosis, "get_list_of_required_metrics", lambda x: ["opengauss_log_errors_rate"])
    monkeypatch.setattr(security_diagnosis.global_vars.agent_proxy,
                        "agent_get_all", lambda: {"host": ["localhost"]})
    monkeypatch.setattr(security_diagnosis.global_vars.worker,
                        "parallel_execute", lambda *args: [{"opengauss_log_errors_rate": 1}])
    monkeypatch.setattr(security_diagnosis,
                        "add_abnormal_value", lambda *args: None)

    def add_scenarios_alarms(*args):
        global find_result
        find_result = args

    monkeypatch.setattr(security_diagnosis, "add_scenarios_alarms", add_scenarios_alarms)


def test_security_calibration(mock_security_anomalies, monkeypatch):
    metric_list = create_metric_list()
    monkeypatch.setattr(ForecastingAnomalyDetector, 'fit_predict', lambda *args: None)
    security_diagnosis.calibrate_security_metrics_serial(metric_list)
    security_diagnosis.find_anomalies_in_security_metrics()
    assert find_result == ([], 1611030000, 1612830000, 'localhost')


def create_detect_data():
    timestamps = list(range(1612840000 - 180000, 1612840000))
    values = [0, 0, 0, 1, 1] * 36000
    sequence = Sequence(timestamps, values, "opengauss_log_errors_rate", 1000)
    return sequence


def test_security_detect():
    calibration_data = {"max_z_score": 1.5, "std_errors": 3, "avg_errors": 0.4, "max_ratio": 5}
    actual_result = security_diagnosis.detect_security_metric(
        "opengauss_log_errors_rate", "localhost", create_detect_data(), calibration_data,
        detection_forecasting=1, model_min_forecast_length=1)
    assert not actual_result


def test_get_metrics():
    global_vars.confpath = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'data')
    global_vars.dynamic_configs = DynamicConfig()
    actual_result = security_diagnosis.get_metrics_that_need_calibration(30)
    assert not actual_result
