# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
import datetime
import os
import time

import numpy as np

from dbmind.common.algorithm.anomaly_detection.forecasting_anomaly_detector import ForecastingAnomalyDetector
from dbmind.common.types.sequence import Sequence
from tests.test_security_anomaly_calibration import calibrate_metric


def _current_ts():
    ct = datetime.datetime.now()
    ts = ct.timestamp()
    return int(ts)


def _print_current_time(message):
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(message, "Time =", current_time)


def _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies,
                   training_length=2000, detection_in_minutes=20):
    model = ForecastingAnomalyDetector(side="positive", detection_area_in_minutes=detection_in_minutes,
                                       max_z_score=max_z_score,
                                       std_errors=std_error,
                                       avg_errors=avg_error,
                                       max_ratio=max_ratio,
                                       min_forecast_length=1500)
    file_name_os = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", file_name)
    with open(file_name_os, newline="") as data_file:
        csv_reader = csv.reader(data_file)
        csv_data = list(csv_reader)
    csv_data = csv_data[1:]
    values = [float(item[1]) for item in csv_data]
    dt_epoch = [int(time.mktime((datetime.datetime.strptime(item[2], "%Y-%m-%d %H:%M:%S")).timetuple()) * 1000)
                for item in csv_data]
    step = (dt_epoch[1] - dt_epoch[0]) / 1000
    detection_length = int(np.divide(detection_in_minutes * 60, step))
    values = values[-(training_length + detection_length):]
    dt_epoch = dt_epoch[-(training_length + detection_length):]
    s = Sequence(timestamps=list(dt_epoch), values=values)
    anomalies_indexes = []
    _print_current_time(f"fit predict {file_name} ...")
    anomalies = model.fit_predict(s)
    _print_current_time(f"fit predict {file_name} done")
    for index in range(detection_length):
        if anomalies[index]:
            anomalies_indexes.append(index)
    print(anomalies_indexes)
    assert anomalies_indexes == expected_anomalies


def test_simple_anomalies():
    max_z_score = 5.5438584227542345
    std_error = 1.7300130792600246
    avg_error = 0.8232622495906153
    max_ratio = 3.2271054879754537
    file_name = "security_test_with_anomalies.csv"
    expected_anomalies = [10, 11, 12, 13, 14, 15, 16, 17, 18]
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies)


def test_art_daily_flat_middle_1min():
    max_z_score = 2.1122084199559605
    std_error = 4353.244027416321
    avg_error = 2934.688912934593
    max_ratio = 104.65949203879369
    file_name = "art_daily_flatmiddle_1min.csv"
    expected_anomalies = []
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies)


def test_art_daily_jumpsup_1min():
    calibration_model = calibrate_metric("art_daily_jumpsup_1min.csv", 1500, 300, 50, "value")
    max_z_score = calibration_model.get_max_z_score()
    std_error = calibration_model.get_std_errors()
    avg_error = calibration_model.get_avg_errors()
    max_ratio = calibration_model.get_max_ratio()

    file_name = "art_daily_jumpsup_1min.csv"
    expected_anomalies = [456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474,
                          475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493,
                          494, 495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512,
                          513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531,
                          532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550,
                          551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 575]
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 2500, 1500)


def test_art_daily_no_jumps_1min():
    max_z_score = 2.1340660167532572
    std_error = 1666.2267910802138
    avg_error = 1120.3206019986853
    max_ratio = 3.5274548868340547
    file_name = "art_daily_nojumps_1min.csv"
    expected_anomalies = []
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 2000, 1045)


def test_zeros_no_anomalies():
    max_z_score = 1
    std_error = 0
    avg_error = 0
    max_ratio = 1
    file_name = "test_zeros_no_anomaly.csv"
    expected_anomalies = []
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 500, 20)


def test_zeros_with_anomalies():
    max_z_score = 1
    std_error = 0
    avg_error = 0
    max_ratio = 1
    file_name = "test_zeros_with_anomaly.csv"
    expected_anomalies = [15]
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 500, 20)


def test_positives_no_anomalies():
    max_z_score = 1
    std_error = 0
    avg_error = 7
    max_ratio = 1
    file_name = "test_positives_no_anomaly.csv"
    expected_anomalies = []
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 500, 20)


def test_positives_with_anomalies():
    max_z_score = 1
    std_error = 0
    avg_error = 7
    max_ratio = 1
    file_name = "test_positives_with_anomaly.csv"
    expected_anomalies = [10, 15]
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 500, 20)


def test_negatives_with_anomalies():
    max_z_score = 1
    std_error = 0
    avg_error = -7
    max_ratio = 1
    file_name = "test_negatives_with_anomaly.csv"
    expected_anomalies = [10, 15]
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 500, 20)


def test_negative_actual_with_anomalies():
    max_z_score = 1
    std_error = 0
    avg_error = 7
    max_ratio = 1
    file_name = "test_negative_actual_with_anomaly1.csv"
    expected_anomalies = []
    _execute_model(file_name, max_z_score, std_error, avg_error, max_ratio, expected_anomalies, 500, 20)
