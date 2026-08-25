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
import datetime
import os

import pytest
import yaml

import dbmind.app.diagnosis.security.security_scenarios as security_scenarios_module
from dbmind.app.diagnosis.security import security_scenarios
from dbmind.app.diagnosis.security.security_scenarios import get_list_of_required_metrics
from dbmind.metadatabase.dao import security_anomalies

security_yml = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'data/security_scenarios.yml')


def test_parsing():
    scenarios = security_scenarios_module.load_scenarios_yaml_definitions(security_yml)
    assert len(scenarios) == 3
    assert str(scenarios[0]) == "name: scanning_attack SCANNING_ATTACK, Thresholds: 0.2 - 0.6 - 0.8, " \
                                "metrics: ['name: opengauss_log_errors_rate, weight: 0.5', " \
                                "'name: opengauss_user_violation_rate, weight: 0.5']"
    assert str(scenarios[1]) == "name: brute_force_login_attack TOO_MANY_INVALID_LOGINS, " \
                                "Thresholds: 0.1 - 0.3 - 0.4, " \
                                "metrics: ['name: opengauss_invalid_logins_rate, weight: 0.5', " \
                                "'name: opengauss_user_locked_rate, weight: 0.5']"
    assert str(scenarios[2]) == "name: user_violation_attack TOO_MANY_USER_VIOLATION, " \
                                "Thresholds: 0.2 - 0.6 - 0.8, " \
                                "metrics: ['name: opengauss_user_violation_rate, weight: 1.0']"


def test_get_required_metrics():
    scenarios = security_scenarios_module.load_scenarios_yaml_definitions(security_yml)
    metrics = get_list_of_required_metrics(scenarios)
    expect_metrics = ['opengauss_log_errors_rate', 'opengauss_user_violation_rate',
                      'opengauss_invalid_logins_rate', 'opengauss_user_locked_rate']
    assert expect_metrics == metrics


@pytest.fixture
def mock_securityscenario(monkeypatch):
    def count_anomalies(*args):
        return args[2]

    monkeypatch.setattr(security_scenarios, "count_anomalies", count_anomalies)
    monkeypatch.setattr(security_scenarios.RootCause, "get", lambda x: "pytest mocking error")


def test_securityscenario_evaluate_alarm(mock_securityscenario):
    yaml_file_path = security_yml
    with open(yaml_file_path) as scenarios_file:
        scenarios_definitions = yaml.safe_load(scenarios_file)
    scenario_par = scenarios_definitions['scenarios'][0]
    scenario_par["metrics"] = [security_scenarios.SecurityScenarioMetric(**scenario_par["metrics"][0])]
    scenario_par["root_cause"] = ""
    scenario = security_scenarios.SecurityScenario(**scenario_par)

    result = dict()
    result["scenario_info"] = scenario.get_metrics()
    result["no_alarm"] = str(scenario.evaluate(0, 0, "localhost"))
    result["low_threshold"] = str(scenario.evaluate(0, 3, "localhost"))
    result["alarm"] = str(scenario.evaluate(0, 5, "localhost"))
    assert result == {'scenario_info': ['opengauss_log_errors_rate'],
                      'no_alarm': 'None',
                      'low_threshold': '[Between 1970-01-01 08:00:00 and 1970-01-01 08:00:00 found the '
                                       'following anomalies: had 3 anomalies in metric:opengauss_log_errors_rate]'
                                       '(pytest mocking error)',
                      'alarm': '[Between 1970-01-01 08:00:00 and 1970-01-01 08:00:00 found the following anomalies: '
                               'had 5 anomalies in metric:opengauss_log_errors_rate](pytest mocking error)'}


@pytest.fixture
def mock_security_anomalies(monkeypatch):
    class Query():

        count_call_query = 0

        @classmethod
        def filter(cls, *args, **kwargs):
            cls.count_call_query += 1
            if cls.count_call_query < 4:
                return [[0]]
            elif cls.count_call_query == 4:
                return [[int(datetime.datetime.now().timestamp()) - 3600]]
            elif cls.count_call_query == 5:
                return [[0]]
            elif cls.count_call_query == 6:
                return [[None]]

    class Session():
        def query(self, *args):
            return Query()

        @staticmethod
        def __enter__():
            return Session()

        @staticmethod
        def __exit__(*args, **kwargs):
            pass

    monkeypatch.setattr(security_anomalies, "get_session", lambda: Session())


def test_security_anomalies(mock_security_anomalies):
    actual_result = list()

    param = {"metric_name": "opengauss_log_errors_rate", "start_period": 0, "end_period": 10, "host": "localhost"}
    count_records = security_anomalies.count_anomalies(**param)
    actual_result.append(count_records)

    security_anomalies.add_abnormal_value("opengauss_log_errors_rate", "localhost", 1, 0)

    count_records = security_anomalies.should_first_calibrate_security_metric(
        "opengauss_log_errors_rate", "localhost")
    actual_result.append(count_records)

    for _ in range(3):
        min_calibration_ts = security_anomalies.get_calibration_model_age_in_minutes(
            "opengauss_log_errors_rate", "host")
        actual_result.append(round(min_calibration_ts))

    assert actual_result == [0, True, 60, 0, 0]
