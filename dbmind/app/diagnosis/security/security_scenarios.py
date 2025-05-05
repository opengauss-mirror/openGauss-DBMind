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
Security scenarios definition
"""
import datetime
import logging

import yaml

from dbmind.common.types import Alarm, ALARM_TYPES, ALARM_LEVEL, AnomalyTypes
from dbmind.common.types import RootCause
from dbmind.metadatabase.dao.security_anomalies import count_anomalies


class SecurityScenarioMetric:
    """
    represents a security metric in a security scenario
    """

    def __init__(self, name, weight, anomalies_num_in_period):
        """
        SecurityScenarioMetric constructor
        @param name: str
        @param weight: float
        @param anomalies_num_in_period: int number of anomalies in time period that receives the maximum weight to its
        score
        """
        self._name = name
        self._weight = weight
        self._anomalies_num_in_period = anomalies_num_in_period

    def __str__(self):
        """
        Standard function - useful for testing
        """
        return f"name: {self._name}, weight: {self.weight}"

    @property
    def weight(self):
        """
        The metric weight should be available from the scenario,
        when is empty it is distributed equally between metrics
        """
        return self._weight

    @weight.setter
    def weight(self, new_weight):
        """
        The metric weight should be editable from the scenario,
        when is empty it is distributed equally between metrics
        """
        self._weight = new_weight

    @property
    def name(self):
        """
        Expose the metric name
        """
        return self._name

    def evaluate(self, from_date, to_date, host):
        """
        evaluates the score of a metric in a given time range
        @param from_date: int ts in milliseconds
        @param to_date: int ts in milliseconds
        @param host: str host name to look for
        @return tuple float with score, number of abnormal values in time period

        """
        metric_anomalies = count_anomalies(self._name, from_date, to_date, host)
        logging.info("Evaluate scenario metric %s on %s, found %s abnormal values",
                     self._name, host, metric_anomalies)
        if metric_anomalies == 0:
            return 0, metric_anomalies
        if metric_anomalies >= self._anomalies_num_in_period:
            return self._weight, metric_anomalies
        if self._anomalies_num_in_period == 0:
            raise ValueError(f'anomalies_num_in_period of {self._name} can not be 0.')
        return self._weight * metric_anomalies / self._anomalies_num_in_period, metric_anomalies

    @staticmethod
    def load_from_yaml(yaml_document, default_anomalies_num_in_period):
        """
        loads a SecurityScenarioMetric from YAML definition
        @param yaml_document: dict the definition
        @param default_anomalies_num_in_period: int
        @return: None
        """
        name = yaml_document.get("name", "")
        if len(name) == 0:
            logging.error("Scenario Metric name is empty, skipping it: %s", yaml_document)
            return None
        anomalies_num_in_period = yaml_document.get("anomalies_num_in_period", default_anomalies_num_in_period)
        weight = yaml_document.get("weight", 0)
        result = SecurityScenarioMetric(name, weight, anomalies_num_in_period)
        return result


class SecurityScenario:
    def __init__(self, name, metrics, low_threshold, medium_threshold, high_threshold, root_cause=None):
        """
        SecurityScenario constructor
        @param name: str
        @param metrics: list of SecurityScenarioMetric
        @param low_threshold: float
        @param medium_threshold: float
        @param high_threshold: float
        @param root_cause: str
        """
        self._name = name
        self._metrics = metrics
        self._low_threshold = low_threshold
        self._medium_threshold = medium_threshold
        self._high_threshold = high_threshold
        self._root_cause = root_cause

    def __str__(self):
        """
        standard function useful for testing
        """
        result = f"name: {self._name} {self._root_cause}, Thresholds: {self._low_threshold} - " \
                 f"{self._medium_threshold} - {self._high_threshold}, " \
                 f"metrics: {[str(metric) for metric in self._metrics]}"
        return result

    def get_metrics(self) -> list:
        """
        @return list of required metrics
        """
        return [item.name for item in self._metrics]

    def evaluate(self, from_date, to_date, host):
        """
        evaluates the score of a scenario
        @param from_date: int ts in milliseconds
        @param to_date: int ts in milliseconds
        @param host: str
        @return list of alarms if any
        """
        from_date_str = datetime.datetime.fromtimestamp(round(from_date / 1000)).strftime("%Y-%m-%d %H:%M:%S")
        to_date_str = datetime.datetime.fromtimestamp(round(to_date / 1000)).strftime("%Y-%m-%d %H:%M:%S")
        logging.info("Evaluating scenario '%s' on %s period: %s - %s",
                     self._name, host, from_date_str, to_date_str)
        evaluate_value = 0
        metric_finding = dict()
        for metric in self._metrics:
            metric_score, anomalies_count = metric.evaluate(from_date, to_date, host)
            evaluate_value += metric_score
            metric_finding[metric.name] = anomalies_count
        if evaluate_value == 0:
            logging.info("No alarms found for scenario %s on %s", self._name, host)
            return None
        if evaluate_value < self._low_threshold:
            logging.info("scenario %s  on %s rate is less than the threshold, %s ignoring it",
                         self._name, host, self._low_threshold)
            return None
        level = ALARM_LEVEL.INFO
        if evaluate_value >= self._high_threshold:
            level = ALARM_LEVEL.CRITICAL
        elif evaluate_value >= self._medium_threshold:
            level = ALARM_LEVEL.WARNING
        logging.info("Adding alarm %s for scenario %s", level, self._name)
        alarm_cause = None
        if self._root_cause is not None:
            try:
                alarm_cause = RootCause.get(self._root_cause)
            except Exception:
                logging.error("Cannot get exception of %s", self._root_cause)
        alarm_content = SecurityScenario.build_alarm_content(from_date, to_date, metric_finding)
        logging.info("content: %s", alarm_content)
        scenario_alarm = Alarm(
            instance=host,
            alarm_content=alarm_content,
            alarm_type=ALARM_TYPES.SECURITY,
            anomaly_type=AnomalyTypes.SPIKE,
            metric_name=self._name,
            metric_filter={},
            alarm_level=level,
            alarm_cause=str(alarm_cause),
            start_timestamp=from_date,
            end_timestamp=to_date
        )
        return scenario_alarm

    @property
    def name(self):
        """
        Expose the scenario name
        """
        return self._name

    def add_missing_weight(self):
        """
        If some metrics does not have weight, distribute the weight evenly between metrics with no weight
        """
        logging.info("Distributing weight for metrics with no weight in scenario %s", self._name)
        count_zero_weights = sum(map(lambda metric: metric.weight == 0, self._metrics))

        if count_zero_weights > 0:
            total_weight = sum([metric.weight for metric in self._metrics])
            weight_per_metric = (1 - total_weight) / count_zero_weights
            if weight_per_metric > 0:
                for index in range(len(self._metrics)):
                    if self._metrics[index].weight == 0:
                        self._metrics[index].weight = weight_per_metric
            else:
                logging.error("Could not distribute negative weight for metrics with no weight in scenario %s",
                              self._name)

    @staticmethod
    def build_alarm_content(from_ts, to_ts, metrics_anomalies):
        """
        @param from_ts: float
        @param to_ts: float
        @param metrics_anomalies: dict with metric_name as keys and number of anomalies fond as value
        @return alarm str content in user friendly format
        """
        from_date_str = datetime.datetime.fromtimestamp(round(from_ts / 1000)).strftime("%Y-%m-%d %H:%M:%S")
        to_date_str = datetime.datetime.fromtimestamp(round(to_ts / 1000)).strftime("%Y-%m-%d %H:%M:%S")
        metrics_anomalies_list = list()
        for metric_name, value in metrics_anomalies.items():
            metrics_anomalies_list.append(f"had {value} anomalies in metric:{metric_name}")
        alarm_content = f"Between {from_date_str} and {to_date_str} found the following anomalies: " \
                        f"{', '.join(metrics_anomalies_list)}"
        return alarm_content

    @staticmethod
    def load_from_yaml(scenario_dict, default_low_threshold, default_medium_threshold, default_high_threshold,
                       default_anomalies_num_in_period):
        """
        loads the scenario definition form YAML file
        @param scenario_dict: dict of scenario details as extracted from yaml
        @param default_low_threshold: float to ne used when missing in YAML document
        @param default_medium_threshold: float to be used when missing in YAML document
        @param default_high_threshold: float to be used when missing in YAML document
        @param default_anomalies_num_in_period: int to be used when missing in YAML document
        score
        @return SecurityScenario instance if yaml document is valid, or None
        """
        name = scenario_dict.get("name", "")
        if len(name) == 0:
            logging.error("scenario name is not defined")
            return None
        if "metrics" not in scenario_dict or not isinstance(scenario_dict["metrics"], list) \
                or len(scenario_dict["metrics"]) < 1:
            logging.error("scenario metrics are not defined skipping %s", scenario_dict)
            return None
        metrics = []
        for metric in scenario_dict["metrics"]:
            metric = SecurityScenarioMetric.load_from_yaml(metric, default_anomalies_num_in_period)
            if metric is not None:
                metrics.append(metric)

        if len(metrics) == 0:
            logging.error("scenario metrics array is empty skipping %s", scenario_dict)
            return None
        root_cause = scenario_dict.get("root_cause", None)
        low_threshold = scenario_dict.get("low_threshold", default_low_threshold)
        medium_threshold = scenario_dict.get("medium_threshold", default_medium_threshold)
        high_threshold = scenario_dict.get("high_threshold", default_high_threshold)
        result = SecurityScenario(name, metrics, low_threshold, medium_threshold, high_threshold, root_cause)
        result.add_missing_weight()

        return result


def get_list_of_required_metrics(scenarios: list) -> list:
    metrics = []
    for scenario in scenarios:
        for metric in scenario.get_metrics():
            if metric not in metrics:
                metrics.append(metric)
    return metrics


def load_scenarios_yaml_definitions(yaml_file_path, default_low_threshold=0.2, default_medium_threshold=0.6,
                                    default_high_threshold=0.8, default_anomalies_num_in_period=5):
    """
    loads scenarios definition form YAML file
    @param yaml_file_path: string path where the yaml file is
    @param default_low_threshold: float to ne used when missing in YAML document
    @param default_medium_threshold: float to be used when missing in YAML document
    @param default_high_threshold: float to be used when missing in YAML document
    @param default_anomalies_num_in_period: int to be used when missing in YAML document
    score
    @return list of SecurityScenario instances if at least one scenario in the yaml document is valid, or an empty list
    """
    try:
        with open(yaml_file_path) as scenarios_file:
            scenarios_definitions = yaml.safe_load(scenarios_file)
            if 'scenarios' not in scenarios_definitions:
                logging.error("No scenarios found in %s", scenarios_definitions)
                return []
        result = []
        for scenario_yaml in scenarios_definitions['scenarios']:
            scenario = SecurityScenario.load_from_yaml(scenario_yaml, default_low_threshold, default_medium_threshold,
                                                       default_high_threshold, default_anomalies_num_in_period)
            if scenario is not None:
                result.append(scenario)
        return result

    except FileNotFoundError as file_access_error:
        logging.error("load_yaml_definitions failed due to exception - cannot run security scenarios and alert")
        logging.exception(file_access_error)
        return []

    except Exception as parse_error:
        logging.error("load_yaml_definitions failed due to exception")
        logging.exception(parse_error)
        return []
