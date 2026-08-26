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

import logging
import os
import time
import yaml
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from prometheus_client.exposition import generate_latest
from prometheus_client.registry import CollectorRegistry

from dbmind import constants
from dbmind.common import ha
from . import main
from .dao import MetricConfig
from .dao import query

_thread_pool_executor = ThreadPoolExecutor(max_workers=os.cpu_count())

_registry = CollectorRegistry()
_registered_metrics = dict()
# data leak protection metrics
_dlb_metric_definition = None  # the definition of the dlp data leak protection metrics from the YANL file if any
_look_for_dlp_metrics = False  # if to look for data leak protection metrics
_dlp_metric_units = []  # time units to repeat for data leak protection metrics
_dlp_metrics_added = []  # data leak protection metrics that were added

metric_clock = defaultdict(dict)
DEFAULT_COVERAGE_WINDOW = 300  # seconds


def register_prometheus_metrics(rule_filepath):
    global _look_for_dlp_metrics
    global _dlb_metric_definition
    with open(rule_filepath) as f:
        data = yaml.safe_load(f)

    for metric_name, item in data.items():
        if metric_name == 'opengauss_dlp':
            _look_for_dlp_metrics = True
            _dlb_metric_definition = item
            logging.info("Gauss Data Leak Protection metrics is on")
            for time_unit in item['time_units']:
                _dlp_metric_units.append(time_unit)
            continue

        if item.get('status') == 'disable':
            logging.info('Skipping to register the metric %s.', metric_name)
            continue
        cnf = MetricConfig(
            name=metric_name,
            promql=item['promql'],
            desc=item.get('desc', 'undefined'),
            ttl=item.get('ttl', 0),
            timeout=item.get('timeout'),
            registry=_registry
        )
        for mtr in item['metrics']:
            if mtr["usage"] == "LABEL":
                cnf.add_label(mtr["label"], mtr["name"])

        _registered_metrics[metric_name] = cnf
        logging.info('Registered the metric %s configuration.', metric_name)

    if _look_for_dlp_metrics:
        add_dlp_metrics()


def _standardize_labels(labels_map):
    if 'from_instance' in labels_map:
        labels_map['from_instance'] = labels_map['from_instance'].replace('\'', '')


def add_dpl_metric(dlp_metric_name):
    """
    Adds a single data leak protection metric to the list of metrics handled by the exporter
    @param dlp_metric_name: the name of the metric in security exporter
    @return: None
    """
    try:
        metric_name_base = dlp_metric_name.split("_total")[0]  # the base metric is a counter metric, remove it
        if metric_name_base in _dlp_metrics_added:
            return  # already added

        for time_unit in _dlp_metric_units:
            promo_query = _dlb_metric_definition["promql"]
            promo_query = promo_query.format(dlp_metric_name=dlp_metric_name, time_unit=time_unit)
            metric_name = f"{metric_name_base}_{time_unit}_rate"
            logging.info("Adding data leak protection metric: %s", metric_name)
            cnf = MetricConfig(
                name=metric_name,
                promql=promo_query,
                desc=f"data leak protection metric {metric_name_base}"
            )
            cnf.add_label('job', 'from_job')
            cnf.add_label('from_instance', 'from_instance')

            _registered_metrics[metric_name] = cnf

        _dlp_metrics_added.append(metric_name_base)  # so we will not add it again

    except ValueError as _:
        #  If the metric is already added, move on
        pass

    except Exception as e:
        logging.error("Failed adding data leak protection metric: %s", dlp_metric_name)
        logging.exception(e)


def add_dlp_metrics():
    """
    Adds all data leak protection metrics to the system
    Data leak metrics are created dynamically by the user using audit policy.
    therefore, there is no way to know in advance what metrics to add and we have to dynamically load them all
    @return: None
    """
    promql = 'sum by(__name__)({ app="opengauss_data_leak_protection"})'
    result = query(promql)
    for item in result:
        add_dpl_metric(item.name)


def query_all_metrics():
    if _look_for_dlp_metrics:
        add_dlp_metrics()

    queried_results = []
    # Return a two-tuples which records the input with output
    # because prevent out of order due to concurrency.
    all_tasks = [
        _thread_pool_executor.submit(
            lambda cnf: (cnf.name, cnf.query()),
            cnf
        ) for cnf in _registered_metrics.values()
    ]
    for future in as_completed(all_tasks):
        try:
            queried_results.append(future.result())
        except Exception as e:
            logging.exception(e)

    scraped_label_values = defaultdict(dict)
    for metric_name, diff_instance_results in queried_results:
        for result_sequence in diff_instance_results:
            if len(result_sequence) == 0:
                logging.warning('Fetched nothing for %s.', metric_name)
                continue

            cnf = _registered_metrics[metric_name]
            actual_labels = result_sequence.labels
            # Unify the outputting label names for all metrics.
            target_labels_map = {}
            for k, v in actual_labels.items():
                target_label_name = cnf.get_label_name(k)
                if not target_label_name:
                    continue
                target_labels_map[target_label_name] = v

            _standardize_labels(target_labels_map)
            value = result_sequence.values[0]

            try:
                label_values = tuple(str(target_labels_map[k]) for k in cnf.labels)
            except KeyError:
                continue

            scraped_label_values[metric_name][label_values] = (target_labels_map, value)
            metric_clock[metric_name][label_values] = time.monotonic() + DEFAULT_COVERAGE_WINDOW

    for metric_name, cnf in _registered_metrics.items():
        # Scraped label_values - directly set
        metric_label_values = scraped_label_values.get(metric_name, {})
        for label_values, (labels, value) in metric_label_values.items():
            try:
                cnf.gauge.labels(**labels).set(value)
            except ValueError as e:
                logging.error('Error occurred: %s. Metric name: %s, rendered labels: %s, '
                              'expected labels: %s.',
                              e.args, metric_name, labels, cnf.labels)
            except Exception as e:
                logging.exception(e)

        for label_values in cnf.gauge._metrics.keys() - scraped_label_values.keys():
            # Remove the expired metric
            if metric_clock[metric_name].get(label_values, 0) < time.monotonic():
                logging.info("The metric: %s of %s was discarded.",
                             metric_name, label_values)
                cnf.gauge.remove(*label_values)
                if label_values in metric_clock[metric_name]:
                    metric_clock[metric_name].pop(label_values)

    return generate_latest(_registry)


def check_status_reprocessing_exporter(cmd):
    cur_path = os.path.realpath(os.path.dirname(__file__))
    proj_path = cur_path[:cur_path.rfind('dbmind')]
    log_path = main.exporter_info_dict['logfile']
    pid_file = os.path.join(proj_path, constants.REPROCESSING_PIDFILE_NAME)
    status_info = ha.check_status_impl(log_path, pid_file, 'reprocessing_exporter', ())
    ha.record_interface_info('check_status', status_info)
    return status_info


def repair_interface_reprocessing_exporter(cmd):
    cur_path = os.path.realpath(os.path.dirname(__file__))
    proj_path = cur_path[:cur_path.rfind('dbmind')]
    log_path = main.exporter_info_dict['logfile']
    pid_file = os.path.join(proj_path, constants.REPROCESSING_PIDFILE_NAME)
    repair_info = ha.repair_interface_impl(log_path, pid_file, 'reprocessing_exporter', ())
    ha.record_interface_info('repair', repair_info)
    return repair_info
