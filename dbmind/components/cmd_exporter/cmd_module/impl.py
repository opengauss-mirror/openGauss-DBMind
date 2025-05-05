# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
import time

from dbmind.common.utils import raise_fatal_and_exit, cast_to_int_or_float
from dbmind.components.cmd_exporter.cmd_module.utils import PROCESS_METHODS

from ..query import Metric
from ..query import QueryInterface

GLOBAL_LABELS = {}
CACHE = {}

DEFAULT_COVERAGE_WINDOW = 300


def set_constant_labels(labels):
    GLOBAL_LABELS.update(labels)


def process_args(args, remaining_time):
    res = dict()
    if args is None:
        return res

    for arg_name, value in args.items():
        if arg_name == "timeout" and value == "remaining_time":
            res[arg_name] = remaining_time
        elif "CACHE:" in value:
            _, name, label = value.split(":")
            if label == "ALL":
                res[arg_name] = CACHE.get(name)
            else:
                res[arg_name] = CACHE.get(name).get(label)
        else:
            res[arg_name] = value

    return res


class CmdQuery(QueryInterface):
    def __init__(self, name, item):
        self.name = name
        self.timeout = item.get('timeout')
        self.disabled = item.get('status') == "disable"
        self.label_names = list()  # label_names is a list because it's order sensitive
        self.process = list()
        self.metrics = list()
        self.ttl = item.get('ttl', 0)
        self.null_coverage_window = item.get('null_coverage_window', DEFAULT_COVERAGE_WINDOW)
        self.last_updated = time.monotonic()
        self.cached_results = None
        self.metric_clock = dict()
        CACHE[self.name] = dict()
        for metric_item in item.get('metrics', []):
            # Parse dict structure to a Metric object, then we can
            # use this object's fields directly.
            suffix = metric_item.get("name")
            self.process.append(metric_item.copy())
            if metric_item.get('usage') is None:
                continue

            metric = Metric(suffix, metric_item)
            if not metric.is_valid:
                continue

            if not metric.is_label:
                metric.set_prefix(self.name)
                self.metrics.append(metric)
            else:
                self.label_names.append(suffix)

        # `GLOBAL_LABELS` is required and must be added anytime.
        for label_name in GLOBAL_LABELS:
            if label_name not in self.label_names:
                self.label_names.append(label_name)

        if len(self.label_names) == 0:
            raise_fatal_and_exit(
                "Please specify at least one label "
                "for '%s' in the configuration file." % self.name,
                use_logging=False
            )

    def attach(self, registry):
        for i, metric in enumerate(self.metrics):
            self.metrics[i].activate(self.label_names)
            registry.register(self.metrics[i].entity)

    def cmd_query(self):
        """ To query os by command line."""
        if self.cached_results and time.monotonic() < self.last_updated + self.ttl:
            return self.cached_results

        self.last_updated = time.monotonic()
        endtime = time.monotonic() + self.timeout if isinstance(self.timeout, (int, float)) else None
        invalid_result = False
        label_names, label_results, value_results = list(), list(), list()
        for metric_item in self.process:
            remaining_time = None if endtime is None else endtime - time.monotonic()
            suffix = metric_item.get("name")
            method = PROCESS_METHODS[metric_item.get("method")]
            kwargs = process_args(metric_item.get("args"), remaining_time)
            if isinstance(remaining_time, (int, float)) and remaining_time < 0:
                logging.warning('Timed out after %d seconds while executing %s, input is %s.',
                                remaining_time, method, kwargs)
                return [], []

            result = method(**kwargs)
            if result:
                CACHE[self.name][suffix] = result
            elif isinstance(result, str):
                if self.name != "opengauss_cluster":
                    CACHE[self.name][suffix] = result
                else:
                    return [], []

            elif metric_item.get('usage') is None:
                logging.warning("Fetched nothing for process: %s(%s).", method, kwargs)
                invalid_result = True

            if metric_item.get('usage') == "LABEL":
                label_names.append(suffix)
                label_results.append(result)
            elif metric_item.get('usage') is not None:
                value_results.append(result)

        if invalid_result:
            return [], []

        for label_name in self.label_names:
            if label_name not in label_names:
                label_results.append(GLOBAL_LABELS.get(label_name, 'None').split('\n'))

        if not label_results:
            logging.warning("Fetched nothing on label for the metric '%s'.", self.name)
            return [], []

        if not value_results:
            logging.warning("Fetched nothing on value for the metric '%s'.", self.name)
            return [], []

        self.cached_results = label_results, value_results

        return label_results, value_results

    def update(self):
        def _get_or_create(list_object, index):
            if index < len(list_object):
                return list_object[index]
            new_dict = dict()
            list_object.append(new_dict)
            return new_dict

        def construct_labels():
            n_row = max([len(vec) for vec in label_results])
            raw_label = [{k: "" for k in self.label_names} for _ in range(n_row)]
            for col_idx, name in enumerate(self.label_names):
                for row_idx in range(n_row):
                    label_result = label_results[col_idx]
                    if not label_result:
                        continue
                    elif row_idx > len(label_result) - 1:
                        raw_label[row_idx][name] = label_result[0]
                    else:
                        raw_label[row_idx][name] = label_result[row_idx]
            return raw_label

        def construct_values():
            r = []
            for col_idx, single_metric in enumerate(self.metrics):
                for row_idx, value in enumerate(value_results[col_idx]):
                    d = _get_or_create(r, row_idx)
                    d[single_metric.name] = value
            return r

        def set_metric_value(name, usage, family_, value_):
            if len(value_) == 0:
                logging.warning('Not found field %s in the %s.', name, self.name)
            else:
                value_ = cast_to_int_or_float(value_)
                # Different usages (Prometheus data type) have different setting methods.
                # Thus, we have to select to different if-branches according to metric's usage.
                if usage == 'COUNTER' or usage == 'GAUGE':
                    family_.set(value_)
                elif usage == 'SUMMARY' or usage == 'HISTOGRAM':
                    family_.observe(value_)
                else:
                    logging.error('Not supported metric %s due to usage %s.', name, usage)

        if self.disabled:
            return

        scraped_label_values = dict()
        label_results, value_results = self.cmd_query()
        if label_results and value_results:
            # Check whether we can merge these label and metric query results.
            if (
                isinstance(label_results[0], list) and
                isinstance(value_results[0], list) and
                len(label_results[0]) != len(value_results[0])
            ):
                logging.error('Cannot fetch the metric because the dimension '
                              'between label and metric is not consistent.')
                return

            labels_ = construct_labels()
            values_ = construct_values()
            # Update for all metrics in current query instance.
            for label, value in zip(labels_, values_):
                label_values = tuple(str(label[k]) for k in self.label_names)
                scraped_label_values[label_values] = (label, value)
                self.metric_clock[label_values] = time.monotonic() + self.null_coverage_window

        for metric in self.metrics:
            # Scraped label_values - directly set
            for label_values, (label, value) in scraped_label_values.items():
                metric_family = metric.entity.labels(**label)
                metric_value = value[metric.name]
                set_metric_value(metric.name, metric.usage, metric_family, metric_value)

            for label_values in metric.entity._metrics.keys() - scraped_label_values.keys():
                if (
                    metric.prefix in ["opengauss_cluster", "opengauss_nic"] or
                    (metric.prefix == "opengauss_ping" and metric.name == "state")
                ):
                    metric.entity.remove(*label_values)
                    if label_values in self.metric_clock:
                        self.metric_clock.pop(label_values)
                    continue

                # Remove the expired metric
                if self.metric_clock.get(label_values, 0) < time.monotonic():
                    logging.info("The metric: %s_%s of %s was discarded.",
                                 metric.prefix, metric.name, label_values)
                    metric.entity.remove(*label_values)
                    if label_values in self.metric_clock:
                        self.metric_clock.pop(label_values)
