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
import subprocess
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from prometheus_client import (
    Gauge, Summary, Histogram, Info, Enum
)
from prometheus_client.exposition import generate_latest
from prometheus_client.registry import CollectorRegistry

from dbmind.common.utils import dbmind_assert, cast_to_int_or_float, raise_fatal_and_exit
from dbmind.common.cmd_executor import multiple_cmd_exec

PROMETHEUS_TYPES = {
    # Indeed, COUNTER should use the type `Counter` rather than `Gauge`,
    # but PG-exporter and openGauss-exporter (golang version)
    # are all using ConstValue (i.e., the same action as Gauge),
    # so we have to inherit the usage.
    'COUNTER': Gauge, 'GAUGE': Gauge, 'SUMMARY': Summary,
    'HISTOGRAM': Histogram, 'INFO': Info, 'ENUM': Enum
}

PROMETHEUS_LABEL = 'LABEL'
PROMETHEUS_DISCARD = 'DISCARD'
FROM_INSTANCE_KEY = 'from_instance'

_thread_pool_executor = None
_registry = CollectorRegistry()

global_labels = {FROM_INSTANCE_KEY: ''}

query_instances = list()


class Metric:
    """Metric family structure:
    Only parsing the metric dict and
    lazy loading the Prometheus metric object."""

    def __init__(self, name, item):
        self.name = name
        self.desc = item.get('description', '')
        self.usage = item['usage'].upper()
        self.subquery = item['subquery']
        self.is_valid = True
        self.is_label = False

        self._prefix = ''
        self._value = None

        if self.usage in PROMETHEUS_TYPES:
            """Supported metric type."""
        elif self.usage == PROMETHEUS_LABEL:
            """Use the `is_label` field to mark this metric as a label."""
            self.is_label = True
        elif self.usage == PROMETHEUS_DISCARD:
            """DISCARD means do nothing."""
            self.is_valid = False
        else:
            raise ValueError('Not supported usage %s.' % self.usage)

    def set_prefix(self, prefix):
        self._prefix = prefix

    def activate(self, labels=()):
        dbmind_assert(not self.is_label and self._prefix)

        self._value = PROMETHEUS_TYPES[self.usage](
            '%s_%s' % (self._prefix, self.name), self.desc, labels
        )

    @property
    def entity(self):
        dbmind_assert(self._value, "Should be activated first.")
        return self._value


def perform_shell_command(cmd, **kwargs):
    r"""Perform a shell command by using subprocess module.

    There are two primary fields for kwargs:
    1. input: text, passing to stdin
    2. timeout: waiting for the command completing in the specific seconds.
    """
    try:
        if 'input' in kwargs and isinstance(kwargs['input'], str):
            kwargs['input'] = kwargs['input'].encode(errors='ignore')
        returncode, output, err = multiple_cmd_exec(cmd, **kwargs)
        exitcode = 0
    except subprocess.CalledProcessError as ex:
        output = ex.output
        exitcode = ex.returncode
    except subprocess.TimeoutExpired:
        output = b''
        logging.warning(
            'Timed out after %d seconds while executing %s, input is %s.',
            kwargs.get('timeout'),
            cmd, kwargs['input']
        )
        exitcode = -1
    except Exception as e:
        logging.error('%s raised while executing %s with input %s.',
                      e, cmd, kwargs.get('input'))
        raise e

    if output[-1:] == b'\n':
        output = output[:-1]
    output = output.decode(errors='ignore')
    return exitcode, output


class QueryInstance:
    def __init__(self, name, item):
        self.name = name
        self.query = item['query']
        self.timeout = item.get('timeout')
        self.metrics = []
        self.label_names = []
        self.label_objs = {}

        for m in item['metrics']:
            for name, metric_item in m.items():
                # Parse dict structure to a Metric object, then we can
                # use this object's fields directly.
                metric = Metric(name, metric_item)
                if not metric.is_valid:
                    continue
                if not metric.is_label:
                    metric.set_prefix(self.name)
                    self.metrics.append(metric)
                else:
                    self.label_names.append(metric.name)
                    self.label_objs[metric.name] = metric

        # `global_labels` is required and must be added anytime.
        self.label_names.extend(global_labels.keys())
        if len(self.label_names) == 0:
            raise_fatal_and_exit(
                "Please specify at least one label "
                "for '%s' in the configuration file." % self.name,
                use_logging=False
            )

    def attach(self, registry):
        for metric in self.metrics:
            metric.activate(self.label_names)
            registry.register(metric.entity)

    def update(self):
        # Clear old metric's value and its labels.
        for metric in self.metrics:
            metric.entity.clear()

        endtime = time.time() + self.timeout
        exitcode, query_result = perform_shell_command(self.query, timeout=self.timeout)

        if not query_result:
            logging.warning("Fetched nothing for query '%s'." % self.query)
            return

        # Update for all metrics in current query instance.
        # `global_labels` is the essential labels for each metric family.
        label_query_results = []
        for label_name in self.label_names:
            if label_name in self.label_objs:
                obj = self.label_objs[label_name]
                remaining_time = endtime - time.time()
                _, result = perform_shell_command(
                    obj.subquery, input=query_result, timeout=remaining_time
                )
            else:
                result = global_labels.get(label_name, 'None')

            label_query_results.append(result.split('\n'))

        if len(label_query_results) == 0:
            logging.warning(
                "Fetched nothing on label for the metric '%s'." % self.name
            )
            return

        metric_query_results = []
        for metric in self.metrics:
            remaining_time = endtime - time.time()
            _, result = perform_shell_command(
                metric.subquery, input=query_result,
                timeout=remaining_time
            )
            metric_query_results.append(result.split('\n'))
        if len(metric_query_results) == 0:
            logging.warning(
                "Fetched nothing on metric value for the metric '%s'." % self.name
            )
            return

        # Check whether we can merge these label
        # and metric query results into a 2-dim array.
        if len(label_query_results[0]) != len(metric_query_results[0]):
            logging.error('Cannot fetch the metric %s because the'
                          'dimension between label and metric is not consistent.')
            return

        def _get_or_create(list_object, index):
            if index < len(list_object):
                return list_object[index]
            new_dict = dict()
            list_object.append(new_dict)
            return new_dict

        def construct_labels():
            r = []
            for label_idx, name in enumerate(self.label_names):
                for row_idx, v in enumerate(label_query_results[label_idx]):
                    d = _get_or_create(r, row_idx)
                    d[name] = v
            return r

        def construct_metric_values():
            r = []
            for metric_idx, metric_ in enumerate(self.metrics):
                for row_idx, v in enumerate(metric_query_results[metric_idx]):
                    d = _get_or_create(r, row_idx)
                    d[metric_.name] = v
            return r

        def set_metric_value(family_, value_):
            # None is equivalent to NaN instead of zero.
            if len(value_) == 0:
                logging.warning(
                    'Not found field %s in the %s.', metric.name, self.name
                )
            else:
                value_ = cast_to_int_or_float(value_)
                # Different usages (Prometheus data type) have different setting methods.
                # Thus, we have to select to different if-branches according to metric's usage.
                if metric.usage == 'COUNTER':
                    family_.set(value_)
                elif metric.usage == 'GAUGE':
                    family_.set(value_)
                elif metric.usage == 'SUMMARY':
                    family_.observe(value_)
                elif metric.usage == 'HISTOGRAM':
                    family_.observe(value_)
                else:
                    logging.error(
                        'Not supported metric %s due to usage %s.' % (metric.name, metric.usage)
                    )

        labels_ = construct_labels()
        values_ = construct_metric_values()
        for l_, v_ in zip(labels_, values_):
            for m in self.metrics:
                metric_family = m.entity.labels(**l_)
                metric_value = v_[m.name]
                set_metric_value(metric_family, metric_value)


def config_collecting_params(parallel, constant_labels):
    global _thread_pool_executor

    _thread_pool_executor = ThreadPoolExecutor(max_workers=parallel)
    # Append extra labels, including essential labels (e.g., from_server)
    # and constant labels from user's configurations.
    global_labels.update(constant_labels)
    logging.info(
        'Perform shell commands with %d threads, extra labels: %s.',
        parallel, constant_labels
    )


def register_metrics(parsed_yml):
    dbmind_assert(isinstance(parsed_yml, dict))

    for name, raw_query_instance in parsed_yml.items():
        dbmind_assert(isinstance(raw_query_instance, dict))
        instance = QueryInstance(name, raw_query_instance)
        instance.attach(_registry)
        query_instances.append(instance)


def query_all_metrics():
    futures = []
    for instance in query_instances:
        futures.append(_thread_pool_executor.submit(instance.update))

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logging.exception(e)

    return generate_latest(_registry)
