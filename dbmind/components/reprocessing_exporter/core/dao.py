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
import time

from prometheus_client import Gauge

from dbmind.common.tsdb.prometheus_client import PrometheusClient

_prometheus_client: 'PrometheusClient' = None


class MetricConfig:
    def __init__(self, name, promql, desc, ttl=None, timeout=None, registry=None):
        self.name = name
        self.desc = desc
        self.promql = promql
        self.ttl = ttl
        self.timeout = timeout
        self._expired_time = 0
        self._labels = []
        self._label_map = dict()
        self._registry = registry
        self._gauge = None
        self._cached_result = None

    def add_label(self, sequence_label, metric_label):
        """Construct a map that can help to make an association between
        Prometheus result and exporter result."""
        self._labels.append(metric_label)
        self._label_map[sequence_label] = metric_label

    def get_label_name(self, sequence_label):
        return self._label_map.get(sequence_label)

    def query(self):
        if self.ttl and time.time() < self._expired_time:
            self._expired_time = time.time() + self.ttl
            return self._cached_result
        self._cached_result = query(self.promql, timeout=self.timeout)
        return self._cached_result

    def __repr__(self):
        return repr((self.name, self.promql, self.labels))

    @property
    def labels(self):
        return self._labels

    @property
    def gauge(self):
        if not self._gauge:
            self._gauge = Gauge(
                self.name, self.desc, self._labels, registry=self._registry
            )
        return self._gauge


def set_prometheus_client(url, username, password):
    global _prometheus_client

    client = PrometheusClient(
        url, username=username, password=password
    )
    if not client.check_connection():
        raise ConnectionRefusedError("Failed to connect to the TSDB url: %s" % url)

    _prometheus_client = client


def query(promql, timeout=None):
    return _prometheus_client.custom_query(
        promql, timeout=timeout
    )
