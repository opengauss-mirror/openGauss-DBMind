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
import abc

from prometheus_client import Gauge, Summary, Histogram, Info, Enum

from dbmind.common.utils import dbmind_assert


class QueryInterface:
    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def attach(self, registry):
        pass


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


class Metric:
    """Metric family structure:
    Only parsing the metric dict and
    lazy loading the Prometheus metric object."""

    def __init__(self, name, item):
        self.name = name
        if 'description' in item:
            self.desc = item.pop('description')
        else:
            self.desc = ''
        self.usage = item.pop('usage').upper()

        # additional and optional fields
        self._additional_fields = item

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

    @property
    def prefix(self):
        return self._prefix

    def activate(self, labels=(), global_labels=None):
        if global_labels is None:
            global_labels = dict()

        dbmind_assert(not self.is_label and self._prefix)

        self._value = PROMETHEUS_TYPES[self.usage](
            '%s_%s' % (self._prefix, self.name), self.desc, labels
        )
        # for cumulative counters start the value zero
        if self.usage == 'COUNTER' and global_labels:
            self._value.labels(**global_labels).set(0)

    @property
    def entity(self):
        dbmind_assert(self._value, "Should be activated first.")
        return self._value

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            return self._additional_fields.get(item, None)


class CumulativeMetric:
    """Helper class to easily get and set the cumulative value of metric that should work like counter"""

    def __init__(self, name):
        """Init CumulativeMetric with 2 empty lists - 1 for labels and 1 for values"""
        self.name = name
        self.labels = []
        self.values = []

    def get(self, labels: dict) -> int:
        """Get cumulative value for labels"""
        dbmind_assert(len(self.labels) == len(self.values))
        for i in range(len(self.labels)):
            if self.labels[i] == labels:
                return self.values[i]
        return 0

    def set(self, value: int, labels: dict) -> None:
        """Set cumulative value for labels, added to current value if exist"""
        try:
            ind = self.labels.index(labels)
            self.values[ind] += value
        except ValueError:
            dbmind_assert(len(self.labels) == len(self.values))
            self.labels.append(labels)
            self.values.append(value)

    def __iter__(self):
        """Iterate over CumulativeMetric should return labels, value"""
        dbmind_assert(len(self.labels) == len(self.values))
        for i in range(len(self.labels)):
            yield self.labels[i], self.values[i]

    def __str__(self):
        """String representation of CumulativeMetric"""
        return ','.join(str(x) for x in zip(self.labels, self.values))
