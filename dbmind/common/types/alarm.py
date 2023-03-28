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
from typing import Optional, Iterable, Union

from .enums import ALARM_LEVEL, ALARM_TYPES
from .root_cause import RootCause


class Alarm:
    def __init__(self,
                 instance: Optional[str] = None,
                 metric_name: str = None,
                 metric_filter: dict = None,
                 alarm_type: ALARM_TYPES = ALARM_TYPES.SYSTEM,
                 alarm_level: ALARM_LEVEL = ALARM_LEVEL.ERROR,
                 start_timestamp=None,
                 end_timestamp=None,
                 alarm_content: str = None,
                 extra=None,
                 anomaly_type=None,
                 alarm_cause: Optional[Union[RootCause, Iterable[RootCause]]] = None):
        self.instance = instance
        self.metric_name = metric_name
        self.metric_filter = metric_filter
        self.alarm_type = alarm_type
        self.alarm_level = alarm_level
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.alarm_content = alarm_content
        self.extra = extra
        self.anomaly_type = anomaly_type

        if isinstance(alarm_cause, Iterable):
            self.alarm_cause = list(alarm_cause)
        elif isinstance(alarm_cause, RootCause):
            self.alarm_cause = [alarm_cause]
        else:
            self.alarm_cause = list()

    def add_reason(self, root_cause):
        self.alarm_cause.append(root_cause)
        return self

    def set_timestamp(self, start, end=None):
        self.start_timestamp = start
        self.end_timestamp = end
        return self

    def __repr__(self):
        return '[%s](%s)' % (
            self.alarm_content, self.alarm_cause
        )

    @property
    def root_causes(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s: (%.2f) %s' % (index, c.title, c.probability, c.detail)
            )
            index += 1
        return '\n'.join(lines)

    @property
    def suggestions(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s' % (index, c.suggestion if c.suggestion else 'No suggestions.')
            )
            index += 1
        return '\n'.join(lines)
