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

from .enums import ALARM_LEVEL, ALARM_TYPES


class Alarm:
    def __init__(
            self,
            instance: str = None,
            metric_name: str = None,
            metric_filter: dict = None,
            alarm_type: ALARM_TYPES = ALARM_TYPES.SYSTEM,
            alarm_level: ALARM_LEVEL = ALARM_LEVEL.ERROR,
            start_timestamp: int = None,
            end_timestamp: int = None,
            alarm_content: str = None,
            extra: str = None,
            anomaly_type: str = None,
            alarm_cause: str = None,
    ):
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
        self.alarm_cause = alarm_cause

    def set_timestamp(self, start, end):
        self.start_timestamp = start
        self.end_timestamp = end
        return self

    def __repr__(self):
        return '[%s](%s)' % (self.alarm_content, self.alarm_cause)
