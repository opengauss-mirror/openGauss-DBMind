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

import inspect
from datetime import datetime, timedelta
from itertools import product
from typing import Optional, Iterable, Union

from dbmind.common.algorithm import anomaly_detection
from dbmind.common.algorithm.anomaly_detection.agg import merge_with_and_operator
from dbmind.common.algorithm.forecasting.forecasting_algorithm import quickly_forecast
from dbmind.common.algorithm.stat_utils import approximatively_merge as merge
from dbmind.common.types import Alarm
from dbmind.common.types.enums import ALARM_LEVEL, ALARM_TYPES
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils


def find_anomaly_detector(detector_name):
    detectors = anomaly_detection.detectors
    if detector_name not in detectors:
        raise KeyError(f"Detector name: {detector_name} was not found.")

    return detectors[detector_name]


def get_monitoring_alarm_args():
    return set(inspect.getfullargspec(AlarmInfo.__init__).args[1:])


def get_monitoring_detector_args():
    return set(inspect.getfullargspec(DetectorInfo.__init__).args[1:])


class AlarmInfo:
    def __init__(self,
                 alarm_content: Optional[str] = None,
                 alarm_type: Optional[str] = ALARM_TYPES.ALARM,
                 alarm_level: Optional[str] = ALARM_LEVEL.ERROR.name,
                 alarm_cause: Optional[str] = None,
                 extra: Optional[str] = None):
        self.alarm_content = alarm_content if alarm_content else ""
        self.alarm_type = alarm_type
        self.alarm_level = alarm_level
        self.alarm_cause = alarm_cause
        self.extra = extra

    def to_dict(self):
        return dict(
            alarm_content=self.alarm_content,
            alarm_type=self.alarm_type,
            alarm_level=self.alarm_level,
            alarm_cause=self.alarm_cause,
            extra=self.extra
        )


class DetectorInfo:
    def __init__(self,
                 metric_name: str,
                 detector_name: str,
                 metric_filter: Optional[dict] = None,
                 detector_kwargs: Optional[dict] = None
                 ):
        self.metric_name = metric_name
        self.metric_filter = metric_filter if metric_filter else {}
        self.detector_name = detector_name
        self.detector_kwargs = detector_kwargs
        kwargs = detector_kwargs if detector_kwargs else {}
        self.detector = find_anomaly_detector(detector_name)(**kwargs)

    def to_dict(self):
        return dict(
            metric_name=self.metric_name,
            detector_name=self.detector_name,
            metric_filter=self.metric_filter,
            detector_kwargs=self.detector_kwargs,
        )


class GenericAnomalyDetector:
    def __init__(self,
                 name: str,
                 duration: int,
                 forecasting_seconds: int,
                 alarm_info: AlarmInfo,
                 detector_info: Union[DetectorInfo, Iterable[DetectorInfo]],
                 fit_once=False):
        self.name = name
        self.duration = duration
        self.forecasting_seconds = forecasting_seconds
        alarm_info.alarm_content = name + ": " + alarm_info.alarm_content
        alarm_info.alarm_level = ALARM_LEVEL[alarm_info.alarm_level]
        self.alarm_info = alarm_info
        self.detector_info = detector_info if isinstance(detector_info, Iterable) else [detector_info]
        self.anomaly_types = [di.detector.__class__.__name__ for di in self.detector_info]
        if forecasting_seconds and fit_once:
            self.models = [None for _ in self.detector_info]
            self.fit_once = fit_once

    def detect(self):
        main_metric_name = self.detector_info[0].metric_name
        main_metric_filter = self.detector_info[0].metric_filter
        end_datetime = datetime.now()
        start_datetime = end_datetime - timedelta(seconds=self.duration)  # unit: second
        main_sequences = dai.get_metric_sequence(
            main_metric_name,
            start_datetime,
            end_datetime
        ).filter(**main_metric_filter).fetchall()
        alarms = []
        for main_sequence in main_sequences:
            sequences_list = [[main_sequence]]
            instance = SequenceUtils.from_server(main_sequence)
            if ":" in instance:
                port = instance.split(":")[1]
                instance_like = instance + f"(:{port}|)"
            else:
                instance_like = instance + "(:[0-9]{4,5}|)"

            for i, di in enumerate(self.detector_info[1:]):
                metric_name = di.metric_name
                metric_filter = di.metric_filter

                fetcher = dai.get_metric_sequence(
                    metric_name, start_datetime, end_datetime
                ).filter(**metric_filter)

                instance_label = dai._get_data_source_flag(metric_name)
                if instance_label not in metric_filter:  # In case of duplication
                    fetcher = fetcher.from_server_like(instance_like)

                sequences = fetcher.fetchall()
                sequences_list.append(sequences)

            for sequence_set in product(*sequences_list):
                anomalies = []
                for i, sequence in enumerate(sequence_set):
                    detector = self.detector_info[i].detector
                    if self.forecasting_seconds:
                        forecast_seq, model = quickly_forecast(sequence,
                                                               self.forecasting_seconds / 60,
                                                               given_model=self.models[i],
                                                               return_model=True)
                        if self.fit_once:
                            self.models[i] = model

                        anomalies.append(detector.fit_predict(merge(sequence, forecast_seq)))
                    else:
                        anomalies.append(detector.fit_predict(sequence))

                result = merge_with_and_operator(anomalies)
                if True in result.values:
                    alarm = Alarm(**self.alarm_info.to_dict())
                    alarm.instance = instance
                    alarm.metric_name = main_metric_name
                    alarm.metric_filter = ",".join([
                        f"{k}={v}" for k, v in main_metric_filter.items()
                    ])
                    alarm.start_timestamp = result.timestamps[result.values.index(True)]
                    alarm.end_timestamp = result.timestamps[-result.values[::-1].index(True) - 1]
                    alarm.anomaly_type = " ".join(self.anomaly_types)
                    alarms.append(alarm)

        return alarms

    def __repr__(self):
        return (
            f"GenericAnomalyDetector:(name: {self.name}, duration: {self.duration}, "
            f"forecasting_seconds: {self.forecasting_seconds}, "
            f"alarm_info: {self.alarm_info.to_dict()}, "
            f"detector_info: {', '.join([str(di.to_dict()) for di in self.detector_info])})"
        )
