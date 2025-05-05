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
from datetime import datetime, timedelta
from itertools import product
from typing import Iterable, Union

from dbmind.common.algorithm.anomaly_detection import *
from dbmind.common.algorithm.anomaly_detection.agg import merge_with_and_operator
from dbmind.common.algorithm.forecasting.forecasting_algorithm import quickly_forecast
from dbmind.common.algorithm.stat_utils import approximatively_merge as merge
from dbmind.common.types import Alarm
from dbmind.common.types.enums import ALARM_LEVEL
from dbmind.common.utils.checking import prepare_ip, split_ip_port, WITH_PORT
from dbmind.constants import PORT_SUFFIX
from dbmind.service import dai

from .monitoring_constants import (
    ONE_WEEK_IN_SECONDS,
    ONE_HOUR_IN_SECONDS,
    AlarmInfo,
    DetectorInfo
)

UNSTABLE_METRICS = (
    "pg_shared_memory_detail_size",
    "pg_session_memory_detail_size"
)

STATISTICS_DETECTORS = (
    IncreaseDetector,
    InterQuartileRangeDetector,
    GradientDetector,
    LevelShiftDetector,
    SeasonalDetector,
    SpikeDetector,
    VolatilityShiftDetector,
    QuantileDetector,
    EsdTestDetector,
    ForecastingAnomalyDetector
)


def check_long_term_sequences(sequences, datetime_now):
    idx = 0
    while idx < len(sequences):
        seq = sequences[idx]
        # If the length of the sequence is not longer than one week as planned
        # or sequence is not updated in one hour, pop it.
        if (
            len(seq.values) * seq.step // 1000 <= ONE_WEEK_IN_SECONDS or
            datetime_now.timestamp() - (seq.timestamps[-1] + seq.step) // 1000 > ONE_HOUR_IN_SECONDS
        ):
            sequences.pop(idx)
            continue

        idx += 1


def get_extra_info(metric_name, labels):
    if metric_name == "core_detector" and "unique_sql_id" in labels:
        unique_sql_id = labels.get('unique_sql_id')
        if (not isinstance(unique_sql_id, str)) or unique_sql_id == "0":
            return None
        return (
            "You may use this SQL to avoid core dump: "
            f"select * from dbe_sql_util.create_abort_sql_patch('avoid_core', {unique_sql_id})"
        )
    elif metric_name == "deadlock_detector":
        return labels.get('content')
    else:
        return None


def get_meta_metric_sequence(metric_name, metric_filter, metric_filter_like,
                             long_term_metrics):
    result = list()
    for seq in long_term_metrics:
        name = seq.name
        labels = seq.labels
        if (
            metric_name != name or
            not dai.labels_matched(labels, metric_filter, metric_filter_like)
        ):
            continue

        result.append(seq)

    return result


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
        alarm_info.alarm_level = ALARM_LEVEL[alarm_info.alarm_level]
        self.alarm_info = alarm_info
        self.detector_info = detector_info if isinstance(detector_info, Iterable) else [detector_info]
        self.fit_once = fit_once
        if forecasting_seconds:
            self.models = ['singleton' for _ in self.detector_info]

    def detect(self, long_term_metrics):
        if long_term_metrics is None:
            long_term_metrics = []

        alarms = []
        end_datetime = datetime.now()
        start_datetime = end_datetime - timedelta(seconds=self.duration)  # unit: second
        main_metric_name = self.detector_info[0].metric_name
        main_metric_filter = self.detector_info[0].metric_filter.copy()
        main_source_flag = dai.get_metric_source_flag(main_metric_name)
        main_metric_filter_like = dict()
        short_term_detection = self.duration != 0
        if PORT_SUFFIX in main_metric_filter.get("from_instance", ""):
            main_metric_filter_like[main_source_flag] = main_metric_filter.pop("from_instance")

        if short_term_detection:
            main_sequences = dai.get_metric_sequence(
                main_metric_name,
                start_datetime,
                end_datetime
            ).filter_like(**main_metric_filter_like).filter(**main_metric_filter).fetchall()
        else:
            main_sequences = get_meta_metric_sequence(
                main_metric_name,
                main_metric_filter,
                main_metric_filter_like,
                long_term_metrics
            )
            check_long_term_sequences(main_sequences, end_datetime)

        for main_sequence in main_sequences:
            sequences_list = [[main_sequence]]
            main_sequence_labels = main_sequence.labels.copy()
            instance = main_sequence_labels.get(main_source_flag)
            if WITH_PORT.match(instance):
                host, port = split_ip_port(instance)
                instance_like = f"{prepare_ip(host)}(:{port})|{host}"
            elif instance:
                instance_like = f"{prepare_ip(instance)}{PORT_SUFFIX}|{instance}"
            else:
                instance_like = None

            for i, di in enumerate(self.detector_info[1:]):
                metric_name = di.metric_name
                metric_filter = di.metric_filter.copy()
                for name, value in metric_filter.items():
                    if value == "":
                        metric_filter[name] = main_sequence_labels[name]

                source_flag = dai.get_metric_source_flag(metric_name)
                metric_filter_like = dict()
                if source_flag not in metric_filter and instance_like:  # In case of user definition
                    metric_filter_like[source_flag] = instance_like

                if short_term_detection:
                    sequences = dai.get_metric_sequence(
                        metric_name,
                        start_datetime,
                        end_datetime
                    ).filter_like(**metric_filter_like).filter(**metric_filter).fetchall()
                else:
                    sequences = get_meta_metric_sequence(
                        metric_name,
                        metric_filter,
                        metric_filter_like,
                        long_term_metrics
                    )
                    check_long_term_sequences(sequences, end_datetime)

                sequences_list.append(sequences)

            for sequence_set in product(*sequences_list):
                anomalies = []
                for i, sequence in enumerate(sequence_set):
                    detector = self.detector_info[i].detector
                    if (
                        self.duration and
                        (
                            isinstance(detector, STATISTICS_DETECTORS) or
                            (
                                isinstance(detector, ThresholdDetector) and
                                detector.percentage is not None and
                                detector.percentage > 0
                            )
                        )
                    ):
                        detector.least_length = self.duration * 0.8 * 1000 // sequence.step

                    if self.forecasting_seconds:
                        if not short_term_detection:
                            sequence_duration_in_seconds = len(sequence) * sequence.step // 1000
                            if sequence_duration_in_seconds < self.forecasting_seconds:
                                logging.warning(
                                    "The forecasting result of future anomalies for metric %s with labels %s "
                                    "could be very unreliable because the forecasting duration %s seconds is "
                                    "longer than the source data duration %s seconds, please modify "
                                    "forecasting_seconds of the detector %s.",
                                    sequence.name, sequence.labels, self.forecasting_seconds,
                                    sequence_duration_in_seconds, self.name
                                )
                        forecast_seq, model = quickly_forecast(sequence,
                                                               self.forecasting_seconds / 60,
                                                               given_model=self.models[i],
                                                               return_model=True)
                        if self.fit_once:
                            self.models[i] = model

                        anomalies.append(detector.fit_predict(merge(sequence, forecast_seq)))
                    else:
                        anomalies.append(detector.fit_predict(sequence))

                try:
                    result = merge_with_and_operator(anomalies)
                except ValueError:
                    if not any([detector.metric_name in UNSTABLE_METRICS for detector in self.detector_info]):
                        logging.warning("Anomalies length mismatched: %s.", self.name)
                    continue

                if True in result.values:
                    self.alarm_info.extra = get_extra_info(self.name, main_sequence_labels)
                    alarm = Alarm(**self.alarm_info.to_dict())
                    alarm.instance = split_ip_port(instance)[0]
                    alarm.metric_name = (main_metric_name if short_term_detection
                                         else main_metric_name.rsplit("_", 1)[0])
                    alarm.metric_filter = main_sequence_labels
                    alarm.start_timestamp = result.timestamps[result.values.index(True)]
                    alarm.end_timestamp = result.timestamps[-result.values[::-1].index(True) - 1]
                    alarm.anomaly_type = self.name
                    alarms.append(alarm)

        return alarms

    def __repr__(self):
        return (
            f"GenericAnomalyDetector:(name: {self.name}, duration: {self.duration}, "
            f"forecasting_seconds: {self.forecasting_seconds}, "
            f"alarm_info: {self.alarm_info.to_dict()}, "
            f"detector_info: {', '.join([str(di.to_dict()) for di in self.detector_info])})"
        )
