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
from typing import List

from dbmind.app import monitoring
from dbmind.common.types import Alarm, ALARM_LEVEL, ALARM_TYPES, ANOMALY_TYPES
from dbmind.common.types.sequence import EMPTY_SEQUENCE
from dbmind.common.types.sequence import Sequence
from dbmind.service.utils import SequenceUtils
from .generic_detection import AnomalyDetections

_rule_mapper = {}
_rules_for_history = set()


def _check_for_metric(metrics, only_history=True):
    def decorator(func):
        if only_history:
            _rules_for_history.add(metrics)
        _rule_mapper[metrics] = func

    return decorator


def _dummy(*args, **kwargs):
    return []


def detect(
        instance, metrics,
        latest_sequences, future_sequences=tuple()
) -> List[Alarm]:
    func = _rule_mapper.get(metrics, _dummy)

    if not future_sequences:
        for latest_sequence in latest_sequences:
            future_sequences += (EMPTY_SEQUENCE,)
            metric = latest_sequence.name
            logging.warning('Forecast future sequences %s at %s is None.', metric, instance)

    alarms = func(latest_sequences, future_sequences)
    for alarm in alarms:
        alarm.instance = instance
    return alarms


def approximatively_merge(s1, s2):
    if not s2:
        return s1
    s1_end = s1.timestamps[-1]
    s2_start = s2.timestamps[0]
    delta = s2_start - s1_end
    if delta % s1.step == 0:
        return s1 + s2
    new_s2 = Sequence(
        name=s2.name,
        labels=s2.labels,
        step=s2.step,
        timestamps=tuple(map(lambda s: s - delta, s2.timestamps)),
        values=s2.values,
    )
    return s1 + new_s2


"""Add checking rules below."""


@_check_for_metric(('os_disk_usage',), only_history=True)
def will_disk_spill(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)
    disk_usage_threshold = monitoring.get_param('disk_usage_threshold')

    disk_device = latest_sequence.labels.get('device', 'unknown')
    disk_mountpoint = latest_sequence.labels.get('mountpoint', 'unknown')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=disk_usage_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                instance=SequenceUtils.from_server(latest_sequence),
                alarm_content='The disk usage has exceeded the warning level: %s%%(device: %s, mountpoint: %s).' % (
                    disk_usage_threshold * 100,
                    disk_device,
                    disk_mountpoint
                ),
                alarm_type=ALARM_TYPES.SYSTEM,
                metric_name='os_disk_usage',
                start_timestamp=full_sequence.timestamps[0],
                end_timestamp=full_sequence.timestamps[-1],
                alarm_level=ALARM_LEVEL.WARNING,
                anomaly_type=ANOMALY_TYPES.THRESHOLD
            )
        )

    return alarms


def _add_anomalies_values_2_msg(anomalies):
    anomaly_values = []
    for anomaly in anomalies:
        anomaly_values += list(anomaly.values)
    return " Abnormal value(s) are " + ",".join(str(item) for item in anomaly_values)


def has_mem_leak(latest_sequences, future_sequences, metric_name=''):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)
    mem_usage_threshold = monitoring.get_param('mem_usage_threshold')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=mem_usage_threshold
    )
    spike_threshold_anomalies = AnomalyDetections.do_spike_detect(full_sequence)
    level_shift_anomalies = AnomalyDetections.do_level_shift_detect(full_sequence)

    # Polish later: the time window needs to be longer
    increase_anomalies = AnomalyDetections.do_increase_detect(full_sequence, side="positive")
    alarms = []
    if True in over_threshold_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="The memory usage has exceeded the warning level: %s%%." % (mem_usage_threshold * 100),
            alarm_type=ALARM_TYPES.ALARM,
            metric_name=metric_name,
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.THRESHOLD
        )
        alarms.append(alarm)
    if True in spike_threshold_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find obvious spikes in memory usage.",
            alarm_type=ALARM_TYPES.SYSTEM,
            metric_name=metric_name,
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.SPIKE
        )
        alarms.append(alarm)
    if True in level_shift_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find obvious level-shift in memory usage.",
            alarm_type=ALARM_TYPES.SYSTEM,
            metric_name=metric_name,
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.LEVEL_SHIFT
        )
        alarms.append(alarm)
    if True in increase_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find continued growth in memory usage.",
            alarm_type=ALARM_TYPES.SYSTEM,
            metric_name=metric_name,
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.INCREASE
        )
        alarms.append(alarm)

    return alarms


@_check_for_metric(('os_mem_usage',), only_history=True)
def os_has_mem_leak(latest_sequences, future_sequences):
    return has_mem_leak(latest_sequences, future_sequences, metric_name='os_mem_usage')


@_check_for_metric(('os_cpu_usage',), only_history=True)
def has_cpu_high_usage(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)
    cpu_usage_threshold = monitoring.get_param('cpu_usage_threshold')
    cpu_high_usage_percent = monitoring.get_param('cpu_high_usage_percent')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=cpu_usage_threshold
    )
    level_shift_anomalies = AnomalyDetections.do_level_shift_detect(full_sequence)
    alarms = []
    if over_threshold_anomalies.values.count(True) > cpu_high_usage_percent * len(full_sequence):
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content='The cpu usage has exceeded the warning level '
                          '%s%% of total for over %s%% of last detection period.' % (
                              cpu_usage_threshold * 100, cpu_high_usage_percent * 100),
            alarm_type=ALARM_TYPES.SYSTEM,
            metric_name='os_cpu_usage',
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.ERROR,
            anomaly_type=ANOMALY_TYPES.THRESHOLD
        )
        alarms.append(alarm)
    if True in level_shift_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find obvious level-shift in cpu usage.",
            alarm_type=ALARM_TYPES.SYSTEM,
            metric_name='os_cpu_usage',
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.LEVEL_SHIFT
        )
        alarms.append(alarm)
    return alarms


@_check_for_metric(('gaussdb_qps_by_instance',), only_history=True)
def has_qps_rapid_change(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)
    spike_threshold_anomalies = AnomalyDetections.do_spike_detect(full_sequence)

    alarms = []
    if True in spike_threshold_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find obvious spikes in QPS.",
            alarm_type=ALARM_TYPES.PERFORMANCE,
            metric_name='gaussdb_qps_by_instance',
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.SPIKE
        )
        alarms.append(alarm)
    return alarms


@_check_for_metric(('gaussdb_connections_used_ratio',), only_history=True)
def has_connections_high_occupation(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)
    connection_usage_threshold = monitoring.get_param('connection_usage_threshold')
    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=connection_usage_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                instance=SequenceUtils.from_server(latest_sequence),
                alarm_content='The connection usage has exceeded the warning level: %s%%.' % (
                        connection_usage_threshold * 100
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='gaussdb_connections_used_ratio',
                start_timestamp=full_sequence.timestamps[0],
                end_timestamp=full_sequence.timestamps[-1],
                alarm_level=ALARM_LEVEL.ERROR,
                anomaly_type=ANOMALY_TYPES.THRESHOLD
            )
        )
    return alarms


@_check_for_metric(('statement_responsetime_percentile_p95',), only_history=True)
def has_p95_rapid_change(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)
    spike_threshold_anomalies = AnomalyDetections.do_spike_detect(full_sequence)

    alarms = []
    if True in spike_threshold_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find obvious spikes in P95.",
            alarm_type=ALARM_TYPES.PERFORMANCE,
            metric_name='statement_responsetime_percentile_p95',
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.SPIKE
        )
        alarms.append(alarm)
    return alarms


@_check_for_metric(('os_disk_ioutils',), only_history=True)
def has_high_disk_ioutils(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)

    disk_usage_threshold = monitoring.get_param('disk_ioutils_threshold')
    disk_device = latest_sequence.labels.get('device', 'unknown')
    disk_mountpoint = latest_sequence.labels.get('mountpoint', 'unknown')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=disk_usage_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                instance=SequenceUtils.from_server(latest_sequence),
                alarm_content='The IOUtils has exceeded the warning level: %s%%(device: %s, mountpoint: %s).' % (
                    disk_usage_threshold * 100,
                    disk_device,
                    disk_mountpoint
                ),
                alarm_type=ALARM_TYPES.SYSTEM,
                metric_name='os_disk_ioutils',
                start_timestamp=full_sequence.timestamps[0],
                end_timestamp=full_sequence.timestamps[-1],
                alarm_level=ALARM_LEVEL.WARNING,
                anomaly_type=ANOMALY_TYPES.THRESHOLD
            )
        )

    return alarms


@_check_for_metric(('os_network_receive_drop',), only_history=True)
def has_p95_rapid_change(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = approximatively_merge(latest_sequence, future_sequence)

    spike_threshold_anomalies = AnomalyDetections.do_spike_detect(full_sequence)

    alarms = []
    if True in spike_threshold_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find obvious spikes in os_network_receive_drop.",
            alarm_type=ALARM_TYPES.PERFORMANCE,
            metric_name='os_network_receive_drop',
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.SPIKE
        )
        alarms.append(alarm)
    return alarms


@_check_for_metric(('os_network_transmit_drop',), only_history=True)
def has_p95_rapid_change(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]

    full_sequence = approximatively_merge(latest_sequence, future_sequence)

    spike_threshold_anomalies = AnomalyDetections.do_spike_detect(full_sequence)

    alarms = []
    if True in spike_threshold_anomalies.values:
        alarm = Alarm(
            instance=SequenceUtils.from_server(latest_sequence),
            alarm_content="Find obvious spikes in os_network_transmit_drop.",
            alarm_type=ALARM_TYPES.PERFORMANCE,
            metric_name='os_network_transmit_drop',
            start_timestamp=full_sequence.timestamps[0],
            end_timestamp=full_sequence.timestamps[-1],
            alarm_level=ALARM_LEVEL.WARNING,
            anomaly_type=ANOMALY_TYPES.SPIKE
        )
        alarms.append(alarm)
    return alarms
