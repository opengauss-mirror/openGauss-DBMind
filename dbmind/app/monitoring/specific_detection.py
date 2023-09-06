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

from dbmind.app import monitoring
from dbmind.common.types import ALARM_LEVEL, ALARM_TYPES


class SpecificDetection:
    try:
        detection_window_seconds = monitoring.cast_to_int_or_float(
            monitoring.get_dynamic_param("self_monitoring", "detection_window_seconds")
        )
        forecasting_seconds = monitoring.cast_to_int_or_float(
            monitoring.get_dynamic_param("self_monitoring", "forecasting_seconds")
        )
        disk_usage_threshold = monitoring.get_detection_threshold('disk_usage_threshold')
        mem_usage_threshold = monitoring.get_detection_threshold('mem_usage_threshold')
        mem_high_usage_percent = monitoring.get_detection_threshold('mem_high_usage_percent')
        cpu_usage_threshold = monitoring.get_detection_threshold('cpu_usage_threshold')
        cpu_high_usage_percent = monitoring.get_detection_threshold('cpu_high_usage_percent')
        spike_outliers_1 = monitoring.get_detection_param("spike_outliers_1")
        spike_outliers_2 = monitoring.get_detection_param("spike_outliers_2")
    except AttributeError:
        detection_window_seconds = 600
        forecasting_seconds = 0
        disk_usage_threshold = 0.8
        mem_usage_threshold = 0.8
        mem_high_usage_percent = 0.8
        cpu_usage_threshold = 0.8
        cpu_high_usage_percent = 0.8
        spike_outliers_1 = None
        spike_outliers_2 = 3

    detections = {
        "high_disk_usage_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": forecasting_seconds,
            "alarm_info": {
                "alarm_content": (
                    f"The disk usage has exceeded the warning level: "
                    f"{disk_usage_threshold * 100}%"
                ),
                "alarm_type": ALARM_TYPES.SYSTEM,
                "alarm_level": ALARM_LEVEL.WARNING.name,
            },
            "detector_info": [
                {
                    "metric_name": "os_disk_usage",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": disk_usage_threshold
                    },
                },
            ]
        },
        "high_mem_usage_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": forecasting_seconds,
            "alarm_info": {
                "alarm_content": (
                    f"The memory usage has exceeded the warning level: "
                    f"{mem_usage_threshold * 100}%"
                ),
                "alarm_type": ALARM_TYPES.SYSTEM,
                "alarm_level": ALARM_LEVEL.WARNING.name,
            },
            "detector_info": [
                {
                    "metric_name": "os_mem_usage",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": mem_usage_threshold,
                        "percentage": mem_high_usage_percent,
                    },
                },
            ]
        },
        "high_cpu_usage_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": forecasting_seconds,
            "alarm_info": {
                "alarm_content": (
                    f"The cpu usage has exceeded the warning level: "
                    f"{cpu_usage_threshold * 100}%"
                ),
                "alarm_type": ALARM_TYPES.SYSTEM,
                "alarm_level": ALARM_LEVEL.WARNING.name,
            },
            "detector_info": [
                {
                    "metric_name": "os_cpu_user_usage",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": cpu_usage_threshold,
                        "percentage": cpu_high_usage_percent,
                    },
                },
            ]
        },
        "mem_usage_spike_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "Find obvious spikes in memory usage.",
                "alarm_type": ALARM_TYPES.SYSTEM,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "os_mem_usage",
                    "detector_name": "SpikeDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "outliers": (spike_outliers_1, spike_outliers_2)
                    },
                },
                {
                    "metric_name": "os_mem_usage",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": mem_usage_threshold,
                    },
                },
            ]
        },
        "qps_spike_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "Find obvious spikes in QPS.",
                "alarm_type": ALARM_TYPES.SYSTEM,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "gaussdb_qps_by_instance",
                    "detector_name": "SpikeDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "outliers": (spike_outliers_1, spike_outliers_2)
                    },
                },
            ]
        }
    }
