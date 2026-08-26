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

import json

from dbmind.app.monitoring import (
    get_detection_threshold,
    get_detection_param,
    get_self_monitoring
)
from dbmind.app.monitoring.monitoring_constants import LONG_TERM_METRIC_STATS
from dbmind.common.types import ALARM_LEVEL, ALARM_TYPES


class SpecificDetection:
    # thresholds
    detection_window_seconds = get_self_monitoring("detection_window_seconds", default=600)
    disk_usage_threshold = get_detection_threshold('disk_usage_threshold', default=0.8)
    mem_usage_threshold = get_detection_threshold('mem_usage_threshold', default=0.8)
    mem_high_usage_percent = get_detection_threshold('mem_high_usage_percent', default=0.8)
    cpu_usage_threshold = get_detection_threshold('cpu_usage_threshold', default=0.8)
    cpu_high_usage_percent = get_detection_threshold('cpu_high_usage_percent', default=0.8)
    thread_pool_usage_threshold = get_detection_threshold('thread_pool_usage_threshold', default=0.95)
    disk_await_threshold = get_detection_threshold("disk_await_threshold", default=30)
    leaked_fds_threshold = get_detection_threshold("leaked_fds_threshold", default=5)
    connection_rate_threshold = get_detection_threshold("connection_rate_threshold", default=0.95)
    disk_ioutils_threshold = get_detection_threshold("disk_ioutils_threshold", default=0.99)
    ping_lag_threshold = get_detection_threshold("ping_lag_threshold", default=50)
    ping_packet_rate_threshold = get_detection_threshold("ping_packet_rate_threshold", default=0.9)
    significance_threshold = get_detection_threshold("significance_threshold", default=0.05)
    long_transaction_threshold = get_detection_threshold("long_transaction_threshold", default=3600)

    # params
    spike_outliers_1 = get_detection_param("spike_outliers_1", default=None)
    spike_outliers_2 = get_detection_param("spike_outliers_2", default=3)
    level_shift_window = get_detection_param("level_shift_window", default=20)
    level_shift_outliers_1 = get_detection_param("level_shift_outliers_1", default=None)
    level_shift_outliers_2 = get_detection_param("level_shift_outliers_2", default=3)

    detections = {
        # disk usage
        "high_disk_usage_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "high_disk_usage_detector: The disk usage has exceeded "
                    "the warning level."
                ),
                "alarm_type": ALARM_TYPES.RESOURCE,
                "alarm_level": ALARM_LEVEL.WARNING.name,
                "alarm_cause": json.dumps([
                    "high_disk_usage",
                ]),
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
        "high_db_disk_usage_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "high_db_disk_usage_detector: The DB disk usage has exceeded "
                    "the warning level."
                ),
                "alarm_type": ALARM_TYPES.RESOURCE,
                "alarm_level": ALARM_LEVEL.WARNING.name,
                "alarm_cause": json.dumps([
                    "high_disk_usage",
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_mount_usage",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": disk_usage_threshold * 100
                    },
                },
            ]
        },
        "high_xlog_count_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "high_xlog_count_detector: The number of xlogs "
                    "has exceeded the warning level."
                ),
                "alarm_type": ALARM_TYPES.DISK_USAGE,
                "alarm_level": ALARM_LEVEL.WARNING.name,
                "alarm_cause": json.dumps([
                    "high_xlog_count"
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "xlog_margin",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "low": 0,
                        "closed": True
                    },
                },
            ]
        },
        # memory
        "high_mem_usage_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "high_mem_usage_detector: The memory usage has exceeded "
                    "the warning level."
                ),
                "alarm_type": ALARM_TYPES.RESOURCE,
                "alarm_level": ALARM_LEVEL.WARNING.name,
                "alarm_cause": json.dumps([
                    "high_dynamic_mem_usage",
                    "high_shared_mem_usage"
                ]),
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
        "mem_leak_detector": {
            "running": 1,
            "duration": 0,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "mem_leak_detector: The memory usage has an increasing trend.",
                "alarm_type": ALARM_TYPES.MEMORY,
                "alarm_level": ALARM_LEVEL.INFO.name,
                "alarm_cause": json.dumps([
                    "mem_leak",
                ]),
                "extra": "'os_mem_usage_mean': "
                         f"{json.dumps(LONG_TERM_METRIC_STATS['os_mem_usage_mean'])}",
            },
            "detector_info": [
                {
                    "metric_name": "os_mem_usage_mean",
                    "detector_name": "IncreaseDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "side": "positive"
                    },
                },
            ]
        },
        "session_mem_increase_detector": {
            "running": 1,
            "duration": detection_window_seconds * 12,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "session_mem_increase_detector: session_used_memory keeps rising.",
                "alarm_type": ALARM_TYPES.MEMORY,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "pg_session_memory_detail_size",
                    "detector_name": "IncreaseDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "side": "positive"
                    },
                },
                {
                    "metric_name": "pg_session_memory_detail_rate",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {"contextname": ""},
                    "detector_kwargs": {
                        "high": significance_threshold,
                        "percentage": 0.8
                    },
                },
                {
                    "metric_name": "dynamic_used_rate",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": mem_usage_threshold,
                        "percentage": 0.8
                    },
                },
            ]
        },
        "shared_mem_increase_detector": {
            "running": 1,
            "duration": detection_window_seconds * 12,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "shared_mem_increase_detector: shrctx_memory keeps rising.",
                "alarm_type": ALARM_TYPES.MEMORY,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "pg_shared_memory_detail_size",
                    "detector_name": "IncreaseDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "side": "positive"
                    },
                },
                {
                    "metric_name": "pg_shared_memory_detail_rate",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {"contextname": ""},
                    "detector_kwargs": {
                        "high": significance_threshold,
                        "percentage": 0.8
                    },
                },
                {
                    "metric_name": "dynamic_used_rate",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": mem_usage_threshold,
                        "percentage": 0.8
                    },
                },
            ]
        },
        "other_mem_increase_detector": {
            "running": 1,
            "duration": detection_window_seconds * 3,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "other_mem_increase_detector: other_used_memory keeps rising.",
                "alarm_type": ALARM_TYPES.MEMORY,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "pg_total_memory_detail_mbytes",
                    "detector_name": "IncreaseDetector",
                    "metric_filter": {"type": "other_used_memory"},
                    "detector_kwargs": {
                        "side": "positive"
                    },
                },
                {
                    "metric_name": "os_mem_usage",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": mem_usage_threshold,
                        "percentage": mem_high_usage_percent
                    },
                },
            ]
        },
        # cpu usage
        "high_cpu_usage_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "high_cpu_usage_detector: The cpu usage has exceeded the warning level."
                ),
                "alarm_type": ALARM_TYPES.RESOURCE,
                "alarm_level": ALARM_LEVEL.WARNING.name,
                "alarm_cause": json.dumps([
                    "high_cpu_usage",
                ]),
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
        # thread pool usage
        "high_thread_pool_rate_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "high_thread_pool_rate_detector: The thread pool rate has exceeded "
                    "the warning level."
                ),
                "alarm_type": ALARM_TYPES.RESOURCE,
                "alarm_level": ALARM_LEVEL.WARNING.name,
                "alarm_cause": json.dumps([
                    "high_thread_pool_rate",
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "pg_thread_pool_rate",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": thread_pool_usage_threshold,
                    },
                },
            ]
        },
        # disk io
        "high_io_delay_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "high_io_delay_detector: The disk io read delay has exceeded "
                    "the warning level."
                ),
                "alarm_type": ALARM_TYPES.DISK_IO,
                "alarm_level": ALARM_LEVEL.INFO.name,
                "alarm_cause": json.dumps([
                    "high_io_delay",
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "os_disk_await",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": disk_await_threshold * 3,
                        "percentage": 0.1
                    },
                },
                {
                    "metric_name": "os_disk_await",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {"device": ""},
                    "detector_kwargs": {
                        "high": disk_await_threshold,
                        "percentage": 0.5
                    },
                },
            ]
        },
        "slow_disk_detector": {
            "running": 1,
            "duration": 0,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    "slow_disk_detector: os_disk_await has exceeded "
                    "the warning level."
                ),
                "alarm_type": ALARM_TYPES.DISK_IO,
                "alarm_level": ALARM_LEVEL.WARNING.name,
                "extra": "'os_disk_await_mean': "
                         f"{json.dumps(LONG_TERM_METRIC_STATS['os_disk_await_mean'])}",
            },
            "detector_info": [
                {
                    "metric_name": "os_disk_await_mean",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": disk_await_threshold * 0.5,
                        "percentage": 0.25
                    },
                },
                {
                    "metric_name": "os_disk_await_mean",
                    "detector_name": "IncreaseDetector",
                    "metric_filter": {"device": ""},
                    "detector_kwargs": {
                        "side": "positive"
                    },
                },
            ]
        },
        "disk_io_jam_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "disk_io_jam_detector: os_disk_ioutils has exceeded "
                                 "the warning level.",
                "alarm_type": ALARM_TYPES.DISK_IO,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "os_disk_await",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": disk_await_threshold,
                        "percentage": 0.5
                    },
                },
                {
                    "metric_name": "os_disk_io_queue_length",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {"device": ""},
                    "detector_kwargs": {
                        "high": 0,
                    },
                },
                {
                    "metric_name": "os_disk_ioutils",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {"device": ""},
                    "detector_kwargs": {
                        "high": disk_ioutils_threshold,
                    },
                }
            ]
        },
        # network
        "lag_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "lag_detector: the lag has exceeded the warning level.",
                "alarm_type": ALARM_TYPES.NETWORK,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_ping_lag",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {"to_primary_dn": "True"},
                    "detector_kwargs": {
                        "high": ping_lag_threshold,
                        "percentage": 0.5
                    },
                },
            ]
        },
        "packet_loss_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "packet_loss_detector: the lag has been below the warning level.",
                "alarm_type": ALARM_TYPES.NETWORK,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_ping_packet_rate",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {"to_primary_dn": "True"},
                    "detector_kwargs": {
                        "low": ping_packet_rate_threshold
                    },
                },
            ]
        },
        # fd leak
        "leaked_fd_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "leaked_fd_detector: There are leaked fds under DB process.",
                "alarm_type": ALARM_TYPES.FD_LEAK,
                "alarm_level": ALARM_LEVEL.WARNING.name
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_process_leaked_fds",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": leaked_fds_threshold,
                    },
                },
            ]
        },
        # slow query
        "slow_sql_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "slow_sql_detector: There are slow sqls.",
                "alarm_type": ALARM_TYPES.SLOW_QUERY,
                "alarm_level": ALARM_LEVEL.INFO.name,
                "alarm_cause": json.dumps([
                    "long_transaction",
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "pg_long_transaction_count",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": long_transaction_threshold,
                    },
                },
            ]
        },
        # alarm log
        "deadlock_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "deadlock_detector: Found deadlock.",
                "alarm_type": ALARM_TYPES.ALARM_LOG,
                "alarm_level": ALARM_LEVEL.INFO.name,
                "alarm_cause": json.dumps([
                    "lock_conflict",
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_log_deadlock_count",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": 0
                    },
                },
            ]
        },
        "core_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "core_detector: Found Core dump.",
                "alarm_type": ALARM_TYPES.ALARM_LOG,
                "alarm_level": ALARM_LEVEL.ERROR.name,
                "alarm_cause": json.dumps([
                    "core_dump",
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_log_ffic",
                    "detector_name": "ThresholdDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "high": 0,
                    },
                },
            ]
        },
        "response_time_fluctuation_detector": {
            "running": 1,
            "duration": detection_window_seconds * 3,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": (
                    f"response_time_fluctuation_detector: The response time is abnormal."
                ),
                "alarm_type": ALARM_TYPES.PERFORMANCE,
                "alarm_level": ALARM_LEVEL.INFO.name,
                "alarm_cause": json.dumps([
                    "shift_response_time",
                ]),
            },
            "detector_info": [
                {
                    "metric_name": "statement_responsetime_percentile_p80",
                    "detector_name": "LevelShiftDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "outliers": (level_shift_outliers_1, level_shift_outliers_2 * 6),
                        "side": "positive",
                        "window": level_shift_window,
                        "agg": "mean"
                    },
                },
                {
                    "metric_name": "statement_responsetime_percentile_p95",
                    "detector_name": "LevelShiftDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "outliers": (level_shift_outliers_1, level_shift_outliers_2 * 3),
                        "side": "positive",
                        "window": level_shift_window,
                        "agg": "mean"
                    },
                }
            ]
        }
    }
    unused_detections = {
        # memory
        "mem_usage_spike_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "mem_usage_spike_detector: Found obvious spikes in memory usage.",
                "alarm_type": ALARM_TYPES.MEMORY,
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
        # performance
        "qps_spike_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "qps_spike_detector: Found obvious spikes in QPS.",
                "alarm_type": ALARM_TYPES.PERFORMANCE,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_qps_by_instance",
                    "detector_name": "SpikeDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "outliers": (spike_outliers_1, spike_outliers_2)
                    },
                },
            ]
        },
        "P95_fluctuation_detector": {
            "running": 1,
            "duration": detection_window_seconds * 6,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "P95_fluctuation_detector: the statement P95 has changed.",
                "alarm_type": ALARM_TYPES.PERFORMANCE,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "statement_responsetime_percentile_p95",
                    "detector_name": "LevelShiftDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "outliers": (level_shift_outliers_1, level_shift_outliers_2),
                        "side": "positive",
                        "window": level_shift_window,
                        "agg": "mean"
                    },
                },
            ]
        },
        # test evt
        "qps_evt_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "qps_fluctuation_detector: Found obvious fluctuation in qps.",
                "alarm_type": ALARM_TYPES.PERFORMANCE,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "opengauss_qps_by_instance",
                    "detector_name": "EvtDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "probability": 1e-3,
                        "depth": 400,
                        "update_interval": 3000,
                        "side": "up",
                        "method": "bispot"
                    },
                },
            ]
        },
        "P95_evt_detector": {
            "running": 1,
            "duration": detection_window_seconds,
            "forecasting_seconds": 0,
            "alarm_info": {
                "alarm_content": "P95_fluctuation_detector: Found obvious fluctuation in P95.",
                "alarm_type": ALARM_TYPES.PERFORMANCE,
                "alarm_level": ALARM_LEVEL.INFO.name,
            },
            "detector_info": [
                {
                    "metric_name": "statement_responsetime_percentile_p95",
                    "detector_name": "EvtDetector",
                    "metric_filter": {},
                    "detector_kwargs": {
                        "probability": 1e-3,
                        "depth": 400,
                        "update_interval": 3000,
                        "side": "up",
                        "method": "bispot"
                    },
                },
            ]
        },
    }
