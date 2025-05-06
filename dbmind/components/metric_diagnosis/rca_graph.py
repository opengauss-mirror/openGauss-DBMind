# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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

"""
Root Cause Analysis Graph
Analysis -- Reason -- Root Cause -- Insight
"""

from functools import partial

from .analyzer import (
    Correlation,
    Increase,
    Threshold,
    MeanThreshold,
    LevelShift,
    Empty
)
from .insight import (
    TableSpace,
    WaitStatus,
    TempFilesSnapshot,
    LongTransactionMemoryContextSnapshot,
    SessionMemoryDetailSnapshot,
    SharedMemoryDetailSnapshot,
    DeadlockLoop,
    BlockingLock,
    ActiveSqlTime,
    CoreDumpSqlId
)
from .utils import get_detector_params

ANALYSES = {
    "correlation": {
        "opengauss_qps_by_instance": {
            "method": Correlation,
            "metric_name": "opengauss_qps_by_instance",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"negative": -0.25, "positive": 0.25, "preprocess_method": "diff"},
            "mode": "beginning"
        },
        "os_network_receive_bytes": {
            "method": Correlation,
            "metric_name": "os_network_receive_bytes",
            "metric_filter_like": {"from_instance": "regex_ip"},
            "params": {"negative": -0.25, "positive": 0.25, "preprocess_method": "diff"},
            "mode": "beginning"
        },
        "os_network_transmit_bytes": {
            "method": Correlation,
            "metric_name": "os_network_transmit_bytes",
            "metric_filter_like": {"from_instance": "regex_ip"},
            "params": {"negative": -0.25, "positive": 0.25, "preprocess_method": "diff"},
            "mode": "beginning"
        },
        "dynamic_used_memory": {
            "method": Correlation,
            "metric_name": "pg_total_memory_detail_mbytes",
            "metric_filter": {"type": "dynamic_used_memory"},
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"negative": -0.25, "positive": 0.25, "preprocess_method": "diff"},
            "mode": "beginning"
        },
        "os_disk_io_read_delay": {
            "method": Correlation,
            "metric_name": "os_disk_io_read_delay",
            "metric_filter_like": {"from_instance": "regex_ip", "device": "devices"},
            "params": {"negative": -0.25, "positive": 0.25, "preprocess_method": "none"},
            "mode": "beginning"
        },
        "os_disk_io_write_delay": {
            "method": Correlation,
            "metric_name": "os_disk_io_write_delay",
            "metric_filter_like": {"from_instance": "regex_ip", "device": "devices"},
            "params": {"negative": -0.25, "positive": 0.25, "preprocess_method": "none"},
            "mode": "beginning"
        },
        "opengauss_active_connection": {
            "method": Correlation,
            "metric_name": "opengauss_active_connection",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"negative": None, "positive": 0.3, "preprocess_method": "diff"},
            "mode": "beginning"
        },
        "pg_sql_count_insert": {
            "method": Correlation,
            "metric_name": "pg_sql_count_insert",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"negative": None, "positive": 0.3, "preprocess_method": "none"},
            "mode": "beginning"
        },
        "pg_sql_count_update": {
            "method": Correlation,
            "metric_name": "pg_sql_count_update",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"negative": None, "positive": 0.3, "preprocess_method": "none"},
            "mode": "beginning"
        },
        "opengauss_process_cpu_usage": {
            "method": Correlation,
            "metric_name": "opengauss_process_cpu_usage",
            "metric_filter_like": {"ip": "regex_ip"},
            "params": {"negative": None, "positive": 0.3, "preprocess_method": "none"},
            "mode": "beginning"
        },
    },
    "threshold": {
        "high_os_disk_ioutils": {
            "method": Threshold,
            "metric_name": "os_disk_ioutils",
            "metric_filter_like": {"from_instance": "regex_ip", "device": "devices"},
            "params": {"low": -float("inf"), "high": 0.9, "percentage": 0.3},
            "score": 0.3,
            "mode": "recent"
        },
        "high_os_disk_await": {
            "method": Threshold,
            "metric_name": "os_disk_await",
            "metric_filter_like": {"from_instance": "regex_ip", "device": "devices"},
            "params": {"low": -float("inf"), "high": 30, "percentage": 0.8},
            "score": 0.3,
            "mode": "recent"
        },
        "high_long_transaction_count": {
            "method": Threshold,
            "metric_name": "pg_long_transaction_count",
            "metric_filter": {"state": "active"},
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": partial(get_detector_params, "slow_sql_detector")},
            "score": 0.3,
            "mode": "recent"
        },
        "high_opengauss_xlog_count": {
            "method": Threshold,
            "metric_name": "xlog_margin",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"low": 0, "high": float("inf"), "closed": True},
            "score": 0.3,
            "mode": "beginning"
        },
        "massive_login_denies": {
            "method": MeanThreshold,
            "metric_name": "opengauss_log_login_denied",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 30 / (60 * 1000)},  # 30 denies per minute
            "score": 0.3,
            "mode": "beginning"
        },
        "massive_deadlocks": {
            "method": MeanThreshold,
            "metric_name": "opengauss_log_dead_lock_count",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 30 / (60 * 1000)},  # 30 deadlocks per minute
            "score": 0.3,
            "mode": "beginning"
        },
        "massive_waitlock_timeouts": {
            "method": MeanThreshold,
            "metric_name": "opengauss_log_lock_wait_timeout",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 30 / (60 * 1000)},  # 30 deadlocks per minute
            "score": 0.3,
            "mode": "beginning"
        },
        "low_lsn_margin": {
            "method": Threshold,
            "metric_name": "lsn_margin",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"low": 0, "high": float("inf"), "closed": True},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_replication_slot": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_replication_slot",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "low_replication_lsn_margin": {
            "method": Threshold,
            "metric_name": "replication_lsn_margin",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"low": 0, "high": float("inf"), "closed": True},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_build": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_build",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_full_build": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_full_build",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_quorum_required": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_quorum_required",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_dcf_zero": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_dcf_zero",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_dcf_else": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_dcf_else",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_dummy_standby": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_dummy_standby",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_cbm": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_cbm",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_standby_backup": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_standby_backup",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_extro_read_zero": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_extro_read_zero",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "log_recycle_extro_read_else": {
            "method": Threshold,
            "metric_name": "opengauss_log_recycle_extro_read_else",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.3,
            "mode": "beginning"
        },
        "low_xlog_setting_margin": {
            "method": Threshold,
            "metric_name": "xlog_setting_margin",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"low": 0, "high": float("inf"), "closed": True},
            "score": 0.4,
            "mode": "beginning"
        },
        "high_non_db_mem_usage": {
            "method": Threshold,
            "metric_name": "non_db_mem_usage",
            "metric_filter_like": {"from_instance": "regex_ip"},
            "params": {"low": -float("inf"), "high": 0.5, "percentage": 0.8},
            "score": 0.4,
            "mode": "beginning",
            "record": False
        },
        "high_opengauss_ping_lag": {
            "method": Threshold,
            "metric_name": "opengauss_ping_lag",
            "metric_filter_like": {"source": "regex_ip"},
            "params": {"low": -float("inf"), "high": 50, "percentage": 0.8},
            "score": 0.25,
            "mode": "beginning"
        },
        "high_io_queue_length": {
            "method": Threshold,
            "metric_name": "os_disk_io_queue_length",
            "metric_filter_like": {"from_instance": "regex_ip", "device": "devices"},
            "params": {"low": -float("inf"), "high": 0},
            "score": 0.25,
            "mode": "recent"
        },
        "high_thread_pool_rate": {
            "method": Threshold,
            "metric_name": "pg_thread_pool_rate",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"low": -float("inf"), "high": 0.9, "percentage": 0.8},
            "score": 0.25,
            "mode": "recent"
        },
    },
    "increase": {
        "dynamic_used_memory": {
            "method": Increase,
            "metric_name": "pg_total_memory_detail_mbytes",
            "metric_filter": {"type": "dynamic_used_memory"},
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"max_coef": 100 / (3600 * 1000)},  # at least 100MB per hour
            "length_in_seconds": 1800,
            "score": 0.3,
            "mode": "recent"
        },
        "shared_used_memory": {
            "method": Increase,
            "metric_name": "pg_total_memory_detail_mbytes",
            "metric_filter": {"type": "dynamic_used_shrctx"},
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"max_coef": 100 / (3600 * 1000)},  # at least 100MB per hour
            "length_in_seconds": 1800,
            "score": 0.3,
            "mode": "recent"
        },
        "other_used_memory": {
            "method": Increase,
            "metric_name": "pg_total_memory_detail_mbytes",
            "metric_filter": {"type": "other_used_memory"},
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "params": {"max_coef": 100 / (3600 * 1000)},  # at least 100MB per hour
            "length_in_seconds": 1800,
            "score": 0.3,
            "mode": "recent"
        },
        "non_db_mem_usage": {
            "method": Increase,
            "metric_name": "non_db_mem_usage",
            "metric_filter_like": {"from_instance": "regex_ip"},
            "params": {"max_coef": 2 / (3600 * 1000)},  # at least 200% per hour
            "length_in_seconds": 1800,
            "score": 0.25,
            "mode": "recent"
        },
        "opengauss_qps_by_instance": {
            "method": Increase,
            "metric_name": "opengauss_qps_by_instance",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "length_in_seconds": 1800,
            "score": 0.3,
            "mode": "recent"
        },
        "os_cpu_user_usage": {
            "method": Increase,
            "metric_name": "os_cpu_user_usage",
            "metric_filter_like": {"from_instance": "regex_ip"},
            "length_in_seconds": 1800,
            "score": 0.3,
            "mode": "recent"
        },
        "os_mem_usage": {
            "method": Increase,
            "metric_name": "os_mem_usage",
            "metric_filter_like": {"from_instance": "regex_ip"},
            "length_in_seconds": 1800,
            "score": 0.3,
            "mode": "recent"
        },
    },
    "level_shift": {
        "p95_shift": {
            "method": LevelShift,
            "metric_name": "statement_responsetime_percentile_p95",
            "metric_filter_like": {"from_instance": "regex_ip_port"},
            "length_in_seconds": 3600,
            "params": {"upper_outliers": 3, "side": "positive", "window": 20},
            "score": 0.25,
            "mode": "recent"
        },
    },
    "empty": {
        "empty_log_recycle_lsn": {
            "method": Empty,
            "metric_name": "opengauss_log_recycle_lsn",
            "metric_filter_like": {"instance": "regex_ip_port"},
            "score": 0.3,
            "mode": "beginning"
        },
    }
}

REASONS = {
    "workload_rise": [
        [ANALYSES["increase"]["opengauss_qps_by_instance"]],
        [ANALYSES["increase"]["os_cpu_user_usage"]],
        [ANALYSES["increase"]["os_mem_usage"]],
    ],
    "heavy_transaction": [
        [ANALYSES["correlation"]["opengauss_qps_by_instance"]],
        [ANALYSES["correlation"]["os_network_receive_bytes"]],
        [ANALYSES["correlation"]["os_network_transmit_bytes"]],
        [ANALYSES["correlation"]["dynamic_used_memory"]],
    ],
    "disk_io_delay": [
        [ANALYSES["correlation"]["os_disk_io_read_delay"]],
        [ANALYSES["correlation"]["os_disk_io_write_delay"]],
    ],
    "disk_sub_health": [
        [ANALYSES["threshold"]["high_os_disk_ioutils"],
         ANALYSES["threshold"]["high_os_disk_await"],
         ANALYSES["threshold"]["high_io_queue_length"]],
    ],
    "heavy_sessions": [
        [ANALYSES["correlation"]["opengauss_active_connection"]],
    ],
    "dynamic_memory_rise": [
        [ANALYSES["increase"]["dynamic_used_memory"]],
    ],
    "shared_memory_rise": [
        [ANALYSES["increase"]["shared_used_memory"]],
    ],
    "other_memory_rise": [
        [ANALYSES["increase"]["other_used_memory"]],
    ],
    "slow_sql": [
        [ANALYSES["threshold"]["high_long_transaction_count"]],
    ],
    "heavy_writing": [
        [ANALYSES["correlation"]["pg_sql_count_insert"]],
        [ANALYSES["correlation"]["pg_sql_count_update"]],
    ],
    "heavy_io": [
        [ANALYSES["threshold"]["high_os_disk_ioutils"]],
    ],
    "massive_login_denies": [
        [ANALYSES["threshold"]["massive_login_denies"]],
    ],
    "high_opengauss_xlog_count": [
        [ANALYSES["threshold"]["high_opengauss_xlog_count"]],
    ],
    "lock_conflict": [
        [ANALYSES["threshold"]["massive_deadlocks"]],
        [ANALYSES["threshold"]["massive_waitlock_timeouts"]],
    ],
    "recycle_lsn": [
        [ANALYSES["threshold"]["low_lsn_margin"]]
    ],
    "replication_slot": [
        [ANALYSES["threshold"]["log_recycle_replication_slot"]],
        [ANALYSES["threshold"]["low_replication_lsn_margin"]],
    ],
    "building": [
        [ANALYSES["threshold"]["log_recycle_build"]],
        [ANALYSES["threshold"]["log_recycle_full_build"]],
        [ANALYSES["threshold"]["log_recycle_quorum_required"]],
    ],
    "dcf": [
        [ANALYSES["threshold"]["log_recycle_dcf_zero"]],
        [ANALYSES["threshold"]["log_recycle_dcf_else"]]
    ],
    "dummy_standby": [
        [ANALYSES["threshold"]["log_recycle_dummy_standby"]]
    ],
    "cbm": [
        [ANALYSES["threshold"]["log_recycle_cbm"]]
    ],
    "standby_backup": [
        [ANALYSES["threshold"]["log_recycle_standby_backup"]]
    ],
    "extro_read": [
        [ANALYSES["threshold"]["log_recycle_extro_read_zero"]],
        [ANALYSES["threshold"]["log_recycle_extro_read_else"]]
    ],
    "wrong_xlog_setting": [
        [ANALYSES["threshold"]["low_xlog_setting_margin"]]
    ],
    "recycle_failed": [
        [ANALYSES["empty"]["empty_log_recycle_lsn"]]
    ],
    "non_db_memory_rise": [
        [ANALYSES["increase"]["non_db_mem_usage"]],
        [ANALYSES["threshold"]["high_non_db_mem_usage"]]
    ],
    "network_lag": [
        [ANALYSES["threshold"]["high_opengauss_ping_lag"]]
    ],
    "process_cpu_usage": [
        [ANALYSES["correlation"]["opengauss_process_cpu_usage"]]
    ],
    "thread_high_used_rate": [
        [ANALYSES["threshold"]["high_thread_pool_rate"]]
    ],
}

ADVICES = {
    "heavy_transaction": (
        "Evaluate whether the resources, such as CPU and memory, meet business requirements "
        "based on business volume and whether capacity expansion is needed."
    ),
    "disk_io_delay": "Increase io throughput, check the processes which are occupying io.",
    "disk_sub_health": (
        "If it is found that the disk read and write latency is frequently too high or has a "
        "pronounced tendency to deteriorate, continue to locate whether the disk hardware "
        "is faulty."
    ),
    "slow_sql": (
        "If P80 and P95 continue to be high, the CPU usage remains high, the thread pool usage "
        "repeatedly exceeds the threshold, and there is no sign of recovery, you need to contact "
        "relevant personnel for further positioning analysis."
    ),
    "heavy_sessions": "Contact the DBA to reduce the sessions.",
    "dynamic_memory_rise": (
        "Terminate the session through pg_terminate_session or restart the DN process."
    ),
    "mem_heavy_writing": (
        "Consider reducing the pagewriter_sleep parameter to speed up the dirty page flushing speed; "
        "consider reducing the dirty_page_percent_max parameter to reduce the upper limit of the "
        "flushing threshold"
    ),
    "shared_memory_rise": (
        "Manual cleaning, ipcrm -m shmid (this command is dangerous to operate, you need to contact "
        "the kernel personnel for confirmation before executing"
    ),
    "disk_heavy_writing": (
        "Analyze the ratio of insert or update operations and disk io reads and writes to determine "
        "whether dirty data is increasing too fast"
    ),
    "heavy_io": "Try to reduce IO pressure and increase disk IO limit",
    "workload_rise": "Workload is rising, consider starting flow control.",
    "lock_conflict": "The lock conflicts increases. Please check whether there are long transactions.",
    "high_opengauss_xlog_count": "There are too many xlogs.",
    "other_memory_rise": "Other used memory is rising, check the memory usage of third-party software.",
    "massive_login_denies": "There are a large number of connection failures, please contact the DBA",
    "replication_slot": "There are logical replication slots that may jam the xlog recovery.",
    "recycle_lsn": "Xlog archiving failed.",
    "building": "Standby node building blocks xlog recycling.",
    "dcf": "DCF blocks xlog recycling.",
    "dummy_standby": "Dummy standby scenario blocks xlog recycling.",
    "cbm": "CBM blocks xlog recovery.",
    "standby_backup": "Standby_backup's replication slot blocks xlog recycling.",
    "extro_read": "Extreme rto blocking xlog recycling.",
    "wrong_xlog_setting": "The disk space is too small and the guc parameter is set improperly.",
    "recycle_failed": "xlog recycling process failed.",
    "non_db_memory_rise": "Non-database process memory leak.",
    "network_lag": "network_lag",
    "process_cpu_usage": "process_cpu_usage",
    "thread_high_used_rate": "thread_high_used_rate"
}

INSIGHTS = {
    "check_table_space": {  # pg_long_transaction_count
        "name": "check_table_space",
        "method": TableSpace,
        "kwargs": {
            "driver": "driver",
            "main_instance": "main_instance",
            "main_ip_list": "main_ip_list",
        },
    },
    "check_wait_status": {  # pg_long_transaction_count
        "name": "check_wait_status",
        "method": WaitStatus,
        "kwargs": {
            "driver": "driver",
            "main_instance": "main_instance",
            "main_ip_list": "main_ip_list",
        },
    },
    "check_long_transaction_memory_context_snapshot": {  # pg_long_transaction_count
        "name": "check_long_transaction_memory_context_snapshot",
        "method": LongTransactionMemoryContextSnapshot,
        "kwargs": {
            "recent_start": "recent_start",
            "recent_end": "recent_end",
            "beginning_start": "beginning_start",
            "beginning_end": "beginning_end",
            "original_start": "original_start",
            "step": "step",
            "main_instance": "main_instance",
        },
    },
    "check_temp_file_snapshot": {  # os_disk_usage, opengauss_mount_usage
        "name": "check_temp_file_snapshot",
        "method": TempFilesSnapshot,
        "kwargs": {
            "recent_start": "recent_start",
            "recent_end": "recent_end",
            "beginning_start": "beginning_start",
            "beginning_end": "beginning_end",
            "original_start": "original_start",
            "step": "step",
            "main_instance": "regex_ip_port",
        },
    },
    "check_session_memory_detail_snapshot": {  # os_mem_usage
        "name": "check_session_memory_detail_snapshot",
        "method": SessionMemoryDetailSnapshot,
        "kwargs": {
            "recent_start": "recent_start",
            "recent_end": "recent_end",
            "beginning_start": "beginning_start",
            "beginning_end": "beginning_end",
            "original_start": "original_start",
            "step": "step",
            "main_instance": "regex_ip_port",
        },
    },
    "check_shared_memory_detail_snapshot": {  # os_mem_usage
        "name": "check_shared_memory_detail_snapshot",
        "method": SharedMemoryDetailSnapshot,
        "kwargs": {
            "recent_start": "recent_start",
            "recent_end": "recent_end",
            "beginning_start": "beginning_start",
            "beginning_end": "beginning_end",
            "original_start": "original_start",
            "step": "step",
            "main_instance": "regex_ip_port",
        },
    },
    "check_deadlock_loop": {  # opengauss_log_deadlock_count, opengauss_log_lock_wait_timeout
        "name": "check_deadlock_loop",
        "method": DeadlockLoop,
        "kwargs": {
            "recent_start": "recent_start",
            "recent_end": "recent_end",
            "beginning_start": "beginning_start",
            "beginning_end": "beginning_end",
            "original_start": "original_start",
            "step": "step",
            "main_instance": "main_instance",
        },
    },
    "check_blocking_lock": {  # opengauss_log_deadlock_count, opengauss_log_lock_wait_timeout
        "name": "check_blocking_lock",
        "method": BlockingLock,
        "kwargs": {
            "recent_start": "recent_start",
            "recent_end": "recent_end",
            "beginning_start": "beginning_start",
            "beginning_end": "beginning_end",
            "original_start": "original_start",
            "step": "step",
            "main_instance": "main_instance",
        },
    },
    "check_active_sql_time": {  # os_cpu_usage
        "name": "check_active_sql_time",
        "method": ActiveSqlTime,
        "kwargs": {
            "recent_start": "recent_start",
            "recent_end": "recent_end",
            "beginning_start": "beginning_start",
            "beginning_end": "beginning_end",
            "original_start": "original_start",
            "step": "step",
            "main_instance": "regex_ip_port",
        },
    },
    "check_core_sql_id": {  # opengauss_log_ffic
        "name": "check_core_sql_id",
        "method": CoreDumpSqlId,
        "kwargs": {
            "metric_filter": "metric_filter",
            "time": "original_start"
        },
    },
}

RCA_GRAPH = {
    "high_cpu_usage": {
        "main_metric_name": ["os_cpu_user_usage"],
        "reasons": {
            "heavy_transaction": (
                REASONS["heavy_transaction"],
                ADVICES["heavy_transaction"],
            ),
            "cpu_io_delay": (
                REASONS["disk_io_delay"],
                ADVICES["disk_io_delay"],
            ),
        },
        "insights": {
            "heavy_transaction": [
                INSIGHTS["check_active_sql_time"]
            ],
        }
    },
    "high_thread_pool_rate": {
        "main_metric_name": ["pg_thread_pool_rate"],
        "reasons": {
            "heavy_transaction": (
                REASONS["heavy_transaction"],
                ADVICES["heavy_transaction"],
            ),
            "disk_io_delay": (
                REASONS["disk_io_delay"],
                ADVICES["disk_io_delay"],
            ),
            "thread_pool_io_delay": (
                REASONS["disk_sub_health"],
                ADVICES["disk_sub_health"],
            ),
            "workload_rise": (
                REASONS["workload_rise"],
                ADVICES["workload_rise"],
            ),
        },
    },
    "high_disk_usage": {
        "main_metric_name": ["os_disk_usage", "opengauss_mount_usage"],
        "reasons": {
            "heavy_writing": (
                REASONS["heavy_writing"],
                ADVICES["disk_heavy_writing"],
            ),
            "high_opengauss_xlog_count": (
                REASONS["high_opengauss_xlog_count"],
                ADVICES["high_opengauss_xlog_count"],
            ),
        },
        "insights": {
            "heavy_writing": [
                INSIGHTS["check_table_space"],
                INSIGHTS["check_temp_file_snapshot"]
            ],
        }
    },
    "high_dynamic_mem_usage": {
        "main_metric_name": ["os_mem_usage"],
        "reasons": {
            "heavy_sessions": (
                REASONS["heavy_sessions"],
                ADVICES["heavy_sessions"],
            ),
            "dynamic_memory_rise": (
                REASONS["dynamic_memory_rise"],
                ADVICES["dynamic_memory_rise"],
            ),
            "non_db_memory_rise": (
                REASONS["non_db_memory_rise"],
                ADVICES["non_db_memory_rise"],
            ),
        },
        "insights": {
            "dynamic_memory_rise": [
                INSIGHTS["check_session_memory_detail_snapshot"],
            ],
        }
    },
    "high_shared_mem_usage": {
        "main_metric_name": ["os_mem_usage"],
        "reasons": {
            "memory_heavy_writing": (
                REASONS["heavy_writing"],
                ADVICES["mem_heavy_writing"],
            ),
            "shared_memory_rise": (
                REASONS["shared_memory_rise"],
                ADVICES["shared_memory_rise"],
            ),
        },
        "insights": {
            "shared_memory_rise": [
                INSIGHTS["check_shared_memory_detail_snapshot"],
            ],
        }
    },
    "mem_leak": {
        "main_metric_name": ["os_mem_usage"],
        "reasons": {
            "dynamic_memory_rise": (
                REASONS["dynamic_memory_rise"],
                ADVICES["dynamic_memory_rise"],
            ),
            "shared_memory_rise": (
                REASONS["shared_memory_rise"],
                ADVICES["shared_memory_rise"],
            ),
            "other_memory_rise": (
                REASONS["other_memory_rise"],
                ADVICES["other_memory_rise"],
            ),
            "non_db_memory_rise": (
                REASONS["non_db_memory_rise"],
                ADVICES["non_db_memory_rise"],
            ),
            "massive_login_denies": (
                REASONS["massive_login_denies"],
                ADVICES["massive_login_denies"],
            ),
        },
        "insights": {
            "dynamic_memory_rise": [
                INSIGHTS["check_session_memory_detail_snapshot"],
            ],
            "shared_memory_rise": [
                INSIGHTS["check_shared_memory_detail_snapshot"],
            ],
        }
    },
    "high_io_delay": {
        "main_metric_name": ["os_disk_await"],
        "reasons": {
            "heavy_io": (
                REASONS["heavy_io"],
                ADVICES["heavy_io"],
            ),
        },
    },
    "high_xlog_count": {
        "main_metric_name": ["xlog_margin"],
        "reasons": {
            "recycle_lsn": [
                REASONS["recycle_lsn"],
                ADVICES["recycle_lsn"]
            ],
            "replication_slot": [
                REASONS["replication_slot"],
                ADVICES["replication_slot"]
            ],
            "building": [
                REASONS["building"],
                ADVICES["building"]
            ],
            "dcf": [
                REASONS["dcf"],
                ADVICES["dcf"]
            ],
            "dummy_standby": [
                REASONS["dummy_standby"],
                ADVICES["dummy_standby"]
            ],
            "cbm": [
                REASONS["cbm"],
                ADVICES["cbm"]
            ],
            "standby_backup": [
                REASONS["standby_backup"],
                ADVICES["standby_backup"]
            ],
            "extro_read": [
                REASONS["extro_read"],
                ADVICES["extro_read"]
            ],
            "wrong_xlog_setting": [
                REASONS["wrong_xlog_setting"],
                ADVICES["wrong_xlog_setting"]
            ],
            "recycle_failed": [
                REASONS["recycle_failed"],
                ADVICES["recycle_failed"]
            ],
        }
    },
    "long_transaction": {
        "main_metric_name": ["pg_long_transaction_count"],
        "min_length": 1,
        "reasons": {
            "slow_sql": (
                REASONS["slow_sql"],
                ADVICES["slow_sql"],
            ),
        },
        "insights": {
            "slow_sql": [
                INSIGHTS["check_long_transaction_memory_context_snapshot"],
                INSIGHTS["check_wait_status"],
            ],
        }
    },
    "lock_conflict": {
        "main_metric_name": ["opengauss_log_deadlock_count", "opengauss_log_lock_wait_timeout"],
        "min_length": 1,
        "reasons": {
            "lock_conflict": (
                REASONS["lock_conflict"],
                ADVICES["lock_conflict"],
            ),
        },
        "insights": {
            "lock_conflict": [
                INSIGHTS["check_deadlock_loop"],
                INSIGHTS["check_blocking_lock"],
            ]
        }
    },
    "core_dump": {
        "main_metric_name": ["opengauss_log_ffic"],
        "min_length": 1,
        "reasons": {},
        "insights": {
            "core_sql_id": [
                INSIGHTS["check_core_sql_id"],
            ]
        }
    },
    "shift_response_time": {
        "main_metric_name": ["statement_responsetime_percentile_p80", "statement_responsetime_percentile_p95"],
        "reasons": {
            "network_lag": (
                REASONS["network_lag"],
                ADVICES["network_lag"],
            ),
            "process_cpu_usage": (
                REASONS["process_cpu_usage"],
                ADVICES["process_cpu_usage"],
            ),
            "disk_sub_health": (
                REASONS["disk_sub_health"],
                ADVICES["disk_sub_health"],
            ),
            "heavy_io": (
                REASONS["heavy_io"],
                ADVICES["heavy_io"],
            ),
            "heavy_transaction": (
                REASONS["heavy_transaction"],
                ADVICES["heavy_transaction"],
            ),
            "workload_rise": (
                REASONS["workload_rise"],
                ADVICES["workload_rise"],
            ),
            "heavy_sessions": (
                REASONS["heavy_sessions"],
                ADVICES["heavy_sessions"],
            ),
            "slow_sql": (
                REASONS["slow_sql"],
                ADVICES["slow_sql"],
            ),
            "thread_high_used_rate": (
                REASONS["thread_high_used_rate"],
                ADVICES["thread_high_used_rate"],
            ),
        }
    },
}
