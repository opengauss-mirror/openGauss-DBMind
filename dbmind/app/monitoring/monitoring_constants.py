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
from typing import Optional

from dbmind.common.algorithm import anomaly_detection
from dbmind.common.types.enums import ALARM_LEVEL, ALARM_TYPES

ONE_HOUR_IN_SECONDS = 3600
SIX_HOUR_IN_SECONDS = 6 * ONE_HOUR_IN_SECONDS
ONE_DAY_IN_SECONDS = 24 * ONE_HOUR_IN_SECONDS
ONE_WEEK_IN_SECONDS = 7 * ONE_DAY_IN_SECONDS
ONE_MONTH_IN_SECONDS = 30 * ONE_DAY_IN_SECONDS

DEFAULT_STEP = 6 * 60 * 1000
MIN_STEP = 15001
INCREASE_MIN_LENGTH = 5
DEFAULT_FORECAST_TIME = 24 * 60  # one day
DEFAULT_UPPER_THRESHOLD = 0.7
DEFAULT_LOWER_THRESHOLD = 0.0
DEFAULT_FORECAST_PARAM = (2, 1, 2)
DEFAULT_DOWNSAMPLE_LENGTH = 60
LONG_TRANSACTION_DURATION = 12 * 60 * 60
RECOMMEND_LOWER_RATE = 0.9
RECOMMEND_UPPER_RATE = 1.1
MAX_FORECAST_NUM = 1e10

LONG_TRANSACTION_SQL = """
    WITH long_transactions AS (
        SELECT DISTINCT
            usename, datname, application_name, sessionid, query_id, query_start, state, unique_sql_id, query, 
            min(xact_start) AS xact_start
        FROM 
            pg_catalog.pg_stat_activity 
        WHERE 
            unique_sql_id != 0
        GROUP BY
            usename, datname, application_name, sessionid, query_id, query_start, state, unique_sql_id, query
    )
    SELECT DISTINCT
        usename, datname, application_name, sessionid, query_id, query_start, xact_start, state, unique_sql_id, 
        extract(epoch from pg_catalog.now() - xact_start) as duration, 
        pg_catalog.regexp_replace((CASE WHEN query like '%;' THEN query ELSE query || ';' END), E'[\\n\\r]+', ' ', 'g') as query 
    FROM 
        long_transactions
    WHERE 
        duration >= {};
"""

TOP_QUERIES_SQL = """
    SELECT 
        user_name, 
        unique_sql_id, 
        query, 
        n_calls, 
        min_elapse_time, 
        max_elapse_time, 
        case when (n_calls = 0) then total_elapse_time else total_elapse_time / n_calls end as avg_elapse_time, 
        n_returned_rows, 
        n_tuples_fetched, 
        n_tuples_returned, 
        n_tuples_inserted, 
        n_tuples_updated, 
        n_tuples_deleted, 
        n_blocks_fetched, 
        n_blocks_hit, 
        n_soft_parse, 
        n_hard_parse, 
        db_time, 
        cpu_time, 
        execution_time, 
        parse_time, 
        plan_time, 
        rewrite_time, 
        sort_count, 
        sort_time, 
        sort_mem_used, 
        sort_spill_count, 
        sort_spill_size, 
        hash_count, 
        hash_time, 
        hash_mem_used, 
        hash_spill_count, 
        hash_spill_size, 
        last_updated 
    FROM 
        dbe_perf.statement 
    WHERE 
        unique_sql_id != 0
    ORDER BY 
        n_calls DESC, 
        avg_elapse_time DESC 
    LIMIT 10;
"""

FULL_INSP_ITEM_LIST = [
    'os_cpu_usage', 'os_disk_usage', 'os_mem_usage', 'os_disk_ioutils', 'network_packet_loss',
    'data_directory', 'log_directory', 'db_size', 'buffer_hit_rate', 'user_login_out', 'log_error_check', 'thread_pool', 'db_latency', 'db_transaction',
    'db_tmp_file', 'db_exec_statement', 'db_deadlock', 'db_tps', 'db_top_query',
    'long_transaction', 'xmin_stuck', 'xlog_accumulate', 'core_dump', 'dynamic_memory',
    'process_memory', 'other_memory', 'component_error'
]

COUPLE_INSP_ITEM_DICT = {
    'os_cpu_usage': ['cpu_user', 'cpu_iowait'],
    'user_login_out': ['login', 'logout'],
    'db_latency': ['p95', 'p80'],
    'db_transaction': ['commit', 'rollback'],
    'db_exec_statement': ['select', 'update', 'insert', 'delete'],
    'db_tps': ['tps', 'qps'],
    'dynamic_memory': ['dynamic_used_memory', 'dynamic_used_shrctx']
}

INCREASE_INSP_ITEM_LIST = [
    'os_cpu_usage', 'os_disk_usage', 'os_mem_usage', 'os_disk_ioutils', 'data_directory',
    'log_directory', 'db_size', 'user_login_out', 'thread_pool', 'db_latency',
    'db_transaction', 'db_tmp_file', 'db_deadlock', 'db_tps', 'xlog_accumulate',
    'dynamic_memory', 'process_memory', 'other_memory'
]

THRESHOLD_INSP_ITEM_LIST = [
    'os_cpu_usage', 'os_disk_usage', 'os_mem_usage', 'os_disk_ioutils', 'network_packet_loss',
    'data_directory', 'log_directory', 'db_size', 'buffer_hit_rate', 'user_login_out',
    'active_session_rate', 'thread_pool', 'db_latency', 'db_transaction', 'db_tmp_file',
    'db_exec_statement', 'db_deadlock', 'db_tps', 'xlog_accumulate', 'dynamic_memory',
    'process_memory', 'other_memory'
]

FORECAST_INSP_ITEM_LIST = [
    'os_cpu_usage', 'os_disk_usage', 'os_mem_usage', 'os_disk_ioutils', 'network_packet_loss',
    'data_directory', 'log_directory', 'db_size', 'buffer_hit_rate', 'user_login_out',
    'active_session_rate', 'thread_pool', 'db_latency', 'db_transaction', 'db_tmp_file',
    'db_exec_statement', 'db_deadlock', 'db_tps', 'xlog_accumulate', 'dynamic_memory',
    'process_memory', 'other_memory'
]

FTYPE_INSP_ITEM_LIST = ['data_directory', 'log_directory']

FIXED_INSP_ITEM_LIST = ['component_error', 'log_error_check', 'db_top_query', 'long_transaction',
                        'xmin_stuck', 'core_dump', 'guc_params']

FULL_LOG_ERROR_LIST = [
    'deadlock_count', 'login_denied', 'errors_rate', 'panic', 'dn_ping_standby', 'node_restart',
    'node_start', 'cn_status', 'dn_status', 'gtm_status', 'dn_writable_failed',
    'etcd_io_overload', 'cms_heartbeat_timeout_restart', 'cms_phonydead_restart', 'cms_cn_down',
    'cn_restart_time_exceed', 'cms_read_only', 'cms_restart_pending', 'cms_heartbeat_timeout',
    'etcd_restart', 'etcd_not_connect_dial_tcp', 'etcd_auth_failed', 'etcd_overload',
    'etcd_sync_timeout', 'etcd_disk_full', 'etcd_be_killed', 'bind_ip_failed',
    'gtm_disconnected_to_primary', 'gtm_panic', 'ffic'
]

WARNING_INFO_DICT = {
    'cpu_user': (False, [float('-inf'), 0.7, None], [24 * 60, float('-inf'), 0.7]),
    'cpu_iowait': (False, [float('-inf'), 0.3, None]),
    'os_disk_usage': (False, [float('-inf'), 0.8, None]),
    'os_mem_usage': (True, [float('-inf'), 0.7, 0.8], [24 * 60, float('-inf'), 0.8]),
    'os_disk_ioutils': (False, [float('-inf'), 0.8, None]),
    'network_packet_loss': (False, [float('-inf'), 0.05, None]),
    'component_error': (False, None, None, False, -1),
    'data_directory': (False, [float('-inf'), 0.8, None], [24 * 60, float('-inf'), 0.8], True),
    'log_directory': (False, [float('-inf'), 0.8, None], [24 * 60, float('-inf'), 0.8], True),
    'db_size': (False,),
    'buffer_hit_rate': (False, [0.9, float('inf'), None]),
    'login': (False,),
    'logout': (False,),
    'active_session_rate': (False, [0.8, float('inf'), None]),
    'log_error_check': (False,),
    'thread_pool': (True, [float('-inf'), 0.9, 0.8]),
    'p95': (False,),
    'p80': (False,),
    'commit': (False,),
    'rollback': (False,),
    'db_tmp_file': (False,),
    'select': (False,),
    'update': (False,),
    'insert': (False,),
    'delete': (False,),
    'db_deadlock': (False,),
    'tps': (False,),
    'qps': (False,),
    'db_top_query': (False,),
    'long_transaction': (False,),
    'xmin_stuck': (False, [1, float('inf'), None]),
    'xlog_accumulate': (False, [float('-inf'), 3000, None]),
    'core_dump': (False,),
    'dynamic_used_memory': (False, [float('-inf'), 0.8, None]),
    'dynamic_used_shrctx': (False, [float('-inf'), 0.8, None]),
    'process_memory': (False, [float('-inf'), 0.8, None]),
    'other_memory': (True, [float('-inf'), 20 * 1024, 0.8]),
    'guc_params': (False,),
    'index_advisor': (False,),
}

SCORE_WEIGHT_DICT = {
    'os_cpu_usage': 0.06,
    'os_disk_usage': 0.06,
    'os_mem_usage': 0.06,
    'os_disk_ioutils': 0.06,
    'network_packet_loss': 0.06,
    'component_error': 0.05,
    'data_directory': 0.04,
    'log_directory': 0.04,
    'db_size': 0,
    'buffer_hit_rate': 0.03,
    'user_login_out': 0,
    'active_session_rate': 0.03,
    'log_error_check': 0.03,
    'thread_pool': 0.03,
    'db_latency': 0.08,
    'db_transaction': 0,
    'db_tmp_file': 0,
    'db_exec_statement': 0,
    'db_deadlock': 0.03,
    'db_tps': 0,
    'db_top_query': 0,
    'long_transaction': 0.05,
    'xlog_accumulate': 0.05,
    'xmin_stuck': 0.05,
    'core_dump': 0.04,
    'dynamic_memory': 0.04,
    'process_memory': 0.04,
    'other_memory': 0.03,
    'index_advisor': 0.05,
    'guc_params': 0.04
}

LONG_TERM_DETECTOR_NAMES = ("slow_disk_detector", "mem_leak_detector")
LONG_TERM_METRIC_STATS = {
    "os_disk_await_mean": {"length": 720, "step": 3600},
    "os_mem_usage_mean": {"length": 720, "step": 3600},
}


class DetectorAction:
    PAUSE = "pause"
    RESUME = "resume"


class DetectorParam:
    DETECTOR = "detector"
    ALARM_INFO = "alarm_info"
    DETECTOR_ID = "detector_id"
    CLUSTER_NAME = "cluster_name"
    DETECTOR_NAME = "detector_name"
    ALARM_CAUSE = "alarm_cause"
    ALARM_CONTENT = "alarm_content"
    ALARM_LEVEL = "alarm_level"
    ALARM_TYPE = "alarm_type"
    EXTRA = "extra"
    DETECTOR_INFO = "detector_info"
    DURATION = "duration"
    FORECASTING_SECONDS = "forecasting_seconds"
    RUNNING = "running"


def find_anomaly_detector(detector_name):
    detectors = anomaly_detection.detectors
    if detector_name not in detectors:
        raise KeyError(f"Detector name: {detector_name} was not found.")

    return detectors[detector_name]


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
    def __init__(
            self,
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


def get_monitoring_alarm_args():
    return set(inspect.getfullargspec(AlarmInfo.__init__).args[1:])


def get_monitoring_detector_args():
    return set(inspect.getfullargspec(DetectorInfo.__init__).args[1:])

