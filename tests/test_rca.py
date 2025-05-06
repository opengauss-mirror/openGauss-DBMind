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

from datetime import datetime

from dbmind.common.types import Sequence
from dbmind.common.utils.checking import split_ip_port
from dbmind.components.metric_diagnosis import analyzer, rca_graph
from dbmind.components.metric_diagnosis.root_cause_analysis import (
    check_params,
    rca,
    insight_view
)
from dbmind.service import dai


def mock_get_metric_sequence_beta(metric_name, start_time, end_time, step):

    class MockFetcher(dai.LazyFetcher):
        def _read_buffer(self):
            instance_label = dai._get_data_source_flag(self.metric_name)
            end = int(datetime.now().timestamp() * 1000)
            start = end - 20 * 15 * 1000
            timestamps = list(range(start, end, 15000))
            if instance_label in self.labels_like:
                instance = split_ip_port(self.labels_like.pop(instance_label))[0]
            elif instance_label in self.labels:
                instance = self.labels[instance_label]
            else:
                instance = "some_ip:some_port"

            self.labels[instance_label] = instance
            self.labels["ip"] = '["127.0.0.1"]'
            self.labels["port"] = '1234'
            self.labels["role"] = 'dn'
            self.labels["device"] = 'sda'
            self.labels["file_system"] = '/dev/sda'
            self.labels["contextname"] = 'Storage'
            self.labels["unique_sql_id"] = '4321'
            self.labels["tid"] = '2345'
            self.labels["sessionid"] = '3456'
            self.labels["query"] = 'select pg_sleep(3650)'
            self.labels["content"] = (
                "Process 140162661480192 waits for ShareLock on transaction 65062974; "
                "blocked by process 140149035517696. "
                "Process 140149035517696 waits for ShareLock on transaction 65062961; "
                "blocked by process 140162661480192. "
                "Process 140162661480192: UPDATE DUC_CONFIG_INSTANCE_ATT_T t0 SET LAST_UPDATE_DATE=$1, "
                "DELETE_FLAG=$2, INVOICING_STATUS=$3 WHERE t0.CONFIG_INSTANCE_ID = $4 and t0.route_id = $5 "
                "Process 140149035517696: UPDATE DUC_CONFIG_INSTANCE_ATT_T t0 SET LAST_UPDATE_DATE=$1, "
                "DELETE_FLAG=$2, INVOICING_STATUS=$3 WHERE t0.CONFIG_INSTANCE_ID = $4 and t0.route_id = $5"
            )

            values = {
                "os_cpu_user_usage": (1.0,) * 5 + (10.0,) * 10 + (1.0,) * 5,
                "os_mem_usage": (1.0,) * 5 + (10.0,) * 10 + (1.0,) * 5,
                "os_disk_usage": (1.0,) * 5 + (20.0,) * 10 + (7.0,) * 5,
                "os_disk_ioutils": (0.999,) * 20,
                "os_disk_await": (1.0,) * 2 + (35.0,) * 17 + (0.2,) * 1,
                "os_disk_io_read_delay": (1.0,) * 5 + (5.0,) * 10 + (0.2,) * 5,
                "os_disk_io_write_delay": (0,) * 20,
                "os_network_receive_bytes": (0,) * 20,
                "os_network_transmit_bytes": (0,) * 20,
                "opengauss_active_connection": (1.0,) * 5 + (10.0,) * 10 + (1.0,) * 5,
                "opengauss_qps_by_instance": (1.0,) * 5 + (5.0,) * 10 + (0.2,) * 5,
                "opengauss_log_deadlock_count": (0,) * 5 + (1,) + (0,) * 14,
                "opengauss_log_login_denied": (70,) * 20,
                "pg_total_memory_detail_mbytes": tuple(range(20)),
                "pg_thread_pool_rate": (1.0,) * 5 + (10.0,) * 10 + (1.0,) * 5,
                "pg_sql_count_insert": (1.0,) * 5 + (20.0,) * 10 + (7.0,) * 5,
                "pg_sql_count_update": (1.0,) * 5 + (20.0,) * 10 + (7.0,) * 5,
                "pg_long_transaction_count": (0,) * 5 + tuple(range(3600, 3615)),
                "pg_sql_active_time": (1,) * 20,
                "non_db_mem_usage": tuple(range(20)),
                "pg_session_memory_detail_size": tuple(range(20)),
                "pg_shared_memory_detail_size": tuple(range(20)),
                "pg_temp_files_count": (7,) * 20,
                "xlog_margin": (-70,) * 20,
                "lsn_margin": (0,) * 20,
                "opengauss_log_recycle_replication_slot": (1,) * 20,
                "replication_lsn_margin": (0,) * 20,
                "opengauss_log_recycle_build": (1,) * 20,
                "opengauss_log_recycle_full_build": (1,) * 20,
                "opengauss_log_recycle_dcf_zero": (1,) * 20,
                "opengauss_log_recycle_dcf_else": (1,) * 20,
                "opengauss_log_recycle_dummy_standby": (1,) * 20,
                "opengauss_log_recycle_cbm": (1,) * 20,
                "opengauss_log_recycle_standby_backup": (1,) * 20,
                "opengauss_log_recycle_extro_read_zero": (1,) * 20,
                "opengauss_log_recycle_extro_read_else": (1,) * 20,
                "xlog_setting_margin": (0,) * 20,
            }
            if metric_name == "opengauss_log_recycle_lsn":
                return []

            seq = Sequence(
                timestamps=timestamps,
                values=values.get(metric_name, (0,) * 20),
                name=self.metric_name,
                labels=self.labels,
                step=self.step
            )
            return [seq]

    return MockFetcher(metric_name, start_time, end_time)


class MockAnalyzer:
    """The Abstract Analyzer"""
    def __init__(self, analyzer_args):
        self.metric_name = analyzer_args.metric_name
        self.metric_filter = analyzer_args.metric_filter
        self.metric_filter_like = analyzer_args.metric_filter_like
        self.length = analyzer_args.length
        self.params = analyzer_args.params
        if isinstance(self.params, dict):
            for name, param in self.params.items():
                if callable(param):
                    self.params[name] = 0.5

        self.score = analyzer_args.score
        self.step = analyzer_args.step
        self.related_seqs = {"normal": [], "abnormal": []}
        self.record = bool(analyzer_args.record)
        if analyzer_args.mode == "beginning":
            self.main_metric_sequence = analyzer_args.beginning_main_seq
            if self.length:
                self.end = max(analyzer_args.beginning_start + self.length, analyzer_args.beginning_end)
            else:
                self.end = analyzer_args.beginning_end

            self.start = analyzer_args.beginning_start
        else:
            self.main_metric_sequence = analyzer_args.recent_main_seq
            if self.length:
                self.start = min(analyzer_args.recent_end - self.length, analyzer_args.recent_start)
            else:
                self.start = analyzer_args.recent_start

            self.end = analyzer_args.recent_end


def test_rca(monkeypatch):
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence_beta)
    monkeypatch.setattr(analyzer.Analyzer, "__init__", MockAnalyzer.__init__)
    end = int(datetime.now().timestamp() * 1000)
    start = end - 20 * 15 * 1000

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "os_cpu_user_usage",
        '{"from_instance":"127.0.0.1"}',
        '["high_cpu_usage"]'
    )
    res = rca("os_cpu_user_usage", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    for k in res[0]:
        res[0][k] = round(res[0][k], 2)
    assert res == (
        {
            'cpu_io_delay': 0.5,
            'heavy_transaction': 0.5
        },
        'heavy_transaction',
        'Evaluate whether the resources, such as CPU and memory, meet business '
        'requirements based on business volume and whether capacity expansion is '
        'needed.'
    )
    res = insight_view("os_cpu_user_usage", metric_filter_dict, start, end, 15,
                       alarm_cause_list=alarm_cause_list,
                       reason_name_list=reason_name_list)
    assert res == {
        'check_active_sql_time': [
            {
                'time': datetime.fromtimestamp(start // 1000).strftime("%Y-%m-%d %H:%M:%S"),
                'unique_sql_id': '4321',
                'sql_time_in_seconds': 1
            }
        ]
    }

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "os_mem_usage",
        '{"from_instance": "127.0.0.1"}',
        'high_dynamic_mem_usage, high_shared_mem_usage'
    )
    res = rca("os_mem_usage", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    for k in res[0]:
        res[0][k] = round(res[0][k], 3)
    assert res == (
        {
            'dynamic_memory_rise': 0.101,
            'heavy_sessions': 0.337,
            'memory_heavy_writing': 0.326,
            'non_db_memory_rise': 0.135,
            'shared_memory_rise': 0.101
        },
        'heavy_sessions',
        'Contact the DBA to reduce the sessions.'
    )
    res = insight_view("os_mem_usage", metric_filter_dict, start, end, 15,
                       alarm_cause_list=alarm_cause_list,
                       reason_name_list=reason_name_list)
    assert res == {
        'check_session_memory_detail_snapshot': [
            {
                'time': datetime.fromtimestamp(start // 1000).strftime("%Y-%m-%d %H:%M:%S"),
                'contextname': 'Storage',
                'size_in_mb': 0.0
            }
        ],
        'check_shared_memory_detail_snapshot': [
            {
                'time': datetime.fromtimestamp(start // 1000).strftime("%Y-%m-%d %H:%M:%S"),
                'contextname': 'Storage',
                'size_in_mb': 0.0
            }
        ]
    }

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "pg_thread_pool_rate",
        'from_instance=127.0.0.1',
        'high_thread_pool_rate'
    )
    res = rca("pg_thread_pool_rate", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    for k in res[0]:
        res[0][k] = round(res[0][k], 2)
    assert res == (
        {
            'disk_io_delay': 0.5,
            'heavy_transaction': 0.5,
            'thread_pool_io_delay': 0.0,
            'workload_rise': 0.0
        },
        'heavy_transaction',
        'Evaluate whether the resources, such as CPU and memory, meet business '
        'requirements based on business volume and whether capacity expansion is '
        'needed.'
    )

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "os_disk_await",
        '{"from_instance":"127.0.0.1"}',
        '["high_io_delay"]'
    )
    res = rca("os_disk_await", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    assert res == (
        {'heavy_io': 1.0},
        'heavy_io',
        'Try to reduce IO pressure and increase disk IO limit'
    )

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "os_disk_usage",
        'from_instance = 127.0.0.1',
        '["high_disk_usage"]'
    )
    res = rca("os_disk_usage", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    assert res == (
        {
            'heavy_writing': 0.76923,
            'high_opengauss_xlog_count': 0.23077
        },
        'heavy_writing',
        'Analyze the ratio of insert or update operations and '
        'disk io reads and writes to determine whether dirty '
        'data is increasing too fast'
    )
    res = insight_view("os_disk_usage", metric_filter_dict, start, end, 15,
                       alarm_cause_list=alarm_cause_list,
                       reason_name_list=reason_name_list)
    assert res == {
        'check_temp_file_snapshot': [
            {
                'time': datetime.fromtimestamp(start // 1000).strftime("%Y-%m-%d %H:%M:%S"),
                'tid': '2345',
                'count': 7,
                'query': 'select pg_sleep(3650)'
            }
        ]
    }

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "os_mem_usage",
        'from_instance = 127.0.0.1',
        '["mem_leak"]'
    )
    res = rca("os_mem_usage", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    assert res == (
        {
            'dynamic_memory_rise': 0.1875,
            'shared_memory_rise': 0.1875,
            'other_memory_rise': 0.1875,
            'non_db_memory_rise': 0.25,
            'massive_login_denies': 0.1875
        },
        'non_db_memory_rise',
        'Non-database process memory leak.'
    )
    res = insight_view("os_mem_usage", metric_filter_dict, start, end, 15,
                       alarm_cause_list=alarm_cause_list,
                       reason_name_list=reason_name_list)
    assert res == {
        'check_session_memory_detail_snapshot': [
            {
                'time': datetime.fromtimestamp(start // 1000).strftime("%Y-%m-%d %H:%M:%S"),
                'contextname': 'Storage',
                'size_in_mb': 0.0
            }
        ],
        'check_shared_memory_detail_snapshot': [
            {
                'time': datetime.fromtimestamp(start // 1000).strftime("%Y-%m-%d %H:%M:%S"),
                'contextname': 'Storage',
                'size_in_mb': 0.0
            }
        ]
    }

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "xlog_margin",
        'from_instance = 127.0.0.1',
        '["high_xlog_count"]'
    )
    res = rca("xlog_margin", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    assert res == (
        {
            'building': 0.09677,
            'cbm': 0.09677,
            'dcf': 0.09677,
            'dummy_standby': 0.09677,
            'extro_read': 0.09677,
            'recycle_failed': 0.09677,
            'recycle_lsn': 0.09677,
            'replication_slot': 0.09677,
            'standby_backup': 0.09677,
            'wrong_xlog_setting': 0.12903
        },
        'wrong_xlog_setting',
        'The disk space is too small and the guc parameter is set improperly.'
    )

    metric_filter_dict, alarm_cause_list, reason_name_list, _ = check_params(
        "pg_long_transaction_count",
        'from_instance = 127.0.0.1',
        '["long_transaction"]'
    )
    res = rca("pg_long_transaction_count", metric_filter_dict, start, end, 15,
              alarm_cause_list=alarm_cause_list,
              reason_name_list=reason_name_list)[:3]
    assert res == (
        {'slow_sql': 1.0},
        'slow_sql',
        'If P80 and P95 continue to be high, the CPU usage remains high, '
        'the thread pool usage repeatedly exceeds the threshold, and there '
        'is no sign of recovery, you need to contact relevant personnel '
        'for further positioning analysis.'
    )
    res = insight_view("pg_long_transaction_count", metric_filter_dict, start, end, 15,
                       alarm_cause_list=alarm_cause_list,
                       reason_name_list=reason_name_list)
    assert res['check_long_transaction_memory_context_snapshot'][0]['sessionid'] == '3456'
    assert res['check_long_transaction_memory_context_snapshot'][0]['contextname'] == 'Storage'
    assert res['check_long_transaction_memory_context_snapshot'][0]['size_in_mb'] == 0.0
