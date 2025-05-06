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
import configparser
from unittest import mock

import pytest

from dbmind.app import monitoring
from dbmind.common.opengauss_driver import Driver
from dbmind.common.types import Sequence, SlowQuery
from dbmind.components.slow_query_diagnosis import query_info_source
from dbmind.components.slow_query_diagnosis.query_feature import QueryFeature
from dbmind.components.slow_query_diagnosis.query_info_source import QueryContextFromTSDBAndRPC, QueryContextFromDriver, \
    SystemInfo, TableStructure, TotalMemoryDetail, DatabaseInfo, PgSetting, WaitEvents, WaitEventItem
from dbmind.metadatabase.schema.config_dynamic_params import DynamicParams
from dbmind.service import dai
from dbmind.service.multicluster import AgentProxy

pg_class_relsize_dict = {'datname': 'database1', 'nspname': 'schema1', 'relname': 'table1',
                         'relkind': 'r', 'relhasindex': 'True', 'relsize': 100}
pg_lock_sql_locked_times_dict = {'locked_query': 'update table2 set age=20 where id=3',
                                 'locked_query_start': 1640139695,
                                 'locker_query': 'delete from table2 where id=3',
                                 'locker_query_start': 1640139690}
pg_tables_structure_dead_rate_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_n_live_tup_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_n_dead_tup_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_column_number_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_vacuum_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_analyze_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_autovacuum_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_autoanalyze_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_never_used_indexes_index_size_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1',
                                         'indexrelname': 'table_index1'}
pg_table_skewness_skewstddev_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1',
                                     'skewratio': 0.3}

pg_tables_structure_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1',
                            'n_live_tup': 10000,
                            'n_dead_tup': 100, 'dead_rate': 0.01, 'last_vacuum': None,
                            'last_autovacuum': None,
                            'last_analyze': None, 'last_autoanalyze': None}
pg_settings_dict = {'name': 'shared_buffers', 'vartype': 'int64', 'setting': 100}
opengauss_qps_by_instance_dict = {'instance': '127.0.0.1:5432'}
pg_connections_max_conn_dict = {'instance': '127.0.0.1:5432'}
pg_connections_used_conn_dict = {'instance': '127.0.0.1:5432'}
pg_wait_events_last_updated_dict = {'nodename': 'node1', 'type': 'IO_EVENT', 'event': 'CopyFileWrite'}
pg_thread_pool_listener_dict = {'worker_info': 'default: 250 new: 0 expect: 250 actual: 250 idle: 250 pending: 0',
                                'session_info': 'total: 4 waiting: 0 running:0 idle: 4', 'group_id': 1, 'listener': 1}
pg_tables_size_bytes_dict = {'datname': 'database1', 'nspname': 'schema1', 'relname': 'tables'}
pg_index_idx_scan_dict = {'datname': 'database1', 'nspname': 'schema1', 'tablename': 'table1',
                          'relname': 'index1',
                          'indexdef': 'CREATE INDEX index1 ON table1 USING btree (col1) TABLESPACE pg_default'}

os_disk_iops_dict = {'instance': '127.0.0.1:5432'}
os_disk_ioutils_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
os_disk_usage_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
os_cpu_iowait_dict = {'instance': '127.0.0.1:5432'}
os_disk_iocapacity_dict = {'instance': '127.0.0.1:5432'}
os_cpu_usage_dict = {'instance': '127.0.0.1:5432'}
os_mem_usage_dict = {'instance': '127.0.0.1:5432'}
node_load1_dict = {'instance': '127.0.0.1:5432'}
load_average_dict = {'instance': '127.0.0.1:5432'}
io_write_delay_time_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
io_read_delay_time_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
io_queue_number_time_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
node_process_fds_rate_dict = {'instance': '127.0.0.1:5432'}

node_network_receive_bytes_dict = {'instance': '127.0.0.1'}
node_network_transmit_bytes_dict = {'instance': '127.0.0.1'}
node_network_receive_drop_dict = {'instance': '127.0.0.1'}
node_network_transmit_drop_dict = {'instance': '127.0.0.1'}
node_network_receive_packets_dict = {'instance': '127.0.0.1'}
node_network_transmit_packets_dict = {'instance': '127.0.0.1'}
node_network_receive_error_dict = {'instance': '127.0.0.1'}
node_network_transmit_error_dict = {'instance': '127.0.0.1'}

pg_stat_bgwriter_buffers_checkpoint_dict = {'instance': '127.0.0.1:5432'}
pg_stat_bgwriter_buffers_clean_dict = {'instance': '127.0.0.1:5432'}
pg_stat_bgwriter_buffers_backend_dict = {'instance': '127.0.0.1:5432'}
pg_stat_bgwriter_buffers_alloc_dict = {'instance': '127.0.0.1:5432'}

pg_replication_lsn_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_sent_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_write_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_flush_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_replay_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}

gs_sql_count_select_dict = {'instance': '127.0.0.1:5432'}
gs_sql_count_update_dict = {'instance': '127.0.0.1:5432'}
gs_sql_count_delete_dict = {'instance': '127.0.0.1:5432'}
gs_sql_count_insert_dict = {'instance': '127.0.0.1:5432'}

timed_task_dict = {'job_id': 1, 'priv_user': 'user', 'dbname': 'database',
                   'job_status': 1, 'last_start_date': 1640139694, 'last_end_date': 1640139695}

pg_class_relsize_seq = Sequence(timestamps=(1640139695000,),
                                values=(1000,),
                                name='pg_class_relsize',
                                step=5,
                                labels=pg_class_relsize_dict)

pg_lock_sql_locked_times_seq = Sequence(timestamps=(1640139695000,),
                                        values=(1000,),
                                        name='pg_lock_sql_locked_times',
                                        step=5,
                                        labels=pg_lock_sql_locked_times_dict)
pg_tables_structure_dead_rate_seq = Sequence(timestamps=(1640139695000,),
                                             values=(0.4,),
                                             name='pg_tables_structure_dead_rate',
                                             step=5,
                                             labels=pg_tables_structure_dead_rate_dict)
pg_tables_structure_n_live_tup_seq = Sequence(timestamps=(1640139695000,),
                                              values=(1000,),
                                              name='pg_tables_structure_n_live_tup',
                                              step=5,
                                              labels=pg_tables_structure_n_live_tup_dict)
pg_tables_structure_n_dead_tup_seq = Sequence(timestamps=(1640139695000,),
                                              values=(1000,),
                                              name='pg_tables_structure_n_dead_tup',
                                              step=5,
                                              labels=pg_tables_structure_n_dead_tup_dict)
pg_tables_structure_column_number_seq = Sequence(timestamps=(1640139695000,),
                                                 values=(3,),
                                                 name='pg_tables_structure_column_number',
                                                 step=5,
                                                 labels=pg_tables_structure_column_number_dict)
pg_tables_structure_last_vacuum_seq = Sequence(timestamps=(1640139695000,),
                                               values=(1640139695000,),
                                               name='pg_tables_structure_last_vacuum',
                                               step=5,
                                               labels=pg_tables_structure_last_vacuum_dict)
pg_tables_structure_last_analyze_seq = Sequence(timestamps=(1640139695000,),
                                                values=(1640139695000,),
                                                name='pg_tables_structure_last_analyze',
                                                step=5,
                                                labels=pg_tables_structure_last_analyze_dict)
pg_tables_structure_last_autovacuum_seq = Sequence(timestamps=(1640139695000,),
                                                   values=(1640139695000,),
                                                   name='pg_tables_structure_last_autovacuum',
                                                   step=5,
                                                   labels=pg_tables_structure_last_autovacuum_dict)
pg_tables_structure_last_autoanalyze_seq = Sequence(timestamps=(1640139695000,),
                                                    values=(1640139695000,),
                                                    name='pg_tables_structure_last_autoanalyze',
                                                    step=5,
                                                    labels=pg_tables_structure_last_autoanalyze_dict)

pg_table_skewness_skewstddev_seq = Sequence(timestamps=(1640139695000,),
                                            values=(0.4,),
                                            name='pg_tables_structure_last_autoanalyze',
                                            step=5,
                                            labels=pg_table_skewness_skewstddev_dict)
pg_tables_size_bytes_seq = Sequence(timestamps=(1640139695000,),
                                    values=(1024 * 1024,),
                                    name='pg_tables_size_bytes',
                                    step=5,
                                    labels=pg_tables_size_bytes_dict)

pg_index_idx_scan_seq = Sequence(timestamps=(1640139695000,),
                                 values=(10000,),
                                 name='pg_index_idx_scan',
                                 step=5,
                                 labels=pg_index_idx_scan_dict)

pg_never_used_indexes_index_size_seq = Sequence(timestamps=(1640139695000,),
                                                values=(1000,),
                                                name='pg_never_used_indexes_index_size',
                                                step=5,
                                                labels=pg_never_used_indexes_index_size_dict)
pg_settings_setting_seq = Sequence(timestamps=(1640139695000,),
                                   values=(100,),
                                   name='pg_settings',
                                   step=5,
                                   labels=pg_settings_dict)
opengauss_qps_by_instance_seq = Sequence(timestamps=(1640139695000,),
                                       values=(1000,),
                                       name='opengauss_qps_by_instance',
                                       step=5,
                                       labels=opengauss_qps_by_instance_dict)

pg_connections_max_conn_seq = Sequence(timestamps=(1640139695000,),
                                       values=(100,),
                                       name='pg_connections_max_conn',
                                       step=5,
                                       labels=pg_connections_max_conn_dict)

pg_connections_used_conn_seq = Sequence(timestamps=(1640139695000,),
                                        values=(10,),
                                        name='pg_connections_used_conn',
                                        step=5,
                                        labels=pg_connections_used_conn_dict)
pg_thread_pool_listener_seq = Sequence(timestamps=(1640139695000,),
                                       values=(10,),
                                       name='pg_thread_pool_listener',
                                       step=5,
                                       labels=pg_thread_pool_listener_dict)
pg_wait_events_last_updated_seq = Sequence(timestamps=(1640139695000,),
                                           values=(1640139695000,),
                                           name='pg_wait_events_last_updated',
                                           step=5,
                                           labels=pg_wait_events_last_updated_dict)
os_disk_iops_seq = Sequence(timestamps=(1640139695000, 1640139700000, 1640139705000),
                            values=(1000, 1000, 1000),
                            name='os_disk_iops',
                            step=5,
                            labels=os_disk_iops_dict)

os_disk_ioutils_seq = Sequence(timestamps=(1640139695000, 1640139700000, 1640139705000),
                               values=(0.5, 0.3, 0.2),
                               name='os_disk_ioutils',
                               step=5,
                               labels=os_disk_ioutils_dict)
os_disk_usage_seq = Sequence(timestamps=(1640139695000, 1640139700000, 1640139705000),
                             values=(0.5, 0.3, 0.2),
                             name='os_disk_ioutils',
                             step=5,
                             labels=os_disk_usage_dict)

os_cpu_iowait_seq = Sequence(timestamps=(1640139695000,),
                             values=(0.15,),
                             name='os_cpu_iowait',
                             step=5,
                             labels=os_cpu_iowait_dict)

os_disk_iocapacity_seq = Sequence(timestamps=(1640139695000,),
                                  values=(200,),
                                  name='os_disk_iocapacity',
                                  step=5,
                                  labels=os_disk_iocapacity_dict)

os_cpu_usage_seq = Sequence(timestamps=(1640139695000,),
                            values=(0.2,),
                            name='os_cpu_usage',
                            step=5,
                            labels=os_cpu_usage_dict)

load_average_seq = Sequence(timestamps=(1640139695000,),
                            values=(0.3,),
                            name='load_average',
                            step=5,
                            labels=node_load1_dict)

os_mem_usage_seq = Sequence(timestamps=(1640139695000,),
                            values=(0.2,),
                            name='os_mem_usage',
                            step=5,
                            labels=os_mem_usage_dict)

node_load1_seq = Sequence(timestamps=(1640139695000,),
                          values=(0.3,),
                          name='node_load1',
                          step=5,
                          labels=node_load1_dict)

io_write_delay_time_seq = Sequence(timestamps=(1640139695000,),
                                   values=(100,),
                                   name='io_write_delay_time',
                                   step=5,
                                   labels=io_write_delay_time_dict)

io_read_delay_time_seq = Sequence(timestamps=(1640139695000,),
                                  values=(100,),
                                  name='io_read_delay_time',
                                  step=5,
                                  labels=io_read_delay_time_dict)
io_queue_number_seq = Sequence(timestamps=(1640139695000,),
                               values=(100,),
                               name='io_queue_number',
                               step=5,
                               labels=io_queue_number_time_dict)

node_process_fds_rate_seq = Sequence(timestamps=(1640139695000,),
                                     values=(0.3,),
                                     name='node_process_fds_rate',
                                     step=5,
                                     labels=node_process_fds_rate_dict)
node_network_receive_bytes_seq = Sequence(timestamps=(1640139695000,),
                                          values=(1000,),
                                          name='node_network_receive_bytes',
                                          step=5,
                                          labels=node_network_receive_bytes_dict)
node_network_transmit_bytes_seq = Sequence(timestamps=(1640139695000,),
                                           values=(1000,),
                                           name='node_network_transmit_bytes',
                                           step=5,
                                           labels=node_network_transmit_bytes_dict)
node_network_receive_drop_seq = Sequence(timestamps=(1640139695000,),
                                         values=(0.3,),
                                         name='node_network_receive_drop',
                                         step=5,
                                         labels=node_network_receive_drop_dict)
node_network_transmit_drop_seq = Sequence(timestamps=(1640139695000,),
                                          values=(0.3,),
                                          name='node_network_transmit_drop',
                                          step=5,
                                          labels=node_network_transmit_drop_dict)
node_network_receive_packets_seq = Sequence(timestamps=(1640139695000,),
                                            values=(100,),
                                            name='node_network_receive_packets',
                                            step=5,
                                            labels=node_network_receive_packets_dict)
node_network_transmit_packets_seq = Sequence(timestamps=(1640139695000,),
                                             values=(100,),
                                             name='node_network_transmit_packets',
                                             step=5,
                                             labels=node_network_transmit_packets_dict)
node_network_receive_error_seq = Sequence(timestamps=(1640139695000,),
                                          values=(0.3,),
                                          name='node_network_receive_error',
                                          step=5,
                                          labels=node_network_receive_error_dict)
node_network_transmit_error_seq = Sequence(timestamps=(1640139695000,),
                                           values=(0.3,),
                                           name='node_network_transmit_error',
                                           step=5,
                                           labels=node_network_transmit_error_dict)

pg_stat_bgwriter_buffers_checkpoint_seq = Sequence(timestamps=(1640139695000,),
                                                   values=(10,),
                                                   name='pg_stat_bgwriter_checkpoint_avg_sync_time',
                                                   step=5,
                                                   labels=pg_stat_bgwriter_buffers_checkpoint_dict)
pg_stat_bgwriter_buffers_clean_seq = Sequence(timestamps=(1640139695000,),
                                              values=(20,),
                                              name='pg_stat_bgwriter_buffers_clean',
                                              step=5,
                                              labels=pg_stat_bgwriter_buffers_clean_dict)
pg_stat_bgwriter_buffers_backend_seq = Sequence(timestamps=(1640139695000,),
                                                values=(30,),
                                                name='pg_stat_bgwriter_buffers_backend',
                                                step=5,
                                                labels=pg_stat_bgwriter_buffers_backend_dict)
pg_stat_bgwriter_buffers_alloc_seq = Sequence(timestamps=(1640139695000,),
                                              values=(40,),
                                              name='pg_stat_bgwriter_buffers_alloc',
                                              step=5,
                                              labels=pg_stat_bgwriter_buffers_alloc_dict)

pg_replication_lsn_seq = Sequence(timestamps=(1640139695000,),
                                  values=(1000,),
                                  name='pg_replication_lsn',
                                  step=5,
                                  labels=pg_replication_lsn_dict)
pg_replication_sent_diff_seq = Sequence(timestamps=(1640139695000,),
                                        values=(1000,),
                                        name='pg_replication_sent_diff',
                                        step=5,
                                        labels=pg_replication_sent_diff_dict)

pg_replication_write_diff_seq = Sequence(timestamps=(1640139695000,),
                                         values=(1000,),
                                         name='pg_replication_write_diff',
                                         step=5,
                                         labels=pg_replication_write_diff_dict)

pg_replication_flush_diff_seq = Sequence(timestamps=(1640139695000,),
                                         values=(1000,),
                                         name='pg_replication_flush_diff',
                                         step=5,
                                         labels=pg_replication_flush_diff_dict)

pg_replication_replay_diff_seq = Sequence(timestamps=(1640139695000,),
                                          values=(1000,),
                                          name='pg_replication_replay_diff',
                                          step=5,
                                          labels=pg_replication_replay_diff_dict)
gs_sql_count_select_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_select',
                                   step=5,
                                   labels=gs_sql_count_select_dict)
gs_sql_count_update_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_update',
                                   step=5,
                                   labels=gs_sql_count_update_dict)

gs_sql_count_delete_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_delete',
                                   step=5,
                                   labels=gs_sql_count_delete_dict)
gs_sql_count_insert_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_insert',
                                   step=5,
                                   labels=gs_sql_count_insert_dict)
db_timed_task_failure_count_seq = Sequence(timestamps=(1640139695000,),
                                           values=(1000,),
                                           name='timed_task',
                                           step=5,
                                           labels=timed_task_dict)


slow_query_instance = SlowQuery(db_host='127.0.0.1',
                                db_port='8080',
                                db_name='database1',
                                schema_name='schema1',
                                query='select count(*) from schema1.table1',
                                start_time=1640139691000,
                                duration_time=1000,
                                track_parameter=True,
                                plan_time=100,
                                parse_time=20,
                                db_time=2000,
                                hit_rate=0.99,
                                fetch_rate=0.98,
                                cpu_time=14200,
                                data_io_time=1231243,
                                template_id=12432453234,
                                query_plan=None,
                                sort_count=13,
                                sort_mem_used=12.43,
                                sort_spill_count=3,
                                hash_count=0,
                                hash_mem_used=0,
                                hash_spill_count=0,
                                lock_wait_time=1,
                                lwlock_wait_time=1,
                                n_returned_rows=1,
                                n_tuples_returned=100000,
                                n_tuples_fetched=1001,
                                n_tuples_deleted=1001,
                                n_tuples_inserted=1001,
                                n_tuples_updated=1001)
slow_query_instance.tables_name = {'schema1': ['table1']}
slow_query_instance.advise = 'some other advise'


class MockedFetcher(dai.LazyFetcher):
    def __init__(self, metric, start_time=None, end_time=None):
        super().__init__(metric)
        self.metric = metric
        self.start_time = start_time
        self.end_time = end_time

    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        self.metric = f"{self.metric}_seq"
        return [globals().get(self.metric, None)]


@pytest.fixture
def mock_get_slow_queries(monkeypatch, mock_dai):
    monkeypatch.setattr(mock_dai, 'get_latest_metric_sequence',
                        mock.Mock(side_effect=lambda x, y: MockedFetcher(metric=x)))
    monkeypatch.setattr(mock_dai, 'get_metric_sequence', mock.Mock(side_effect=lambda x, y, z: MockedFetcher(metric=x)))


@pytest.fixture(autouse=True)
def mock_get_function(monkeypatch):
    monkeypatch.setattr(monitoring, 'get_detection_threshold',
                        mock.Mock(side_effect=lambda x: mock_get_detection_threshold(param=x)))
    monkeypatch.setattr(monitoring, 'get_slow_query_param',
                        mock.Mock(side_effect=lambda x: mock_get_slow_query_param(param=x)))
    monkeypatch.setattr(monitoring, 'get_self_optimization',
                        mock.Mock(side_effect=lambda x: mock_get_self_optimization(param=x)))


configs = configparser.ConfigParser()
configs.add_section('detection_threshold')
for option, value, _, _ in DynamicParams.__default__.get('detection_threshold'):
    configs.set('detection_threshold', option, str(value))

configs.add_section('slow_query_threshold')
for option, value, _, _ in DynamicParams.__default__.get('slow_query_threshold'):
    configs.set('slow_query_threshold', option, str(value))

configs.add_section('self_optimization')
for option, value, _, _ in DynamicParams.__default__.get('self_optimization'):
    configs.set('self_optimization', option, str(value))


def mock_get_detection_threshold(param):
    return configs.getfloat('detection_threshold', param)


def mock_get_slow_query_param(param):
    return configs.getfloat('slow_query_threshold', param)


def mock_get_self_optimization(param):
    return configs.getfloat('self_optimization', param)


def test_get_sequence_max_value():
    assert query_info_source._get_sequence_max_value(os_disk_usage_seq, 0) == 0
    assert query_info_source._get_sequence_max_value(os_disk_usage_seq, 2) == 0.50
    assert query_info_source._get_sequence_max_value(os_disk_usage_seq, -1) == 0.5


def test_get_sequence_values():
    expect = [[0.5, 0.3, 0.2], [0, 0, 0], [0.50, 0.30, 0.20]]
    for index, item in enumerate([-1, 0, 2]):
        result = query_info_source._get_sequence_values(os_disk_usage_seq, item)
        assert all(result[i] == expect[index][i] for i in range(len(expect[index])))


qc_tsdb_rpc = QueryContextFromTSDBAndRPC(slow_query_instance)
qc_driver = QueryContextFromDriver(slow_query_instance)
feature_generator = QueryFeature(qc_tsdb_rpc)


def test_acquire_from_tsdb():
    feature_generator.query_context.acquire_database_info()
    feature_generator.query_context.acquire_tables_structure_info()
    feature_generator.query_context.acquire_system_info()
    feature_generator.query_context.acquire_network_info()
    feature_generator.query_context.acquire_rewritten_sql()
    feature_generator.query_context.acquire_plan_parse()
    feature_generator.query_context.acquire_wait_event_info()


def test_acquire_index_analysis_info_from_rpc(monkeypatch):
    monkeypatch.setattr(AgentProxy, 'call', mock.MagicMock())
    qc_tsdb_rpc.acquire_index_analysis_info()


def test_acquire_from_driver(monkeypatch):
    qc_driver.driver = Driver()
    monkeypatch.setattr(qc_driver.driver, 'query', mock.MagicMock())
    feature_generator_driver = QueryFeature(qc_driver)
    feature_generator_driver.initialize_metrics()


def test_sql_type():
    assert feature_generator.select_type is True
    assert feature_generator.update_type is False
    assert feature_generator.delete_type is False
    assert feature_generator.insert_type is False


def test_table_structure():
    table_info = TableStructure()
    table_info.db_host = slow_query_instance.db_host
    table_info.db_port = slow_query_instance.db_port
    table_info.db_name = slow_query_instance.db_name
    table_info.schema_name = 'schema1'
    table_info.table_name = 'table1'
    table_info.live_tuples = 2000
    table_info.dead_tuples = 1000
    table_info.table_size = 60
    table_info.dead_rate = 0.5
    table_info.index = {'c1': [], 'c2': [], 'c3': [], 'c4': []}
    table_info.vacuum_delay = 10
    table_info.analyze_delay = 10
    table_info.data_changed_delay = 1
    table_info.tuples_diff = 1001
    feature_generator.table_structure = [table_info]
    assert feature_generator.large_table is True
    assert feature_generator.many_dead_tuples is True
    assert feature_generator.update_large_data is True
    assert feature_generator.insert_large_data is True
    assert feature_generator.delete_large_data is True
    slow_query_instance.query = 'update schema1.table1 set age=30 where id=3'
    assert feature_generator.too_many_index is False
    assert feature_generator.lack_of_statistics is True
    feature_generator.unused_index_info = {'schema1:table1': 'c1'}
    feature_generator.redundant_index_info = {'schema1:table1': 'c2'}
    assert feature_generator.unused_and_redundant_index is True


def test_lock_contention():
    wait_events = WaitEvents()
    wait_events.lock_event_list.append(WaitEventItem('LOCK_EVENT', 'transactionid', '100000'))
    wait_events.lock_event_list.append(WaitEventItem('LOCK_EVENT', 'partition_seq', '2000000'))
    wait_events.lock_event_list.append(WaitEventItem('LOCK_EVENT', 'virtualxid', '2000000'))
    feature_generator.wait_event_info = wait_events

    assert feature_generator.lock_contention is True
    assert feature_generator.detail.get(
        'lock_contention') == 'SQL was blocked by lock event, detail: partition_seq-2000000us; virtualxid-2000000us'


def test_abnormal_plan_time():
    slow_query_instance.n_soft_parse = 1
    slow_query_instance.n_hard_parse = 2
    slow_query_instance.plan_time = 70
    slow_query_instance.duration_time = 100
    assert feature_generator.abnormal_plan_time is True
    assert feature_generator.detail.get(
        'abnormal_plan_time') == 'There exists some hard parses in the execution plan generation process'
    assert feature_generator.suggestion.get('abnormal_plan_time') == 'Modify business to support PBE'


def test_workload_resource_contention():
    memory_detail = TotalMemoryDetail()
    memory_detail.max_process_memory = 10
    memory_detail.process_used_memory = 9
    memory_detail.max_dynamic_memory = 10
    memory_detail.dynamic_used_memory = 9
    memory_detail.other_used_memory = 5121
    feature_generator.total_memory_detail = memory_detail

    database_info = DatabaseInfo()
    database_info.tps = 1000
    database_info.connection = 1
    database_info.thread_pool_rate = 0.96
    feature_generator.database_info = database_info

    pg_setting1 = PgSetting()
    pg_setting1.setting = 10
    pg_setting2 = PgSetting()
    pg_setting2.setting = 10
    feature_generator.pg_setting_info = {'max_connections': pg_setting1, 'enable_thread_pool': pg_setting2}

    system_info = SystemInfo()
    system_info.db_cpu_usage = [0.9]
    system_info.db_mem_usage = [0.9]
    system_info.disk_usage = [0.8]
    system_info.user_cpu_usage = [0.9]
    system_info.system_mem_usage = [0.9]
    system_info.process_fds_rate = [0.9]
    feature_generator.system_info = system_info

    assert feature_generator.workload_contention is True
    assert feature_generator.os_resource_contention is True
    assert feature_generator.cpu_resource_contention is True
    assert feature_generator.memory_resource_contention is True
    assert feature_generator.io_resource_contention is False


def test_database_wait_event():
    wait_events = WaitEvents()
    wait_events.wait_event_list.append(WaitEventItem('IO_EVENT', 'DataFileFlush', '200000'))
    wait_events.wait_event_list.append(WaitEventItem('STATUS', 'acquire lwlock', '2000000'))
    feature_generator.wait_event_info = wait_events

    assert feature_generator.database_wait_event is True
    assert feature_generator.detail.get(
        'wait_event') == 'SQL was blocked by wait event, detail: acquire lwlock-2000000us'


def test_other():
    assert feature_generator.disk_spill is True
    assert feature_generator.vacuum_event is False
    assert feature_generator.analyze_event is False
    feature_generator.recommend_index_info = 'c1'
    assert feature_generator.missing_index is True
    feature_generator.rewritten_sql_info = 'xxx'
    assert feature_generator.abnormal_sql_structure is True
    assert feature_generator.heavy_scan_operator is False
    assert feature_generator.complex_boolean_expression is False
    assert feature_generator.string_matching is False
    assert feature_generator.complex_execution_plan is False
    assert feature_generator.poor_join_performance is False
    assert feature_generator.poor_aggregation_performance is False
    assert feature_generator.risk_information is True
