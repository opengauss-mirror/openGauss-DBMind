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
import configparser
from unittest import mock

import numpy as np
import pytest

from dbmind.app import monitoring
from dbmind.app.diagnosis.query.slow_sql import analyzer
from dbmind.app.diagnosis.query.slow_sql import query_info_source
from dbmind.app.diagnosis.query.slow_sql.featurelib import load_feature_lib, get_feature_mapper
from dbmind.app.diagnosis.query.slow_sql.significance_detection import average_base, ks_base, sum_base
from dbmind.app.diagnosis.query.slow_sql.slow_query import SlowQuery
from dbmind.metadatabase.schema.config_dynamic_params import DynamicParams

big_current_data = [10, 10, 10, 10, 10]
big_history_data = [1, 1, 1, 1, 1]

small_current_data = [2, 1, 3, 1, 0]
small_history_data = [1, 1, 1, 1, 1]

ilegal_current_data = []
ilegal_history_data = [1, 1, 1, 1, 1]

configs = configparser.ConfigParser()
configs.add_section('detection_threshold')
for option, value, _ in DynamicParams.__default__.get('detection_threshold'):
    configs.set('detection_threshold', option, str(value))

configs.add_section('slow_sql_threshold')
for option, value, _ in DynamicParams.__default__.get('slow_sql_threshold'):
    configs.set('slow_sql_threshold', option, str(value))

slow_sql_instance = SlowQuery(db_host='127.0.0.1', db_port='8080', db_name='database1', schema_name='schema1',
                              query='update schema1.table1 set age=30 where id=3', start_timestamp=1640139691000,
                              duration_time=1000, track_parameter=True, plan_time=1000, parse_time=20, db_time=2000,
                              hit_rate=0.8, fetch_rate=0.98, cpu_time=14200, data_io_time=1231243,
                              template_id=12432453234, query_plan=None,
                              sort_count=13, sort_mem_used=12.43, sort_spill_count=3, hash_count=0, hash_mem_used=0,
                              hash_spill_count=0, lock_wait_count=10, lwlock_wait_count=20, n_returned_rows=1,
                              n_tuples_returned=100000, n_tuples_fetched=0, n_tuples_deleted=0, n_tuples_inserted=0,
                              n_tuples_updated=0)
slow_sql_instance.tables_name = {'schema1': ['table1']}


def mock_get_param(param):
    return configs.getfloat('detection_threshold', param)


def mock_get_threshold(param):
    return configs.getfloat('slow_sql_threshold', param)


@pytest.fixture
def mock_get_funcntion(monkeypatch):
    monkeypatch.setattr(monitoring, 'get_param', mock.Mock(side_effect=lambda x: mock_get_param(param=x)))
    monkeypatch.setattr(monitoring, 'get_threshold', mock.Mock(side_effect=lambda x: mock_get_threshold(param=x)))


class MockedComplexQueryContext(query_info_source.QueryContext):
    def __init__(self, slow_sql_instance):
        super().__init__(slow_sql_instance)

    @staticmethod
    def acquire_pg_settings():
        return {}

    @staticmethod
    def acquire_redundant_index():
        return {}

    @staticmethod
    def acquire_fetch_interval():
        return 5

    @staticmethod
    def acquire_tables_structure_info():
        table_info = query_info_source.TableStructure()
        table_info.db_name = 'database1'
        table_info.schema_name = 'schema1'
        table_info.table_name = 'table1'
        table_info.dead_tuples = 80000
        table_info.live_tuples = 100000
        table_info.dead_rate = 0.45
        table_info.last_autovacuum = 1640139691000
        table_info.last_autoanalyze = 1640139691000
        table_info.analyze = 1640139691000
        table_info.vacuum = 1640139691000
        table_info.table_size = 1000000
        table_info.index = {'index1': ['col1'], 'index2': ['col2'], 'index3': ['col3']}
        table_info.redundant_index = ['redundant_index1', 'redundant_index2', 'redundant_index3', 'redundant_index4']
        return [table_info]

    @staticmethod
    def acquire_database_info():
        db_info = query_info_source.DatabaseInfo()
        db_info.tps = 100000
        db_info.connection = 100
        db_info.thread_pool_rate = 0.6
        return db_info

    @staticmethod
    def acquire_wait_event():
        wait_event_info = query_info_source.ThreadInfo()
        wait_event_info.status = 'IO_EVENT'
        wait_event_info.event = 'CopyFileWrite'
        return wait_event_info

    @staticmethod
    def acquire_system_info():
        system_info = query_info_source.SystemInfo()
        system_info.ioutils = {'sdm-0': 0.9}
        system_info.iowait_cpu_usage = [0.9]
        system_info.user_cpu_usage = [0.9]
        system_info.system_mem_usage = [0.9]
        system_info.disk_usage = {'sdm-0': 0.8}
        system_info.process_fds_rate = [0.8]
        system_info.io_read_delay = [1000000]
        system_info.io_write_delay = [1000000]
        return system_info

    @staticmethod
    def acquire_network_info():
        network_info = query_info_source.NetWorkInfo()
        network_info.transmit_drop = [0.9]
        network_info.receive_drop = [0.9]
        network_info.bandwidth_usage = 0.8
        return network_info

    @staticmethod
    def acquire_rewritten_sql():
        return ''

    @staticmethod
    def acquire_recommend_index():
        return 'schema: schema1, table: table1, column: id'

    @staticmethod
    def acquire_timed_task():
        timed_task_info = query_info_source.TimedTask()
        timed_task_info.job_id = 1
        timed_task_info.priv_user = 'user'
        timed_task_info.dbname = 'database1'
        timed_task_info.job_status = 1
        timed_task_info.last_start_date = 1640139688000
        timed_task_info.last_end_date = 1640139693000
        return [timed_task_info]


def test_average_base():
    check_res_1 = average_base.detect(big_current_data, big_history_data, method='bool')
    check_res_2 = average_base.detect(small_current_data, small_history_data, method='bool')
    check_res_3 = average_base.detect(ilegal_current_data, ilegal_history_data, method='bool')
    check_res_4 = average_base.detect(big_current_data, big_history_data, method='other')
    check_res_5 = average_base.detect(big_history_data, big_current_data, method='other')
    try:
        _ = average_base.detect(100, 200)
    except TypeError as execinfo:
        assert 'The format of the input data is wrong' in str(execinfo)
    try:
        _ = average_base.detect(big_current_data, big_history_data, method='inner')
    except ValueError as execinfo:
        assert 'Not supported method' in str(execinfo)
    assert check_res_1
    assert not check_res_2
    assert not check_res_3
    assert round(check_res_4, 4) == 0.9000
    assert check_res_5 == 0


def test_ks_base():
    check_res_1 = ks_base.detect(big_current_data, big_history_data)
    check_res_2 = ks_base.detect(small_current_data, small_history_data)
    check_res_3 = ks_base.detect(ilegal_current_data, ilegal_history_data)
    assert not check_res_1
    assert check_res_2
    assert not check_res_3


def test_sum_base():
    check_res_1 = sum_base.detect(big_current_data, big_history_data, method='bool')
    check_res_2 = sum_base.detect(small_current_data, small_history_data, method='bool')
    check_res_3 = sum_base.detect(ilegal_current_data, ilegal_history_data, method='bool')
    check_res_4 = sum_base.detect(big_current_data, big_history_data, method='other')
    check_res_5 = sum_base.detect(big_history_data, big_current_data, method='other')
    try:
        _ = sum_base.detect(100, 200)
    except TypeError as execinfo:
        assert 'The format of the input data is wrong' in str(execinfo)
    try:
        _ = sum_base.detect(big_current_data, big_history_data, method='inner')
    except ValueError as execinfo:
        assert 'Not supported method' in str(execinfo)
    assert check_res_1
    assert not check_res_2
    assert not check_res_3
    assert round(check_res_4, 4) == 0.9000
    assert check_res_5 == 0


def test_load_feature_lib():
    feature_lib = load_feature_lib()
    assert len(feature_lib) == 3
    assert len(feature_lib['features']) > 0
    assert len(feature_lib['labels']) > 0
    assert len(feature_lib['weight_matrix']) > 0


def test_get_feature_mapper():
    feature_mapping = get_feature_mapper()
    assert len(feature_mapping) == 34


def test_vector_distance():
    feature_lib = load_feature_lib()
    features, causes, weight_matrix = feature_lib['features'], feature_lib['labels'], feature_lib['weight_matrix']
    feature_instance1 = np.array(
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    feature_instance2 = np.array(
        [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    distance = analyzer._vector_distance(feature_instance1, features[0], 1, weight_matrix)
    assert round(distance, 4) == 0.871
    try:
        _ = analyzer._vector_distance(feature_instance2, features[0], 1, weight_matrix)
    except ValueError as execinfo:
        assert 'not equal' in str(execinfo)


def test_euclid_distance():
    feature1 = np.array([1, 1, 0, 0, 0])
    feature2 = np.array([0, 1, 0, 0, 0])
    distance = analyzer._euclid_distance(feature1, feature2)
    assert distance == 1.0


def test_calculate_nearest_feature():
    feature = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    nearest_feature = analyzer._calculate_nearest_feature(feature)
    assert len(nearest_feature) == 1
    assert nearest_feature[0][0] == 1
    assert nearest_feature[0][1] == 13
