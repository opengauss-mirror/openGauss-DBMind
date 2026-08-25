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
from collections import defaultdict
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import patch

from dbmind import global_vars
from dbmind.app.monitoring import regular_inspection
from dbmind.common.http import Request
from dbmind.common.types import Sequence
from dbmind.common.utils.checking import split_ip_port
from dbmind.controllers import dbmind_core
from dbmind.metadatabase import dao
from dbmind.service import dai
from dbmind.service.web import data_transformer, jsonify_utils


class TestMultipleHoursInspection(regular_inspection.MultipleHoursInspection):
    def __init__(self, instance, username="user", password="text", start=None, end=None, step=6 * 60 * 1000):
        self._report = {}
        self._username = username
        self._password = password
        self._start = start
        self._end = end
        self._step = step
        self._agent_instance = instance
        self._agent_instance_no_port = split_ip_port(self._agent_instance)[0]
        self._instances_with_port = [instance]
        self._instances_with_no_port = [split_ip_port(i)[0] for i in self._instances_with_port]
        self.data_ip_list = []


class TestCentralizeMultipleHoursInspection(regular_inspection.CentralizeMultipleHoursInspection):
    def __init__(self, instance, username="user", password="text", start=None, end=None, step=6 * 60 * 1000):
        self._report = {}
        self._username = username
        self._password = password
        self._start = start
        self._end = end
        self._step = step
        self._agent_instance = instance
        self._agent_instance_no_port = split_ip_port(self._agent_instance)[0]
        self._instances_with_port = [instance]
        self._instances_with_no_port = [split_ip_port(i)[0] for i in self._instances_with_port]
        self.data_ip_list = []


class TestDistributeMultipleHoursInspection(regular_inspection.DistributeMultipleHoursInspection):
    def __init__(self, instance, username="user", password="text", start=None, end=None, step=6 * 60 * 1000):
        self._report = {}
        self._username = username
        self._password = password
        self._start = start
        self._end = end
        self._step = step
        self._agent_instance = instance
        self._agent_instance_no_port = split_ip_port(self._agent_instance)[0]
        self._instances_with_port = [instance]
        self._instances_with_no_port = [split_ip_port(i)[0] for i in self._instances_with_port]
        self.data_ip_list = []
        self.coordinator_list, self.datanode_list, self.standby_list = [instance], [instance], [instance]
        self.coordinator_no_port = [split_ip_port(i)[0] for i in self.coordinator_list]
        self.datanode_no_port = [split_ip_port(i)[0] for i in self.datanode_list]
        self.standby_no_port = [split_ip_port(i)[0] for i in self.standby_list]


def mock_get_metric_sequence(metric_name, start_time, end_time, step):
    class MockFetcher(dai.LazyFetcher):

        def _read_buffer(self):
            instance_label = dai.get_metric_source_flag(self.metric_name)
            self.step = self.step or 5

            timestamps = [1691510400000]
            if instance_label in self.labels_like:
                instance = split_ip_port(self.labels_like.pop(instance_label))[0]
            elif instance_label in self.labels:
                instance = self.labels[instance_label]
            else:
                instance = "some_ip:some_port"

            self.labels[instance_label] = instance

            if metric_name == 'node_memory_MemTotal_bytes':
                values = [269852749824]
            elif metric_name == 'pg_total_memory_detail_mbytes':
                name_list = [
                    'dynamic_used_memory', 'max_dynamic_memory',
                    'dynamic_used_shrctx', 'process_used_memory',
                    'max_process_memory', 'other_used_memory'
                ]
                value_list = [628, 3442, 231, 2115, 10240, 0]
                seq_list = []
                for i, name in enumerate(name_list):
                    self.labels['type'] = name
                    values = [value_list[i]]
                    seq = Sequence(timestamps=timestamps,
                                   values=values,
                                   name=self.metric_name,
                                   labels=self.labels,
                                   step=self.step)
                    seq_list.append(seq)
                return seq_list
            elif metric_name == 'opengauss_log_ffic':
                self.labels['unique_sql_id'] = 123
                self.labels['debug_sql_id'] = 133
                values = [1]
            elif metric_name == 'pg_database_size_bytes':
                self.labels['datname'] = 'dbmind'
                values = [1024]
            elif metric_name == 'pg_node_info_uptime':
                self.labels['from_instance'] = '127.0.0.1:12345'
                self.labels['datapath'] = '/media/sdd/lk_new/data'
                self.labels['log_directory'] = 'pg_log'
                values = [7215.672444]
            elif metric_name == 'os_disk_usage':
                self.labels['from_instance'] = '127.0.0.1'
                self.labels['mountpoint'] = '/'
                self.labels['fstype'] = 'ext4'
                values = [0.5837577127293705]
            elif metric_name == 'opengauss_total_connection':
                values = [10]
            elif metric_name == 'opengauss_active_connection':
                values = [3]
            else:
                values = [1024]

            seq = Sequence(timestamps=timestamps,
                           values=values,
                           name=self.metric_name,
                           labels=self.labels,
                           step=self.step)
            return [seq]

    return MockFetcher(metric_name, start_time, end_time, step)


def mock_get_latest_metric_sequence(metric_name):
    class MockFetcher(dai.LazyFetcher):

        def _read_buffer(self):
            instance_label = dai.get_metric_source_flag(self.metric_name)
            self.step = self.step or 5

            timestamps = [1691510400000]
            if instance_label in self.labels:
                instance = self.labels[instance_label]
            else:
                instance = "some_ip:some_port"

            self.labels[instance_label] = instance
            self.labels['vartype'] = 'interger'
            if metric_name == 'node_memory_MemTotal_bytes':
                values = [269852749824]
            elif metric_name == 'pg_settings_setting':
                name_list = [
                    'max_process_memory', 'shared_buffers', 'work_mem'
                ]
                value_list = [12582912, 4096, 64]
                seq_list = []
                for i, name in enumerate(name_list):
                    values = [value_list[i]]
                    self.labels['name'] = name
                    seq = Sequence(timestamps=timestamps,
                                   values=values,
                                   name=self.metric_name,
                                   labels=self.labels,
                                   step=self.step)
                    seq_list.append(seq)
                return seq_list
            else:
                values = [1024]

            seq = Sequence(timestamps=timestamps,
                           values=values,
                           name=self.metric_name,
                           labels=self.labels,
                           step=self.step)
            return [seq]

    return MockFetcher(metric_name)


sys_res_insp_item_dict = {
    'os_cpu_usage': {
        'cpu_user': {},
        'cpu_iowait': {}
    },
    'os_disk_usage': {},
    'os_mem_usage': {},
    'os_disk_ioutils': {},
    'network_packet_loss': {}
}
ins_statu_insp_item_dict = {'component_error': {}}
dat_res_insp_item_dict = {
    'data_directory': {},
    'log_directory': {},
    'db_size': {}
}
dat_per_insp_item_dict = {
    'buffer_hit_rate': {},
    'user_login_out': {
        'login': {},
        'logout': {}
    },
    'active_session_rate': {},
    'thread_pool': {},
    'db_latency': {
        'p95': {},
        'p80': {}
    },
    'db_transaction': {
        'commit': {},
        'rollback': {}
    },
    'db_tmp_file': {},
    'db_exec_statement': {
        'select': {},
        'update': {},
        'insert': {},
        'delete': {}
    },
    'db_deadlock': {},
    'db_tps': {
        'tps': {},
        'qps': {}
    },
    'xmin_stuck': {},
    'xlog_accumulate': {},
    'db_top_query': {},
    'log_error_check': {},
    'long_transaction': {}
}
dia_opt_insp_item_dict = {
    'dynamic_memory': {
        'dynamic_used_memory': {},
        'dynamic_used_shrctx': {}
    },
    'process_memory': {},
    'other_memory': {},
    'core_dump': {}
}
full_inspection_item_dict = {
    'system_resource': sys_res_insp_item_dict,
    'instance_status': ins_statu_insp_item_dict,
    'database_resource': dat_res_insp_item_dict,
    'database_performance': dat_per_insp_item_dict,
    'diagnosis_optimization': dia_opt_insp_item_dict,
    'conclusion': {}
}


def test_inspector():
    instance = '127.0.0.1:12345'
    with patch('dbmind.global_vars.agent_proxy') as mock_agent:
        mock_agent.context.return_value = global_vars.agent_proxy.context.Inner('127.0.0.1:12345')
        mock_agent.current_cluster_instances.return_value = ['127.0.0.1:12345']
        mock_agent.agents = {'127.0.0.1:12345': 0}
        inspector = regular_inspection.MultipleHoursInspection(instance, "user", "text")
        assert inspector._agent_instance == instance


def test_get_instance_cluster_type(monkeypatch):
    instance = '127.0.0.1:12345'
    monkeypatch.setattr(regular_inspection,
                        'get_cluster_type_view',
                        mock.Mock(return_value='centralize'))
    assert regular_inspection.get_instance_cluster_type(instance, "user", "text") == 'centralize'

    monkeypatch.setattr(regular_inspection,
                        'get_cluster_type_view',
                        mock.Mock(return_value='distribute'))
    assert regular_inspection.get_instance_cluster_type(instance, "user", "text") == 'distribute'

    monkeypatch.setattr(regular_inspection,
                        'get_cluster_type_view',
                        mock.Mock(return_value=None))
    assert regular_inspection.get_instance_cluster_type(instance, "user", "text") is None


def test_get_instance_type_dict():
    instance = '127.0.0.1:12345'
    node_info_list = [
        {'node_type': 'P', 'node_host': '127.0.0.2', 'node_port': '12345'},
        {'node_type': 'C', 'node_host': '127.0.0.1', 'node_port': '12345'}
    ]
    type_dict = {'C': {'127.0.0.1:12345'}, 'D': set(), 'S': set()}
    instance_type_dict = {'127.0.0.1:12345': 'C'}
    with patch('dbmind.global_vars.agent_proxy') as mock_agent:
        mock_agent.context.return_value = global_vars.agent_proxy.context.Inner(
            '127.0.0.1:12345')
        mock_agent.call.return_value = node_info_list
        assert regular_inspection.get_instance_type_dict(instance, "user", "text") == (type_dict, instance_type_dict)


def test_extend_instance_type():
    type_dict = {'C': set(), 'D': set(), 'S': set()}
    instance_type_dict = {'127.0.0.1:12345': 'C', '127.0.0.1': 'P', '127.0.0.1:15432': 'P'}
    regular_inspection.ip_dict = {'127.0.0.2': ['127.0.0.2'], '127.0.0.1': ['127.0.0.1']}
    regular_inspection.extend_instance_type(type_dict, instance_type_dict)


def test_get_instance_type_management():
    regular_inspection.ip_dict = {
        '127.0.0.2': ['127.0.0.2', '127.0.0.4'],
        '127.0.0.1': ['127.0.0.1'],
        '127.0.0.3': ['127.0.0.3']
    }
    instance_type_dict = {'127.0.0.1:12345': 'C', '127.0.0.2:12345': 'D', '127.0.0.3:12345': 'S'}
    management_ip_list = ['127.0.0.1:12345', '127.0.0.2:12345', '127.0.0.3:12345']
    res = regular_inspection.get_instance_type_management(management_ip_list, instance_type_dict)
    assert res == (['127.0.0.1:12345'], ['127.0.0.2:12345'], ['127.0.0.3:12345'])


def test_get_increase_status(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    timestamps = [1691510400000]
    values = [True]
    seq = Sequence(timestamps, values)
    monkeypatch.setattr(regular_inspection._continuous_increasing_detector,
                        'fit_predict', mock.Mock(return_value=seq))
    assert inspector.get_increase_status(seq) is False


def test_get_warning_dict():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    timestamps = [
        1691510400000, 1691511400000, 1691512400000, 1691513400000,
        1691514400000, 1691515400000, 1691516400000, 1691517400000,
        1691518400000
    ]
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    labels = {'fstype': 'ext2'}
    seq = Sequence(timestamps, values, labels=labels)
    inspection_items = {
        'increase': True,
        'threshold': {
            'upper_threshold': 7,
            'lower_threshold': 3
        },
        'forecast': {
            'upper_threshold': 7,
            'lower_threshold': 3,
            'forecast_time': 24
        },
        'ftype': True
    }
    inspector.get_warning_dict(seq, inspection_items)
    inspection_items = {
        'increase': True,
        'threshold': {
            'upper_threshold': 20,
            'lower_threshold': 3
        },
        'forecast': {
            'upper_threshold': 20,
            'lower_threshold': 3,
            'forecast_time': 24
        },
        'ftype': True
    }
    warning_dict = {
        'threshold_warning': [
            {
                'risk': 'lower',
                'timestamp': 1691510400000,
                'value': 1
            },
            {
                'risk': 'lower',
                'timestamp': 1691511400000,
                'value': 2
            }
        ],
        'ftype_warning': True
    }
    assert inspector.get_warning_dict(seq, inspection_items) == (True, warning_dict)


def test_get_instance_result_no_port(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    mem_warning_info = inspector.get_warning_info(True, [0.7, 0.0, None], [24 * 60, 0.8, 0.0])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1': {
            'statistic': {
                'max': 1024,
                'min': 1024,
                'avg': 1024.0,
                'the_95th': 1024.0
            },
            'warnings': {
                'threshold_warning': [{
                    'risk': 'upper',
                    'timestamp': 1691510400000,
                    'value': 1024
                }]
            },
            'timestamps': (1691510400000,),
            'data': [1024]
        }
    }
    assert inspector.get_instance_result_no_port('os_mem_usage', mem_warning_info) == (1, instance_dict)


def test_get_instance_result_port(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    active_warning_info = inspector.get_warning_info(False, [1.0, 0.8, None])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1:12345': {
            'statistic': {
                'max': 1024,
                'min': 1024,
                'avg': 1024.0,
                'the_95th': 1024.0
            },
            'warnings': {
                'threshold_warning': [{
                    'risk': 'upper',
                    'timestamp': 1691510400000,
                    'value': 1024
                }]
            },
            'timestamps': (1691510400000,),
            'data': [1024]
        }
    }
    assert inspector.get_instance_result_port(
        'pg_session_active_session_rate',
        active_warning_info) == (1, instance_dict)


def test_get_instance_result_filter_like(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    mem_warning_info = inspector.get_warning_info(True, [0.7, 0.0, None],
                                                  [24 * 60, 0.8, 0.0])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1': {
            'statistic': {
                'max': 1024,
                'min': 1024,
                'avg': 1024.0,
                'the_95th': 1024.0
            },
            'warnings': {
                'threshold_warning': [{
                    'risk': 'upper',
                    'timestamp': 1691510400000,
                    'value': 1024
                }]
            },
            'timestamps': (1691510400000,),
            'data': [1024]
        }
    }
    assert inspector.get_instance_result_filter_like(
        'os_mem_usage', mem_warning_info) == (1, instance_dict)


def test_get_sys_disk_usage(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    disk_warning_info = inspector.get_warning_info(False, [0.0, 0.8, None])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1': {
            'statistic': {
                'max': 0.5837577127293705,
                'min': 0.5837577127293705,
                'avg': 0.5837577127293705,
                'the_95th': 0.5837577127293705
            },
            'warnings': {},
            'timestamps': (1691510400000,),
            'data': [0.5837577127293705]
        }
    }
    assert inspector.get_sys_disk_usage('os_disk_usage', disk_warning_info) == (0, instance_dict)


def test_match_paths():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    mount = '/'
    directory = '/media/sdd/lk_new/data'
    assert inspector.match_paths(directory, mount) == 0.5
    mount = '/media/sdd'
    assert inspector.match_paths(directory, mount) == 2


def test_get_database_disk_usage(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    data_dir_warning_info = inspector.get_warning_info(
        False,
        [0.8, 0.0, None],
        [24 * 60, 0.8, 0.0],
        True
    )
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    assert inspector.get_database_disk_usage(
        'data_directory',
        'os_disk_usage',
        data_dir_warning_info
    ) == (0, {})


def test_get_db_result(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    db_size_warning_info = inspector.get_warning_info()
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        'dbmind': {
            'statistic': {
                'max': 1024,
                'min': 1024,
                'avg': 1024.0,
                'the_95th': 1024.0
            },
            'warnings': {},
            'timestamps': (1691510400000,),
            'data': [1024]
        }
    }
    assert inspector.get_db_result('pg_database_size_bytes',
                                   db_size_warning_info) == (0, instance_dict)
    dis_inspector = TestDistributeMultipleHoursInspection(instance)
    dis_instance_dict = {
        '127.0.0.1:12345': instance_dict
    }
    assert dis_inspector.get_db_result('pg_database_size_bytes', db_size_warning_info) == (0, dis_instance_dict)


def test_get_packet_loss_ins(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    network_warning_info = inspector.get_warning_info(False, [0.05, 0.0, None])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
    }
    assert inspector.get_network_packet_loss('opengauss_ping_packet_rate', network_warning_info) == (0, instance_dict)


def test_get_ip_state_from_sequence():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    timestamps = [1691510400000]
    values = [1]
    labels = {'cn_state': '[{"ip": "127.0.0.1", "state": "Down"}]'}
    normal_list = []
    seq = Sequence(timestamps, values, labels=labels)
    instance_dict = {'127.0.0.1': 'Down'}
    assert inspector.get_ip_state_from_sequence(seq, 'cn_state',
                                                normal_list) == (1,
                                                                 instance_dict)


def test_parse_opengauss_cluster_state(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    timestamps = [1691510400000]
    values = [1]
    labels = {'cn_state': 'ext2'}
    seq = Sequence(timestamps, values, labels=labels)
    monkeypatch.setattr(inspector, 'get_ip_state_from_sequence',
                        mock.Mock(return_value=(1, {})))
    instance_dict = {
        'cms_state': {},
        'dn_state': {},
        'etcd_state': {},
        'central_cn_state': {},
        'cn_state': {},
        'gtm_state': {}
    }
    assert inspector.parse_opengauss_cluster_state(seq) == (6, instance_dict)


def test_get_node_status_from_metadatabase(monkeypatch):
    mock_cluster_diagnosis_records = mock.Mock()
    mock_cluster_diagnosis_records.return_value = [1693926720000, 1693929600000], [0, 1]
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(regular_inspection, 'get_cluster_diagnosis_records',
                        mock_cluster_diagnosis_records)
    expected_dict = {
        "127.0.0.1": {
            "role": "dn",
            "status": {
                "timestamps": [1693926720000, 1693929600000],
                "value": [0, 1]
            },
            "warnings": {
                "timestamps": [1693929600000],
                "value": [1]
            }
        }
    }
    dis_expected_dict = {
        'dn': expected_dict,
        'cn': {
            "127.0.0.1": {
                "role": "cn",
                "status": {
                    "timestamps": [1693926720000, 1693929600000],
                    "value": [0, 1]
                },
                "warnings": {
                    "timestamps": [1693929600000],
                    "value": [1]
                }
            }
        }
    }
    assert inspector.get_node_status_from_metadatabase({'target': 0}) == (1, expected_dict)
    dis_inspector = TestDistributeMultipleHoursInspection(instance)
    assert dis_inspector.get_node_status_from_metadatabase({'target': 0}) == (2, dis_expected_dict)


def test_get_top_querys():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    dis_inspector = TestDistributeMultipleHoursInspection(instance)
    with patch('dbmind.global_vars.agent_proxy') as mock_agent:
        mock_agent.context.return_value = global_vars.agent_proxy.context.Inner(
            '127.0.0.1:12345')
        mock_agent.call.return_value = []
        mock_agent.agents = {'127.0.0.1:12345': 0}
        assert inspector.get_top_querys() == (0, [])
        assert dis_inspector.get_top_querys() == (0, {'127.0.0.1:12345': []})


def test_long_transaction():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    dis_inspector = TestDistributeMultipleHoursInspection(instance)
    with patch('dbmind.global_vars.agent_proxy') as mock_agent:
        mock_agent.context.return_value = global_vars.agent_proxy.context.Inner(
            '127.0.0.1:12345')
        mock_agent.call.return_value = []
        mock_agent.agents = {'127.0.0.1:12345': 0}
        assert inspector.long_transaction() == (0, [])
        assert dis_inspector.long_transaction() == (0, {'127.0.0.1:12345': []})


def test_get_core_dump_detail(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1': {
            'count': 1,
            'timestamps': [1691510400000],
            'data': [1]
        }
    }
    assert inspector.get_core_dump_detail() == (1, instance_dict)


def test_get_label_info_use(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    other_warning_info = inspector.get_warning_info(True,
                                                    [0.0, 20 * 1024 * 1024, None])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1:12345': {
            'statistic': {
                'max': 628,
                'min': 628,
                'avg': 628.0,
                'the_95th': 628.0
            },
            'warnings': {},
            'timestamps': (1691510400000,),
            'data': [628]
        }
    }
    assert inspector.get_label_info_use('pg_total_memory_detail_mbytes',
                                        'other_used_memory',
                                        other_warning_info) == (0,
                                                                instance_dict)


def test_get_label_info_rate(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    dynamic_warning_info = inspector.get_warning_info(
        threshold_list=[0.8, 0.0, None])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1:12345': {
            'statistic': {
                'max': 1.0,
                'min': 1.0,
                'avg': 1.0,
                'the_95th': 1.0
            },
            'warnings': {
                'threshold_warning': [{
                    'risk': 'upper',
                    'timestamp': 1691510400000,
                    'value': 1.0
                }]
            },
            'timestamps': (1691510400000,),
            'data': [1.0]
        }
    }
    assert inspector.get_label_info_rate(
        'pg_total_memory_detail_mbytes', 'dynamic_used_memory',
        'max_dynamic_memory', dynamic_warning_info) == (1, instance_dict)


def test_get_active_session_rate(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    active_warning_info = inspector.get_warning_info(False, [1.0, 0.8, None])
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1:12345': {
            'statistic': {
                'max': 0.3,
                'min': 0.3,
                'avg': 0.3,
                'the_95th': 0.3
            },
            'warnings': {
                'threshold_warning': [{
                    'risk': 'lower',
                    'timestamp': 1691510400000,
                    'value': 0.3
                }]
            },
            'timestamps': (1691510400000,),
            'data': [0.3]
        }
    }
    assert inspector.get_active_session_rate(active_warning_info) == (
        1, instance_dict)


def test_get_log_error(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    instance_dict = {
        '127.0.0.1': {
            'error_count': 60,
            'error_types': {
                'bind_ip_failed': 1,
                'cms_cn_down': 1,
                'cms_heartbeat_timeout': 1,
                'cms_heartbeat_timeout_restart': 1,
                'cms_phonydead_restart': 1,
                'cms_read_only': 1,
                'cms_restart_pending': 1,
                'cn_restart_time_exceed': 1,
                'cn_status': 1,
                'deadlock_count': 1,
                'dn_ping_standby': 1,
                'dn_status': 1,
                'dn_writable_failed': 1,
                'errors_rate': 1,
                'etcd_auth_failed': 1,
                'etcd_be_killed': 1,
                'etcd_disk_full': 1,
                'etcd_io_overload': 1,
                'etcd_not_connect_dial_tcp': 1,
                'etcd_overload': 1,
                'etcd_restart': 1,
                'etcd_sync_timeout': 1,
                'ffic': 1,
                'gtm_disconnected_to_primary': 1,
                'gtm_panic': 1,
                'gtm_status': 1,
                'login_denied': 1,
                'node_restart': 1,
                'node_start': 1,
                'panic': 1
            }
        }
    }
    assert inspector.get_log_error() == (1, instance_dict)


def test_get_recommend_params():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    best_param = 1
    cur_param = 1
    inspector.get_recommend_params(best_param, cur_param)
    cur_param = 0.8
    inspector.get_recommend_params(best_param, cur_param)
    vartype = 'bool'
    best_param = False
    cur_param = False
    inspector.get_recommend_params(best_param, cur_param, vartype)
    best_param = True
    inspector.get_recommend_params(best_param, cur_param, vartype)
    cur_param = True
    instance_dict = {
        'opt_param': True,
        'cur_param': True,
        'recommend_scope': [],
        'warning': False
    }
    assert inspector.get_recommend_params(best_param, cur_param,
                                          vartype) == (False, instance_dict)


def test_get_guc_params(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(dai, 'get_latest_metric_value',
                        mock_get_latest_metric_sequence)
    monkeypatch.setattr(inspector, 'get_recommend_params',
                        mock.Mock(return_value=(True, {})))
    instance_dict = {
        '127.0.0.1:12345': {
            'max_process_memory': {},
            'shared_buffers': {},
            'work_mem': {}
        }
    }
    assert inspector.get_guc_params() == (1, instance_dict)


def test_system_resource(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(inspector, 'get_inspection_result_single',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'get_inspection_result_list',
                        mock.Mock(return_value=({})))
    inspector.system_resource([
        "os_cpu_usage", "os_disk_usage", "os_mem_usage", "os_disk_ioutils",
        "network_packet_loss"
    ], {})
    assert inspector.system_resource([
        "os_cpu_usage", "os_disk_usage", "os_mem_usage", "os_disk_ioutils",
        "network_packet_loss"
    ], {}) == {}


def test_instance_status(monkeypatch):
    instance = '127.0.0.1:12345'
    ins_statu_insp_item = ["component_error"]
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(inspector, 'get_inspection_result_single',
                        mock.Mock(return_value=({'component_error': {}})))
    instance_dict = {'component_error': {}}
    assert inspector.instance_status(ins_statu_insp_item, {}) == instance_dict


def test_database_resource(monkeypatch):
    instance = '127.0.0.1:12345'
    dat_res_insp_item = ["data_directory", "log_directory", "db_size"]
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(inspector, 'get_inspection_result_single',
                        mock.Mock(return_value=({})))
    assert inspector.database_resource(dat_res_insp_item, {}) == {}


def test_database_performance(monkeypatch):
    instance = '127.0.0.1:12345'
    dat_per_insp_item = [
        "buffer_hit_rate", "user_login_out", "active_session_rate",
        "log_error_check", "thread_pool", "db_latency", "db_transaction",
        "db_tmp_file", "db_exec_statement", "db_deadlock", "db_tps",
        "db_top_query", "long_transaction", "xmin_stuck", "xlog_accumulate"
    ]
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(inspector, 'get_inspection_result_single',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'get_inspection_result_list',
                        mock.Mock(return_value=({})))
    assert inspector.database_performance(dat_per_insp_item, {}) == {}


def test_diagnosis_optimization(monkeypatch):
    instance = '127.0.0.1:12345'
    dia_opt_insp_item = [
        "core_dump", "dynamic_memory", "process_memory", "other_memory",
        "guc_params", "index_advisor"
    ]
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(inspector, 'get_inspection_result_list',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'get_inspection_result_single',
                        mock.Mock(return_value=({})))
    assert inspector.diagnosis_optimization(dia_opt_insp_item, {}) == {}


def test_get_inspection_conclusion():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    score_dict = {
        'full_score': 0,
        'health_score': 0,
        'count': {}
    }
    inspector.get_inspection_conclusion(score_dict)
    score_dict['full_score'] = 100
    inspector.get_inspection_conclusion(score_dict)
    score_dict['health_score'] = 65
    inspector.get_inspection_conclusion(score_dict)
    score_dict['health_score'] = 77
    inspector.get_inspection_conclusion(score_dict)
    score_dict['health_score'] = 93
    inspector.get_inspection_conclusion(score_dict)
    score_dict['health_score'] = 100
    conclusion_dict = {
        'full_score': 100,
        'health_score': 100,
        'health_status': 'perfect',
        'top3': []
    }
    assert inspector.get_inspection_conclusion(score_dict) == conclusion_dict


def test_intelligent_inspection(monkeypatch):
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    mock_cluster_diagnosis_records = mock.Mock()
    mock_cluster_diagnosis_records.return_value = [1693926720000, 1693929600000], [0, 1]
    monkeypatch.setattr(regular_inspection, 'get_cluster_diagnosis_records',
                        mock_cluster_diagnosis_records)
    monkeypatch.setattr(inspector, 'get_top_querys',
                        mock.Mock(return_value=(0, {})))
    monkeypatch.setattr(inspector, 'get_log_error',
                        mock.Mock(return_value=(0, {})))
    monkeypatch.setattr(inspector, 'long_transaction',
                        mock.Mock(return_value=(0, {})))
    monkeypatch.setattr(inspector, 'get_core_dump_detail',
                        mock.Mock(return_value=(0, {})))
    monkeypatch.setattr(inspector, 'get_guc_params',
                        mock.Mock(return_value=(0, {})))
    inspection_items = dbmind_core.InspectionItems(system_resource=[],
                                                   instance_status=[],
                                                   database_resource=[],
                                                   database_performance=[],
                                                   diagnosis_optimization=[])
    monkeypatch.setattr(inspector, 'system_resource',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'instance_status',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'database_resource',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'database_performance',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'diagnosis_optimization',
                        mock.Mock(return_value=({})))
    inspector.intelligent_inspection(inspection_items)
    instance_dict = {
        'system_resource': {},
        'instance_status': {},
        'database_resource': {},
        'database_performance': {},
        'diagnosis_optimization': {},
        'conclusion': {
            'health_score': 0,
            'full_score': 0,
            'health_status': 'perfect',
            'top3': []
        }
    }
    assert inspector.intelligent_inspection(inspection_items) == instance_dict


def test_get_customize_warning_info():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    inspection_items = [{"component_error": True, "db_latency": True, "os_mem_usage": False,
                         "os_cpu_usage": {"cpu_user": True, "cpu_iowait": {}}, "thread_pool": {}, "user_login_out": {}}]
    inspector.get_customize_warning_info(inspection_items)
    try:
        inspection_items = [{"os_disk_ioutils": 111}]
        inspector.get_customize_warning_info(inspection_items)
    except ValueError as e:
        assert str(e) == 'The customized warning info of os_disk_ioutils inspection must be dict.'
    try:
        inspection_items = [{"db_latency": {"login": {}}}]
        inspector.get_customize_warning_info(inspection_items)
    except ValueError as e:
        assert str(e) == 'The warning info of db_latency inspection is wrong.'
    try:
        inspection_items = [{"os_cpu_usage": {"cpu_user": True, "cpu_iowait": 111}}]
        inspector.get_customize_warning_info(inspection_items)
    except ValueError as e:
        assert str(e) == 'The sub customized warning info of os_cpu_usage inspection must be dict.'


def test_get_warning_params():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)

    try:
        inspection_item = "buffer_hit_rate"
        inspection_warning = {"increase": 111}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The buffer_hit_rate inspection does not support increase warning.'

    inspection_item = "os_mem_usage"
    inspection_warning = {"increase": 111}
    try:
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The increase warning type of os_mem_usage inspection must be bool.'
    inspection_warning = {"increase": True}
    inspector.get_warning_params(inspection_item, inspection_warning)
    try:
        inspection_item = "core_dump"
        inspection_warning = {"threshold": 111}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The {} inspection does not support threshold warning.'.format(inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"threshold": 111}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The threshold warning type of {} inspection must be list.'.format(inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"threshold": ['123']}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The type of {} inspection threshold warning info must be int or float.'.format(
            inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"threshold": [0.0]}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(
            e) == 'The threshold warning info of {} inspection must be [lower_threshold, upper_threshold].'.format(
            inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"threshold": [0.1, 0.0]}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'For {} inspection threshold warning info, the first num must lower than last.'.format(
            inspection_item)
    inspection_warning = {"threshold": [0.0, 0.1]}
    inspector.get_warning_params(inspection_item, inspection_warning)
    try:
        inspection_item = "core_dump"
        inspection_warning = {"forecast": 111}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The {} inspection does not support forecast warning.'.format(inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"forecast": 111}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The forecast warning type of {} inspection must be list.'.format(inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"forecast": ['123']}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The type of {} inspection forecast warning info must be int or float.'.format(inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"forecast": [0.0]}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The forecast warning info of {} inspection must be ' \
                         '[forecast_time, lower_threshold, upper_threshold].'.format(inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"forecast": [60 * 60, 0.1, 0.0]}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The {} inspection forecast time ranges from 0 to 2880, the unit is minute.'.format(
            inspection_item)
    try:
        inspection_item = "os_mem_usage"
        inspection_warning = {"forecast": [24, 0.1, 0.0]}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'For {} inspection forecast warning info, the mid num must lower than last.'.format(
            inspection_item)
    inspection_warning = {"forecast": [24, 0.0, 0.1]}
    inspector.get_warning_params(inspection_item, inspection_warning)
    try:
        inspection_item = "buffer_hit_rate"
        inspection_warning = {"ftype": 111}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The buffer_hit_rate inspection does not support ftype warning.'
    try:
        inspection_item = "data_directory"
        inspection_warning = {"ftype": 111}
        inspector.get_warning_params(inspection_item, inspection_warning)
    except ValueError as e:
        assert str(e) == 'The ftype warning type of data_directory inspection must be bool.'
    inspection_warning = {"ftype": True}
    inspector.get_warning_params(inspection_item, inspection_warning)


def test_check_daily_report_exist():
    instance = '127.0.0.1:12345'
    inspection_results = {
        'header': ['start', 'report'],
        'rows': [
            [1691510400000, {}],
            [1691511400000, full_inspection_item_dict],
            [1691512400000, {}]
        ]
    }
    start_time = '1691510400000'
    end_time = '1691511400000'
    start_time = datetime.fromtimestamp(int(start_time) / 1000)
    end_time = datetime.fromtimestamp(int(end_time) / 1000)
    with patch(
        'dbmind.app.monitoring.regular_inspection.sqlalchemy_query_jsonify_for_multiple_instances'
    ) as inspection_result:
        inspection_result.return_value = inspection_results
        inspector = TestMultipleHoursInspection(instance)
        inspector.check_daily_report_exist(instance, start_time, end_time, 0)
        instance_dict = {
            'header': ['start', 'report'],
            'rows': [[
                1691511400000, {
                    'system_resource': {
                        'os_cpu_usage': {
                            'cpu_user': {},
                            'cpu_iowait': {}
                        },
                        'os_disk_usage': {},
                        'os_mem_usage': {},
                        'os_disk_ioutils': {},
                        'network_packet_loss': {}
                    },
                    'instance_status': {
                        'component_error': {}
                    },
                    'database_resource': {
                        'data_directory': {},
                        'log_directory': {},
                        'db_size': {}
                    },
                    'database_performance': {
                        'buffer_hit_rate': {},
                        'user_login_out': {
                            'login': {},
                            'logout': {}
                        },
                        'active_session_rate': {},
                        'thread_pool': {},
                        'db_latency': {
                            'p95': {},
                            'p80': {}
                        },
                        'db_transaction': {
                            'commit': {},
                            'rollback': {}
                        },
                        'db_tmp_file': {},
                        'db_exec_statement': {
                            'select': {},
                            'update': {},
                            'insert': {},
                            'delete': {}
                        },
                        'db_deadlock': {},
                        'db_tps': {
                            'tps': {},
                            'qps': {}
                        },
                        'xmin_stuck': {},
                        'xlog_accumulate': {},
                        'db_top_query': {},
                        'log_error_check': {},
                        'long_transaction': {}
                    },
                    'diagnosis_optimization': {
                        'dynamic_memory': {
                            'dynamic_used_memory': {},
                            'dynamic_used_shrctx': {}
                        },
                        'process_memory': {},
                        'other_memory': {},
                        'core_dump': {}
                    },
                    'conclusion': {}
                }
            ]]
        }
        assert inspector.check_daily_report_exist(instance, start_time, end_time, 2) == (False, instance_dict)


def test_union_instance_res():
    instance = '127.0.0.1:12345'
    result = {}
    first_report = {
        '127.0.0.1:12345': {
            'timestamps': [1691510400000],
            'data': [1],
            'warnings': {
                'ftype_warning': True
            }
        }
    }
    second_report = {
        '127.0.0.1:12345': {
            'timestamps': [1691511400000, 1691511400000],
            'data': [2, 2],
            'warnings': {
                'ftype_warning': True
            }
        }
    }
    third_report = {'127.0.0.1:12345': {}}
    inspector = TestMultipleHoursInspection(instance)
    inspector.union_instance_res(first_report, result)
    inspector.union_instance_res(second_report, result)
    assert inspector.union_instance_res(third_report, result) is None
    result = {}
    dis_inspector = TestDistributeMultipleHoursInspection(instance)
    dis_inspector.union_instance_res(first_report, result)
    dis_inspector.union_instance_res(second_report, result)
    dis_inspector.union_instance_res(third_report, result)
    result = {}
    dis_inspector.union_db_res('111', first_report, result)
    dis_inspector.union_db_res('111', second_report, result)
    dis_inspector.union_db_res('111', third_report, result)


def test_union_warning_info(monkeypatch):
    instance = '127.0.0.1:12345'
    timestamps = [1691510400000]
    values = [1]
    record_sequence = regular_inspection.RecordSequence(
        timestamps, values, True)
    record_dict = {'127.0.0.1:12345': record_sequence}
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(
        inspector, 'get_warning_dict',
        mock.Mock(return_value=(True, {
            'ftype_warning': True
        })))
    instance_dict = {
        '127.0.0.1:12345': {
            'statistic': {
                'max': 1,
                'min': 1,
                'avg': 1.0,
                'the_95th': 1.0
            },
            'warnings': {
                'ftype_warning': True
            },
            'timestamps': (1691510400000,),
            'data': (1,)
        }
    }
    assert inspector.union_warning_info(record_dict, {}) == (1, instance_dict)


def test_union_system_resource():
    instance = '127.0.0.1:12345'
    valid_inspection_reports = [{'system_resource': sys_res_insp_item_dict}]
    inspector = TestMultipleHoursInspection(instance)
    score_dict = {
        'full_score': 0,
        'health_score': 0,
        'count': {}
    }
    instance_dict = {
        'os_cpu_usage': {
            'cpu_user': {},
            'cpu_iowait': {}
        },
        'os_disk_usage': {},
        'os_mem_usage': {},
        'os_disk_ioutils': {},
        'network_packet_loss': {}
    }
    assert inspector.union_system_resource(valid_inspection_reports, {}, score_dict) == instance_dict


def test_union_instance_status():
    instance = '127.0.0.1:12345'
    valid_inspection_reports = [{'instance_status': ins_statu_insp_item_dict}]
    inspector = TestMultipleHoursInspection(instance)
    score_dict = {
        'full_score': 0,
        'health_score': 0,
        'count': {}
    }
    instance_dict = {'component_error': {}}
    assert inspector.union_instance_status(valid_inspection_reports, {}, score_dict) == instance_dict


def test_union_special_res():
    instance = '127.0.0.1:12345'
    inspector = TestMultipleHoursInspection(instance)
    dis_inspector = TestDistributeMultipleHoursInspection(instance)
    inspection_name = "component_error"
    report = {'111': {'status': {}}, '222': {'warnings': {'timestamps': [111]}},
              '333': {'warnings': {'timestamps': [111]}}}
    dis_report = {'dn': report}
    result = {'222': {'status': {'timestamps': [111], 'value': [222]}, 'warnings': {}},
              '333': {'status': {'timestamps': [111], 'value': [222]},
                      'warnings': {'timestamps': [111], 'value': [222]}}}
    dis_result = {}
    inspector.union_special_res(report, result, inspection_name)
    dis_inspector.union_special_res(dis_report, dis_result, inspection_name)
    inspection_name = 'log_error_check'
    report = {'111': {}, '222': {'error_count': 1}}
    result = {'222': {'error_count': 2}}
    inspector.union_special_res(report, result, inspection_name)
    inspection_name = 'core_dump'
    report = {'222': {'count': 1, 'timestamps': [111], 'data': [222]},
              '333': {'count': 1, 'timestamps': [111], 'data': [222]}}
    result = {'333': {'count': 1, 'timestamps': [111], 'data': [222]}}
    inspector.union_special_res(report, result, inspection_name)
    inspection_name = 'network_packet_loss'
    report = {'111': {'222': {}}, '222': {'333': {'timestamps': [111], 'data': [222]}}}
    result = {'222': {'444': {}}}
    inspector.union_special_res(report, result, inspection_name)
    dis_inspector.union_special_res(report, result, inspection_name)
    inspection_name = 'long_transaction'
    result = []
    report = {'111': [], '222': [1]}
    dis_inspector.union_special_res(report, result, inspection_name)
    dis_inspector.union_special_res(report, result, inspection_name)


def test_union_inspection_result_single(monkeypatch):
    instance = '127.0.0.1:12345'
    dis_inspector = TestDistributeMultipleHoursInspection(instance)
    monkeypatch.setattr(dis_inspector, 'union_warning_info',
                        mock.Mock(return_value=(0, {})))
    inspection_name = 'db_size'
    data_dict = {
        '111': {}
    }
    score_dict = {
        'full_score': 0,
        'health_score': 0,
        'count': {}
    }
    dis_inspector.union_inspection_result_single(inspection_name, data_dict, score_dict, defaultdict(list))
    inspection_name = 'os_mem_usage'
    assert dis_inspector.union_inspection_result_single(inspection_name, data_dict, score_dict, defaultdict(list)) == {
        'os_mem_usage': {}}


def test_union_database_resource():
    instance = '127.0.0.1:12345'
    valid_inspection_reports = [{'database_resource': dat_res_insp_item_dict}]
    inspector = TestMultipleHoursInspection(instance)
    score_dict = {
        'full_score': 0,
        'health_score': 0,
        'count': {}
    }
    instance_dict = {'data_directory': {}, 'log_directory': {}, 'db_size': {}}
    assert inspector.union_database_resource(valid_inspection_reports, {}, score_dict) == instance_dict


def test_union_database_performance(monkeypatch):
    instance = '127.0.0.1:12345'
    valid_inspection_reports = [{
        'database_performance': dat_per_insp_item_dict
    }]
    inspector = TestMultipleHoursInspection(instance)
    score_dict = {
        'full_score': 0,
        'health_score': 0,
        'count': {}
    }
    monkeypatch.setattr(inspector, 'get_top_querys',
                        mock.Mock(return_value=(0, {})))
    monkeypatch.setattr(inspector, 'get_log_error',
                        mock.Mock(return_value=(0, {})))
    monkeypatch.setattr(inspector, 'long_transaction',
                        mock.Mock(return_value=(0, {})))
    instance_dict = {
        'buffer_hit_rate': {},
        'user_login_out': {
            'login': {},
            'logout': {}
        },
        'active_session_rate': {},
        'thread_pool': {},
        'db_latency': {
            'p95': {},
            'p80': {}
        },
        'db_transaction': {
            'commit': {},
            'rollback': {}
        },
        'db_tmp_file': {},
        'db_exec_statement': {
            'select': {},
            'update': {},
            'insert': {},
            'delete': {}
        },
        'db_deadlock': {},
        'db_tps': {
            'tps': {},
            'qps': {}
        },
        'xmin_stuck': {},
        'xlog_accumulate': {},
        'db_top_query': {},
        'log_error_check': {},
        'long_transaction': []
    }
    assert inspector.union_database_performance(valid_inspection_reports, {}, score_dict) == instance_dict


def test_union_diagnosis_optimization(monkeypatch):
    instance = '127.0.0.1:12345'
    valid_inspection_reports = [{
        'diagnosis_optimization':
            dia_opt_insp_item_dict
    }]
    inspector = TestMultipleHoursInspection(instance)
    score_dict = {
        'full_score': 0,
        'health_score': 0,
        'count': {}
    }
    monkeypatch.setattr(inspector, 'get_core_dump_detail',
                        mock.Mock(return_value=(0, {})))
    monkeypatch.setattr(inspector, 'get_guc_params',
                        mock.Mock(return_value=(0, {})))
    instance_dict = {
        'dynamic_memory': {
            'dynamic_used_memory': {},
            'dynamic_used_shrctx': {}
        },
        'process_memory': {},
        'other_memory': {},
        'core_dump': {},
        'guc_params': {}
    }
    assert inspector.union_diagnosis_optimization(
        valid_inspection_reports, {}, score_dict) == instance_dict


def test_get_report_result_union(monkeypatch):
    instance = '127.0.0.1:12345'
    valid_inspection_results = {'header': ['report'], 'rows': [[1, 2], [2, 3]]}
    inspector = TestMultipleHoursInspection(instance)
    monkeypatch.setattr(inspector, 'union_system_resource',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'union_instance_status',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'union_database_resource',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'union_database_performance',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(inspector, 'union_diagnosis_optimization',
                        mock.Mock(return_value=({})))
    instance_dict = {
        'system_resource': {},
        'instance_status': {},
        'database_resource': {},
        'database_performance': {},
        'diagnosis_optimization': {},
        'conclusion': {
            'health_score': 0,
            'full_score': 0,
            'health_status': 'perfect',
            'top3': []
        }
    }
    inspection_items = dbmind_core.InspectionItems(system_resource=[],
                                                   instance_status=[],
                                                   database_resource=[],
                                                   database_performance=[],
                                                   diagnosis_optimization=[])
    assert inspector.get_report_result_union(
        valid_inspection_results, inspection_items) == instance_dict


def test_check_time_valid():
    start_time = '1691510400000'
    end_time = '1691510000000'
    start_time = datetime.fromtimestamp(int(start_time) / 1000)
    end_time = datetime.fromtimestamp(int(end_time) / 1000)
    for inspection_type in ('daily_check', 'real_time_check', 'weekly_check',
                            'monthly_check'):
        regular_inspection.check_time_valid(inspection_type, start_time,
                                            end_time)
    end_time = start_time + timedelta(seconds=30 * 24 * 60 * 60)
    regular_inspection.check_time_month_valid(start_time, end_time)
    start_time = start_time.replace(day=1,
                                    hour=0,
                                    minute=0,
                                    second=0,
                                    microsecond=0)
    end_time = (start_time + timedelta(days=31)).replace(day=1,
                                                         hour=0,
                                                         minute=0,
                                                         second=0,
                                                         microsecond=0)
    assert regular_inspection.check_time_month_valid(start_time,
                                                     end_time) == (True, 31)


def test_real_time_inspection(monkeypatch):
    inspection_type = 'real_time_check'
    start_time = '1'
    end_time = '1'
    instance = '127.0.0.1:12345'
    tz = None
    inspection_items = dbmind_core.InspectionItems(system_resource=[],
                                                   instance_status=[],
                                                   database_resource=[],
                                                   database_performance=[],
                                                   diagnosis_optimization=[])
    try:
        regular_inspection.real_time_inspection("user", "text",
                                                inspection_type, start_time,
                                                end_time, instance,
                                                inspection_items, tz)
    except ValueError as e:
        assert str(e) == "Incorrect value for parameter start_time: 1."
    start_time = '1691510400000'
    try:
        regular_inspection.real_time_inspection("user", "text",
                                                inspection_type, start_time,
                                                end_time, instance,
                                                inspection_items, tz)
    except ValueError as e:
        assert str(e) == "Incorrect value for parameter end_time: 1."
    end_time = '1691510000000'
    try:
        regular_inspection.real_time_inspection("user", "text",
                                                inspection_type, start_time,
                                                end_time, instance,
                                                inspection_items, tz)
    except ValueError as e:
        assert str(
            e
        ) == "The time interval between start_time and end_time is not suit for inspection_type: real_time_check."
    inspector = TestCentralizeMultipleHoursInspection(instance)
    monkeypatch.setattr(regular_inspection, 'CentralizeMultipleHoursInspection',
                        mock.Mock(return_value=inspector))
    monkeypatch.setattr(dai, 'save_regular_inspection_results',
                        mock.Mock(return_value=1))
    monkeypatch.setattr(regular_inspection, 'get_instance_cluster_type',
                        mock.Mock(return_value='centralize'))
    for inspection_type in ('daily_check', 'real_time_check'):
        regular_inspection.real_time_inspection("user", "text",
                                                inspection_type, None,
                                                None, instance,
                                                inspection_items, tz)
    inspection_type = 'weekly_check'
    monkeypatch.setattr(inspector, 'check_daily_report_exist',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(inspector, 'get_report_result_union',
                        mock.Mock(return_value={}))
    try:
        regular_inspection.real_time_inspection("user", "text",
                                                inspection_type, None,
                                                None, instance,
                                                inspection_items, tz)
    except Exception as e:
        assert str(
            e
        ) == "inspection failed because do not have enough daily_check."
    inspection_type = 'monthly_check'
    monkeypatch.setattr(inspector, 'check_daily_report_exist',
                        mock.Mock(return_value=(False, {})))
    monkeypatch.setattr(inspector, 'get_report_result_union',
                        mock.Mock(return_value={}))
    try:
        regular_inspection.real_time_inspection("user", "text",
                                                inspection_type, None,
                                                None, instance,
                                                inspection_items, tz)
    except Exception as e:
        assert str(
            e
        ) == "inspection failed because do not have enough daily_check."


def test_exec_real_time_inspections_api(monkeypatch):
    scope = {'type': 'http', 'method': 'POST'}
    request = Request(scope)
    monkeypatch.setattr(data_transformer, 'exec_real_time_inspections',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(data_transformer, 'delete_real_time_inspections',
                        mock.Mock(return_value=({})))
    dbmind_core.exec_real_time_inspections(request)
    scope['method'] = 'DELETE'
    request = Request(scope)
    result = b'{"data":{},"success":true}'
    assert dbmind_core.exec_real_time_inspections(request).body == result


def test_list_real_time_inspections_api(monkeypatch):
    monkeypatch.setattr(data_transformer, 'list_real_time_inspections',
                        mock.Mock(return_value=({})))
    result = b'{"data":{},"success":true}'
    assert dbmind_core.list_real_time_inspections().body == result


def test_report_real_time_inspections_api(monkeypatch):
    monkeypatch.setattr(data_transformer, 'report_real_time_inspections',
                        mock.Mock(return_value=({})))
    result = b'{"data":{},"success":true}'
    assert dbmind_core.report_real_time_inspections().body == result


def test_exec_real_time_inspections_impl(monkeypatch):
    monkeypatch.setattr(regular_inspection, 'real_time_inspection',
                        mock.Mock(return_value=({})))
    inspection_items = dbmind_core.InspectionItems(system_resource=[],
                                                   instance_status=[],
                                                   database_resource=[],
                                                   database_performance=[],
                                                   diagnosis_optimization=[])

    inspection_type = '111'
    instance = '127.0.0.1:12345'
    try:

        data_transformer.exec_real_time_inspections("user", "text",
                                                    inspection_type, None,
                                                    None, None,
                                                    inspection_items, None)
    except ValueError:
        pass

    try:
        data_transformer.exec_real_time_inspections("user", "text",
                                                    inspection_type, None,
                                                    None, instance,
                                                    inspection_items, None)
    except ValueError:
        inspection_type = 'daily_check'
    assert data_transformer.exec_real_time_inspections("user", "text",
                                                       inspection_type, None,
                                                       None, instance,
                                                       inspection_items, None) == {}


def test_list_real_time_inspections_impl(monkeypatch):
    monkeypatch.setattr(jsonify_utils, 'sqlalchemy_query_jsonify',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(dao.regular_inspections,
                        'select_metric_regular_inspections',
                        mock.Mock(return_value=([])))
    result = {
        'header': [
            'instance', 'start', 'end', 'id', 'state', 'cost_time',
            'inspection_type'
        ],
        'rows': []
    }
    instance = ''
    try:
        data_transformer.list_real_time_inspections(instance)
    except ValueError:
        instance = '127.0.0.1:12345'
    assert data_transformer.list_real_time_inspections(instance) == result


def test_report_real_time_inspections_impl(monkeypatch):
    monkeypatch.setattr(jsonify_utils, 'sqlalchemy_query_jsonify',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(dao.regular_inspections,
                        'select_metric_regular_inspections',
                        mock.Mock(return_value=([])))
    instance = ''
    spec_id = []
    try:
        data_transformer.report_real_time_inspections(instance, spec_id)
    except ValueError:
        instance = '127.0.0.1:12345'
    try:
        data_transformer.report_real_time_inspections(instance, spec_id)
    except ValueError:
        spec_id = '123'
    result = {
        'header': [
            'instance', 'report', 'start', 'end', 'id', 'state', 'cost_time',
            'inspection_type'
        ],
        'rows': []
    }
    assert data_transformer.report_real_time_inspections(instance,
                                                         spec_id) == result


def test_delete_real_time_inspections_impl(monkeypatch):
    monkeypatch.setattr(dao.regular_inspections,
                        'delete_metric_regular_inspections',
                        mock.Mock(return_value='success'))
    instance = ''
    spec_id = '1asd,123'
    try:
        data_transformer.delete_real_time_inspections(instance, spec_id)
    except ValueError:
        instance = '127.0.0.1:12345'
    try:
        data_transformer.delete_real_time_inspections(instance, spec_id)
    except ValueError:
        spec_id = '123'
    result = {'success': True}
    assert data_transformer.delete_real_time_inspections(instance,
                                                         spec_id) == result
