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
import multiprocessing
import os
from unittest import mock

from dbmind.common import ha
from dbmind.common.http.dbmind_protocol import HighAvailabilityConfig
from dbmind.components.cmd_exporter import controller as cmd_controller, service as cmd_service
from dbmind.components.opengauss_exporter.core import controller as open_controller, service as open_service
from dbmind.components.reprocessing_exporter.core import controller as repr_controller, service as repr_service
from dbmind.controllers import ha as ha_core
from dbmind.service.web import runtime_status
from dbmind.common.platform import WIN32


def test_check_param_validity():
    cmd = ''
    component_type = 'opengauss_exporter'
    ha.check_param_validity(cmd, component_type)
    cmd = './gs_dbmind'
    ha.check_param_validity(cmd, component_type)
    cmd = './gs_dbmind  component opengauss_exporter opengauss_exporter'
    ha.check_param_validity(cmd, component_type)
    cmd = './gs_dbmind component opengauss_exporter --constant-labels instance=111 --url ****** ' \
          '--web.listen-address 0.0.0.0 --web.listen-port 9198 --log.level info --disable-https'
    assert ha.check_param_validity(cmd, component_type) is True


def test_get_pid_file_constant_labels():
    cmd = ''
    proj_path = '/'
    pid_filename = ''
    component_type = ''
    constant_labels_instance = ''
    ha.get_pid_file_constant_labels(cmd, proj_path, pid_filename,
                                    component_type, constant_labels_instance)
    constant_labels_instance = '123'
    ha.get_pid_file_constant_labels(cmd, proj_path, pid_filename,
                                    component_type, constant_labels_instance)
    cmd = './gs_dbmind component opengauss_exporter --constant-labels ' \
          'instance=3d742909422d4163b9f5947787e73978in14_4c8b3b0ff54940c6aa8f1efa89bb024ano14=3 --url ****** ' \
          '--web.listen-address 0.0.0.0 --web.listen-port 9198 --log.level info --disable-https'
    ha.get_pid_file_constant_labels(cmd, proj_path, pid_filename,
                                    component_type, constant_labels_instance)
    cmd = './gs_dbmind component opengauss_exporter --constant-labels instance=111 --url ****** ' \
          '--web.listen-address 0.0.0.0 --web.listen-port 9198 --log.level info --disable-https'
    ha.get_pid_file_constant_labels(cmd, proj_path, pid_filename,
                                    component_type, constant_labels_instance)
    cmd = './gs_dbmind component opengauss_exporter --constant-labels instance=123 --url ****** ' \
          '--web.listen-address 0.0.0.0 --web.listen-port 9198 --log.level info --disable-https'
    result = os.path.join(
        proj_path, '{}_{}.pid'.format(component_type,
                                      constant_labels_instance))
    assert ha.get_pid_file_constant_labels(cmd, proj_path, pid_filename,
                                           component_type,
                                           constant_labels_instance) == result


def test_get_log_path():
    cmd = ''
    log_path = 'dbmind_opengauss_exporter.log'
    ha.get_log_path(cmd, log_path)
    cmd = './gs_dbmind component  opengauss_exporter --constant-labels instance=123 --url ****** ' \
          '--web.listen-address 0.0.0.0 --web.listen-port 9198 --log.level info --disable-https --log.filepath'
    ha.get_log_path(cmd, log_path)
    cmd = './gs_dbmind component  opengauss_exporter --constant-labels instance=123 --url ****** ' \
          '--web.listen-address 0.0.0.0 --web.listen-port 9198 --log.filepath dbmind_opengauss_exporter.log ' \
          '--log.level info --disable-https'
    result = 'dbmind_opengauss_exporter.log'
    assert ha.get_log_path(cmd, log_path) == result


def test_get_self_mem_usage(monkeypatch):
    ha.get_self_mem_usage()
    # friendly to mock on Win32
    if WIN32:
        setattr(os, 'sysconf', lambda x: x)
    monkeypatch.setattr(os, 'sysconf',
                        mock.Mock(return_value=(0)))
    assert ha.get_self_mem_usage() == (False, 'can not get mem usage.', 0.0)


def test_get_self_cpu_usage(monkeypatch):
    ha.get_self_cpu_usage()
    monkeypatch.setattr(multiprocessing, 'cpu_count',
                        mock.Mock(return_value=(0)))
    assert ha.get_self_cpu_usage() == (True, 'can not get cpu usage.', 0.0)


def test_get_self_resource_usage(monkeypatch):
    monkeypatch.setattr(ha, 'get_self_cpu_usage',
                        mock.Mock(return_value=(False, 'can not get cpu usage.', 0.0)))
    assert ha.get_self_resource_usage() == (False, 'can not get cpu usage.', 0.0, 0.0)


def test_check_log_file():
    log_path = ''
    ha.check_log_file(log_path)
    log_path = './non_exist.txt'
    ha.check_log_file(log_path)
    log_path = os.path.realpath('./test.log')
    with open(log_path, 'w+') as fp:
        fp.write('%d\n' % (os.getpid() + 1))
    assert ha.check_log_file(log_path) == (True, {})


def test_check_pid_file():
    pid_file = ''
    ha.check_pid_file(pid_file)
    pid_file = os.path.realpath('./test_check.pid')
    ha.check_pid_file(pid_file)
    with open(pid_file, 'w+') as fp:
        fp.write('%d\n' % (os.getpid() + 1))
    os.chmod(pid_file, 0o444)
    ha.check_pid_file(pid_file)
    os.chmod(pid_file, 0o600)
    ha.check_pid_file(pid_file)
    assert ha.check_pid_file(pid_file) == (True, {})
    os.remove(pid_file)


def test_check_resource_usage(monkeypatch):
    monkeypatch.setattr(ha, 'get_self_resource_usage',
                        mock.Mock(return_value=(False, '', 86, 86)))
    ha.check_resource_usage('DBMind')
    monkeypatch.setattr(ha, 'get_self_resource_usage',
                        mock.Mock(return_value=(True, '', 86, 86)))
    ha.check_resource_usage('DBMind')
    monkeypatch.setattr(ha, 'get_self_resource_usage',
                        mock.Mock(return_value=(True, '', 0, 0)))
    assert ha.check_resource_usage('DBMind') == (True, {})


class TestSession:

    def __enter__(self):
        self.tmp_var = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tmp_var = 1

    def query(self, *args):
        self.tmp_var = args[0]
        return self

    def count(self):
        return 0


def test_check_metadatabase(monkeypatch):
    monkeypatch.setattr(ha, 'get_session',
                        mock.Mock(return_value=(ValueError('111'))))
    ha.check_metadatabase()
    session = TestSession()
    monkeypatch.setattr(ha, 'get_session', mock.Mock(return_value=(session)))
    assert ha.check_metadatabase() == (True, {})


def test_check_database():
    driver = None
    ha.check_database(driver)
    driver = {}
    error_info_dict = {'state': 'FAIL', 'error_msg': 'can not connect to database.',
                       'result': {"suggest": 'RESTART_PROCESS'}}
    assert ha.check_database(driver) == (False, error_info_dict)


def test_repair_pid_file():
    pid_file = ''
    ha.repair_pid_file(pid_file)
    pid_file = os.path.realpath('./test_repair.pid')
    ha.repair_pid_file(pid_file)
    with open(pid_file, 'w+') as fp:
        fp.write('%d\n' % (os.getpid() + 1))
    os.chmod(pid_file, 0o444)
    ha.repair_pid_file(pid_file)
    os.chmod(pid_file, 0o600)
    ha.repair_pid_file(pid_file)
    assert ha.repair_pid_file(pid_file) == (True, {})
    os.remove(pid_file)


def test_check_status_common(monkeypatch):
    log_path = './test_high_availability.py'
    pid_file = './test_check.pid'
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_common(log_path, pid_file, 'DBMind')
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_pid_file',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_common(log_path, pid_file, 'DBMind')
    monkeypatch.setattr(ha, 'check_pid_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_common(log_path, pid_file, 'DBMind')
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(True, {})))
    assert ha.check_status_common(log_path, pid_file, 'DBMind') == (True, {})


def test_repair_interface_common(monkeypatch):
    log_path = './test_high_availability.py'
    pid_file = './test_check.pid'
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_common(log_path, pid_file, 'DBMind')
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'repair_pid_file',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_common(log_path, pid_file, 'DBMind')
    monkeypatch.setattr(ha, 'repair_pid_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_common(log_path, pid_file, 'DBMind')
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(True, {})))
    assert ha.repair_interface_common(log_path, pid_file, 'DBMind') == (True, {})


def test_check_status_impl(monkeypatch):
    log_path = './test_high_availability.py'
    pid_file = './test_check.pid'
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_impl(log_path, pid_file, 'cmd_exporter', ())
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_pid_file',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_impl(log_path, pid_file, 'cmd_exporter', ())
    monkeypatch.setattr(ha, 'check_pid_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_impl(log_path, pid_file, 'DBMind', ())
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_metadatabase',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_impl(log_path, pid_file, 'DBMind', ())
    monkeypatch.setattr(ha, 'check_metadatabase',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_database',
                        mock.Mock(return_value=(False, {})))
    ha.check_status_impl(log_path, pid_file, 'opengauss_exporter', (0,))
    monkeypatch.setattr(ha, 'check_database',
                        mock.Mock(return_value=(True, {})))
    result = {'state': 'NORMAL', 'error_msg': '', 'result': {}}
    assert ha.check_status_impl(log_path, pid_file, 'opengauss_exporter', (0,)) == result


def test_repair_interface_impl(monkeypatch):
    log_path = './test_high_availability.py'
    pid_file = './test_check.pid'
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_impl(log_path, pid_file, 'cmd_exporter', ())
    monkeypatch.setattr(ha, 'check_log_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'repair_pid_file',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_impl(log_path, pid_file, 'cmd_exporter', ())
    monkeypatch.setattr(ha, 'repair_pid_file',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_impl(log_path, pid_file, 'cmd_exporter', ())
    monkeypatch.setattr(ha, 'check_resource_usage',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_metadatabase',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_impl(log_path, pid_file, 'DBMind', ())
    monkeypatch.setattr(ha, 'check_metadatabase',
                        mock.Mock(return_value=(True, {})))
    monkeypatch.setattr(ha, 'check_database',
                        mock.Mock(return_value=(False, {})))
    ha.repair_interface_impl(log_path, pid_file, 'opengauss_exporter', (0,))
    monkeypatch.setattr(ha, 'check_database',
                        mock.Mock(return_value=(True, {})))
    result = {'state': 'SUCCESS', 'error_msg': '', 'result': {}}
    assert ha.repair_interface_impl(log_path, pid_file, 'opengauss_exporter', (0,)) == result


def test_index():
    cmd_controller.index()
    open_controller.index()
    result = b'reprocessing exporter (DBMind) \nmetric URI: /metrics \ncheck status URI: ' \
             b'/check-status \nrepair URI: /repair \n'
    assert repr_controller.index().body == result


def test_check_status_api(monkeypatch):
    monkeypatch.setattr(cmd_service, 'check_status_cmd_exporter',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(open_service, 'check_status_opengauss_exporter',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(repr_service, 'check_status_reprocessing_exporter',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(runtime_status, 'check_status_dbmind',
                        mock.Mock(return_value=({})))
    high_availability_config = HighAvailabilityConfig(cmd='111')
    cmd_controller.check_status(high_availability_config)
    cmd_controller.check_status()
    open_controller.check_status(high_availability_config)
    open_controller.check_status()
    repr_controller.check_status(high_availability_config)
    repr_controller.check_status()
    ha_core.check_status(high_availability_config)
    result = b'{"data":{},"success":true}'
    assert ha_core.check_status().body == result


def test_repair_interface_api(monkeypatch):
    monkeypatch.setattr(cmd_service, 'repair_interface_cmd_exporter',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(open_service, 'repair_interface_opengauss_exporter',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(repr_service, 'repair_interface_reprocessing_exporter',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(runtime_status, 'repair_interface_dbmind',
                        mock.Mock(return_value=({})))
    high_availability_config = HighAvailabilityConfig(cmd='111')
    cmd_controller.repair_interface(high_availability_config)
    cmd_controller.repair_interface()
    open_controller.repair_interface(high_availability_config)
    open_controller.repair_interface()
    repr_controller.repair_interface(high_availability_config)
    repr_controller.repair_interface()
    ha_core.repair_interface(high_availability_config)
    result = b'{"data":{},"success":true}'
    assert ha_core.repair_interface().body == result


def test_check_status(monkeypatch):
    monkeypatch.setattr(ha, 'get_log_path', mock.Mock(return_value=('')))
    monkeypatch.setattr(ha, 'get_pid_file_constant_labels',
                        mock.Mock(return_value=('')))
    monkeypatch.setattr(ha, 'check_status_impl',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(ha, 'record_interface_info',
                        mock.Mock())
    monkeypatch.setattr(runtime_status, 'record_high_availability_info',
                        mock.Mock())
    cmd_service.check_status_cmd_exporter('')
    open_service.check_status_opengauss_exporter('')
    repr_service.check_status_reprocessing_exporter('')
    assert runtime_status.check_status_dbmind() == {}


def test_repair_interface(monkeypatch):
    monkeypatch.setattr(ha, 'get_log_path', mock.Mock(return_value=('')))
    monkeypatch.setattr(ha, 'get_pid_file_constant_labels',
                        mock.Mock(return_value=('')))
    monkeypatch.setattr(ha, 'repair_interface_impl',
                        mock.Mock(return_value=({})))
    monkeypatch.setattr(ha, 'record_interface_info',
                        mock.Mock())
    monkeypatch.setattr(runtime_status, 'record_high_availability_info',
                        mock.Mock())
    cmd_service.repair_interface_cmd_exporter('')
    open_service.repair_interface_opengauss_exporter('')
    repr_service.repair_interface_reprocessing_exporter('')
    assert runtime_status.repair_interface_dbmind() == {}
