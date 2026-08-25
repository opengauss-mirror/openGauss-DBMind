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

from unittest import mock

import pytest

from dbmind import global_vars
from dbmind.common.rpc import RPCClient
from dbmind.common.tsdb import TsdbClientFactory, tsdb_client
from dbmind.common.types import Sequence
from dbmind.service import multicluster
from dbmind.service.multicluster import (
    get_remote_instance_addresses,
    replace_sequence_ip,
    _get_inet_server_addr,
    _get_remote_instance_addresses_centralized,
    _get_remote_instance_addresses_from_tsdb,
    _is_primary,
    AgentProxy,
    REMOTE_INSTANCE_ADDRESSES_CENTRALIZED_STMT,
    REMOTE_INSTANCE_ADDRESSES_DISTRIBUTED_STMT,
    INET_SERVER_ADDR_STMT,
    IS_PRIMARY_STMT
)
from dbmind.service.web.jsonify_utils import split_ip_and_port

remote_instance_addresses_distributed_result = [{
    "node_type": "C",
    "node_host": "127.0.0.1",
    "node_port": "1"
}, {
    "node_type": "C",
    "node_host": "127.0.0.2",
    "node_port": "2"
}]

remote_instance_addresses_centralized_result = [{
    "name": "replconninfo1",
    "key": "remotehost",
    "value": "127.0.0.3"
}, {
    "name": "replconninfo1",
    "key": "remoteport",
    "value": "19996"
}, {
    "name": "replconninfo2",
    "key": "remotehost",
    "value": "127.0.0.4"
}, {
    "name": "replconninfo2",
    "key": "remoteport",
    "value": "19996"
}]

inet_server_addr_result = [{"host": "127.0.0.1", "port": "1"}]

is_primary_result = [{"r": "f"}]


def mock_query_in_postgres(*args):
    if args[1] == 'query_in_postgres':
        if args[2] == REMOTE_INSTANCE_ADDRESSES_CENTRALIZED_STMT:
            return remote_instance_addresses_centralized_result
        if args[2] == REMOTE_INSTANCE_ADDRESSES_DISTRIBUTED_STMT:
            return remote_instance_addresses_distributed_result
        if args[2] == INET_SERVER_ADDR_STMT:
            return inet_server_addr_result
        if args[2] == IS_PRIMARY_STMT:
            return is_primary_result
    return None


def mock_get_current_metric_value(metric_name, *args):
    if metric_name == "opengauss_exporter_fixed_info":
        return [{
            "labels": {
                "dbname": "postgres",
                "instance": "127.0.0.1:9187",
                "job": "opengauss_exporter",
                "monitoring": "127.0.0.1:19995",
                "primary": "False",
                "rpc": "True",
                "updated": "1690613121.9595578",
                "url": "http://0.0.0.0:9187",
                "version": "3.1.0"
            }
        }]
    if metric_name == "opengauss_cluster_state":
        return [{"labels": {
            "cn_state": [{"ip": "127.0.0.2", "instance_id": "5001", "port": "6000", "state": "Down"},
                         {"ip": "127.0.0.3", "instance_id": "5002", "port": "6000", "state": "Down"}]
        }}]


@pytest.fixture(autouse=True)
def get_mock_rpc(monkeypatch):
    monkeypatch.setattr(RPCClient, 'call', mock_query_in_postgres)

    mock_client = mock.MagicMock()
    mock_client_instance = mock_client.return_value
    mock_client_instance.get_current_metric_value = mock_get_current_metric_value
    monkeypatch.setattr(TsdbClientFactory, 'get_tsdb_client', mock_client)


def test_query_in_postgres():
    rpc = RPCClient(url='http://127.0.0.1')
    assert get_remote_instance_addresses(rpc) == ['127.0.0.1:1', '127.0.0.2:2']
    assert _get_inet_server_addr(rpc) == ['127.0.0.1:1']
    assert _get_remote_instance_addresses_centralized(rpc) == ['127.0.0.1:1', '127.0.0.3:19995', '127.0.0.4:19995']
    assert _is_primary(rpc) == 'f'


def test_split_ip_and_port():
    ip, port = split_ip_and_port('127.0.0.1:9090')
    assert ip == "127.0.0.1"
    assert port == "9090"


sequence_test = Sequence(
    timestamps=[1731590762000],
    values=[1],
    name='opengauss_exporter_fixed_info',
    step=15000,
    labels={"instance": "192.168.1.1:3306", "primary": "192.168.1.2:3306", "standby": "192.168.1.2:3306"}
)


@mock.patch.object(global_vars, 'ip_map', {"192.168.1.1": {"192.168.1.2": "192.168.1.3"}})
def test_replace_sequence_ip():
    replace_sequence_ip(sequence_test)
    assert sequence_test.labels["instance"] == "192.168.1.1:3306"
    assert sequence_test.labels["primary"] == "192.168.1.3:3306"
    assert sequence_test.labels["standby"] == "192.168.1.3:3306"


def test_get_remote_instance_addresses_from_tsdb():
    TsdbClientFactory.get_tsdb_client = mock.MagicMock()
    tsdb = TsdbClientFactory.get_tsdb_client.return_value

    tsdb.get_current_metric_value = mock.MagicMock()

    sequence1 = mock.MagicMock()
    sequence1.labels = {'primary': 'instance1', 'standby': 'instance2,instance3'}
    tsdb.get_current_metric_value.return_value = [sequence1]
    assert _get_remote_instance_addresses_from_tsdb('instance1') == (True, ['instance2', 'instance3', 'instance1'])

    sequence2 = mock.MagicMock()
    sequence2.labels = {
        'primary': 'instance4', 'standby': 'instance5', 'cn_state': '[{"ip": "192.168.1.1", "port": "5432"}]'
    }
    tsdb.get_current_metric_value.return_value = [sequence2]
    assert _get_remote_instance_addresses_from_tsdb('192.168.1.1:5432') == \
           (True, ['192.168.1.1:5432', 'instance4', 'instance5'])

    sequence3 = mock.MagicMock()
    sequence3.labels = {
        'primary': 'instance6', 'standby': 'instance7', 'cn_state': '[{"ip": "192.168.1.2", "port": "5432"}]'
    }
    tsdb.get_current_metric_value.return_value = [sequence3]
    assert _get_remote_instance_addresses_from_tsdb('192.168.1.3:5432') == (False, None)


sequence_primary = Sequence(
    timestamps=[1731590762000],
    values=[1],
    name='opengauss_exporter_fixed_info',
    step=15000,
    labels={
        'dbname': 'postgres',
        'instance': '192.168.0.0:9188',
        'job': 'opengauss_exporter',
        'monitoring': '192.168.0.0:19995',
        'primary': 'True',
        'rpc': 'True',
        'updated': '1731590762.1234567',
        'url': 'http://0.0.0.0:9188',
        'version': '5.3.0'
    }
)

sequence_standby_1 = Sequence(
    timestamps=[1731590762000],
    values=[1],
    name='opengauss_exporter_fixed_info',
    step=15000,
    labels={
            'dbname': 'postgres',
            'instance': '192.168.0.1:9188',
            'job': 'opengauss_exporter',
            'monitoring': '192.168.0.1:19995',
            'primary': 'False',
            'rpc': 'True',
            'updated': '1731590762.1234567',
            'url': 'http://0.0.0.0:9188',
            'version': '5.3.0'
        }
)

sequence_standby_2 = Sequence(
    timestamps=[1731590762000],
    values=[1],
    name='opengauss_exporter_fixed_info',
    step=15000,
    labels={
        'dbname': 'postgres',
        'instance': '192.168.0.2:9188',
        'job': 'opengauss_exporter',
        'monitoring': '192.168.0.2:19995',
        'primary': 'False',
        'rpc': 'True',
        'updated': '1731590762.1234567',
        'url': 'http://0.0.0.0:9188',
        'version': '5.3.0'
    }
)


def mock_get_agent_instance_detail(agent):
    return agent.url.split(':')[-2].split('/')[-1], agent.url.split(':')[-1].split('/')[0]


def mock_is_primary(agent):
    if '192.168.0.0' in agent.url:
        return True
    else:
        return False


def mock_get_remote_instance_addresses(rpc):
    return ['192.168.0.0:9188', '192.168.0.1:9188', '192.168.0.2:9188']


def test_agent_proxy(monkeypatch):
    agent = AgentProxy()
    agent.set_autodiscover_connection_info(
        username='test',
        password='testtest'
    )
    tsdb = tsdb_client.TsdbClient()
    tsdb.get_current_metric_value = mock.MagicMock(return_value=[
        sequence_primary, sequence_standby_1, sequence_standby_2
    ])
    agent.autodiscover(tsdb)

    assert agent.agent_can_heavyweight_update()
    assert len(agent._unchecked_agents) == 3

    monkeypatch.setattr(multicluster, '_get_agent_instance_details', mock_get_agent_instance_detail)
    monkeypatch.setattr(multicluster, '_get_remote_instance_addresses_from_tsdb', lambda x: (False, None))
    monkeypatch.setattr(RPCClient, 'heartbeat', lambda x: True)
    monkeypatch.setattr(multicluster, '_is_primary', mock_is_primary)
    monkeypatch.setattr(multicluster, 'get_remote_instance_addresses', mock_get_remote_instance_addresses)

    agent.agent_finalize()
    assert agent.agent_get_all() == {'192.168.0.0:9188': ['192.168.0.0:9188', '192.168.0.1:9188', '192.168.0.2:9188']}
    agent.agent_lightweight_update()
    assert agent.agent_get_all() == {'192.168.0.0:9188': ['192.168.0.0:9188', '192.168.0.1:9188', '192.168.0.2:9188']}
