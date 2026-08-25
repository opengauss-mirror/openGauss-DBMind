# Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
unittest case for microservice client
"""

from psycopg2.extras import RealDictRow

from dbmind.common.opengauss_driver import Driver
from dbmind.common.rpc.microservice_client import MicroserviceClient


def mock_driver_query(*args, **kwargs):
    row1 = RealDictRow()
    row1['col1'] = 'val1'
    row2 = RealDictRow()
    row2['col2'] = 'val2'
    return [row1, row2]


def test_microservice_client(monkeypatch):
    monkeypatch.setattr(Driver, 'initialize', lambda dsn1, dns2: None)
    monkeypatch.setattr(Driver, 'query', mock_driver_query)

    test_connection_kwargs = {
        'dbname': 'postgres',
        'user': 'test',
        'password': 'testtest',
        'host': '127.0.0.1',
        'port': '19995'
    }
    microservice_rpc = MicroserviceClient(
        test_connection_kwargs
    )

    assert microservice_rpc.call('query_in_database', 'select 1', 'postgres') == [{'col1': 'val1'}, {'col2': 'val2'}]

    assert microservice_rpc.call_with_another_credential(
        'test1',
        'testtest1',
        'query_in_database',
        'select 1',
        'dbmind'
    ) == [{'col1': 'val1'}, {'col2': 'val2'}]
