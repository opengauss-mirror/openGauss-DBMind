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
"""
unittest case for microservice
"""

from dbmind.common.opengauss_driver import Driver
from dbmind.common.rpc.microservice_client import MicroserviceClient
from dbmind.service.microservice import Microservice


def test_microservice(monkeypatch):
    monkeypatch.setattr(Driver, 'initialize', lambda dsn1, dsn2: None)

    microservice = Microservice(
        database='postgres',
        host='127.0.0.1',
        port='19995',
        username='test',
        password='testtest'
    )

    assert microservice.agent_get_all() == {'127.0.0.1:19995': ['127.0.0.1:19995']}
    assert microservice.current_agent_addr() == '127.0.0.1:19995'
    assert isinstance(microservice.current_rpc(), MicroserviceClient)
    assert microservice.current_cluster_instances() == ['127.0.0.1:19995']
    assert isinstance(microservice.get('127.0.0.1:19995'), MicroserviceClient)

    with microservice.context(
        instance_address='127.0.0.1:19995',
        username='test',
        pwd='testtest'
    ):
        assert microservice.agent_get_all() == {'127.0.0.1:19995': ['127.0.0.1:19995']}
