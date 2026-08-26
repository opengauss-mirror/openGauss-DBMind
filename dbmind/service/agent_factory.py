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

import threading
from typing import Optional

from dbmind.common.utils.base import try_to_get_an_element
from dbmind.common.utils.checking import split_ip_port
from dbmind.service.agent_adapter import AgentAdapter
from dbmind.service.microservice import Microservice
from dbmind.service.multicluster import AgentProxy


class AgentFactory:
    agent = None
    # configs
    agent_mode = None
    auto_discover_mode = None
    tsdb = None
    master_url = None
    cluster_info = None
    # ssl info
    ssl_certfile = None
    ssl_keyfile = None
    ssl_keyfile_password = None
    ssl_ca_file = None
    # connection info
    agent_username = None
    agent_pwd = None

    @classmethod
    def set_agent_info(cls, agent_mode, ssl_certfile, ssl_keyfile,
                       ssl_keyfile_password, ssl_ca_file,
                       agent_username, agent_pwd, auto_discover_mode,
                       tsdb=None, master_url=None, cluster_info=None):
        cls.agent_mode = agent_mode
        cls.auto_discover_mode = auto_discover_mode
        cls.tsdb = tsdb
        cls.master_url = master_url if master_url is not None else []
        cls.cluster_info = cluster_info
        # ssl info
        cls.ssl_certfile = ssl_certfile
        cls.ssl_keyfile = ssl_keyfile
        cls.ssl_keyfile_password = ssl_keyfile_password
        cls.ssl_ca_file = ssl_ca_file
        # connection info
        cls.agent_username = agent_username
        cls.agent_pwd = agent_pwd

    @classmethod
    def get_agent(cls) -> Optional[AgentAdapter]:
        return cls.agent


class RegularAgent(AgentFactory):
    shared_lock = threading.Lock()

    @classmethod
    def get_agent(cls) -> Optional[AgentAdapter]:
        if cls.agent is not None:
            return cls.agent

        with cls.shared_lock:
            cls.init_agent()

        return cls.agent

    @classmethod
    def init_agent(cls):
        agent = AgentProxy()
        if cls.auto_discover_mode:
            agent.set_autodiscover_connection_info(
                username=try_to_get_an_element(cls.agent_username, 0),
                password=try_to_get_an_element(cls.agent_pwd, 0),
                ssl_certfile=try_to_get_an_element(cls.ssl_certfile, 0),
                ssl_keyfile=try_to_get_an_element(cls.ssl_keyfile, 0),
                ssl_key_password=try_to_get_an_element(cls.ssl_keyfile_password, 0),
                ca_file=try_to_get_an_element(cls.ssl_ca_file, 0)
            )
            agent.autodiscover(cls.tsdb)
        else:
            for i, url in enumerate(cls.master_url):
                agent.agent_add(
                    url=url,
                    username=try_to_get_an_element(cls.agent_username, i),
                    password=try_to_get_an_element(cls.agent_pwd, i),
                    ssl_certfile=try_to_get_an_element(cls.ssl_certfile, i),
                    ssl_keyfile=try_to_get_an_element(cls.ssl_keyfile, i),
                    ssl_key_password=try_to_get_an_element(cls.ssl_keyfile_password, i),
                    ca_file=try_to_get_an_element(cls.ssl_ca_file, i)
                )

        agent.agent_finalize()
        cls.agent = agent


class DistributedAgent(AgentFactory):
    @classmethod
    def get_agent(cls) -> Optional[AgentAdapter]:
        host, port = split_ip_port(cls.master_url)
        return Microservice(
            database='postgres',
            host=host,
            port=port,
            username=cls.agent_username,
            password=cls.agent_pwd,
            cluster=cls.cluster_info
        )
