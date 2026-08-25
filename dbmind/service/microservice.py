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

"""We introduce this file to indicate
which instance we are connecting to.
In this mode, DBMind only connects to single node.
"""
from dbmind.common.rpc.microservice_client import MicroserviceClient
from dbmind.common.utils.checking import prepare_ip
from dbmind.service.agent_adapter import AgentAdapter


class RPCAddressError(ValueError):
    pass


class Microservice(AgentAdapter):
    """
    Under microservice mode, there is only single connection scenario, so many interfaces are for adaptation.
    """
    def __init__(self, database, host, port, username, password, cluster=None):
        """
        Control the connection between DBMind and instances under distribute mode.
        """
        self.connection_kwargs = {
            'dbname': database,
            'user': username,
            'password': password,
            'host': host,
            'port': port
        }
        self.rpc = MicroserviceClient(self.connection_kwargs)

        self.instance = f'{prepare_ip(host)}:{port}'
        # Only used when this DBMind node needs to do tasks that traverse all instances.
        if cluster is not None:
            self.cluster = cluster
        else:
            self.cluster = {self.instance: [self.instance]}

        self._agents = {self.instance: self.rpc}

    def agent_get_all(self):
        return self.cluster

    def call(self, funcname, *args, **kwargs):
        return self.rpc.call(funcname, *args, **kwargs)

    def current_agent_addr(self):
        return self.instance

    def current_rpc(self):
        return self.rpc

    def current_cluster_instances(self):
        return [self.instance]

    def switch_context(self, agent_addr, username=None, pwd=None):
        """
        Only for interface adaptation, no actual context switching scenario because DBMind is stateless.
        :param agent_addr: openGauss database instance address, e.g., 127.0.0.1:6789
        :param username: openGauss database instance username;
        :param pwd: openGauss database instance password;
        :return True for success, False meaning failure.
        """
        if not agent_addr:
            self.rpc = None
            self.cluster = None
            self.connection_kwargs = None
            return True

        if agent_addr != self.current_agent_addr():
            return False

        return True

    def context(self, instance_address, username=None, pwd=None):
        """
        For interface adaptation and short-time database switch.
        :param instance_address:
        :param username:
        :param pwd:
        :return:
        """
        outer = self
        old = outer.current_agent_addr()

        class Inner:
            def __init__(self, addr):
                self.addr = addr

            def __enter__(self):
                if not outer.switch_context(self.addr, username, pwd):
                    raise RPCAddressError('Cannot switch to this RPC address %s' % instance_address)

            def __exit__(self, exc_type, exc_val, exc_tb):
                outer.switch_context(old)

        return Inner(instance_address)

    def get(self, instance_address):
        return self._agents.get(instance_address)
