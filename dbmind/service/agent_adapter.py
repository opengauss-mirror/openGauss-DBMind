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

"""The abstract parent class of agent.
"""


class AgentAdapter(object):
    """The common baseclass of RPC(openGauss exporter based)
    mode and microservice mode, which is actually an interface,
    and other subclasses are implemented based on this
    interface in order to keep consistent format of
    return value to the upper layer's calling.

    """
    def autodiscover(self):
        pass

    def agent_get_all(self):
        pass

    def current_rpc(self):
        pass

    def current_agent_addr(self):
        pass

    def current_cluster_instances(self):
        pass

    def call(self, funcname, *args, **kwargs):
        pass

    def context(self, instance_address, username=None, pwd=None):
        pass

    def switch_context(self, agent_addr, username=None, pwd=None):
        pass

    def get(self, instance_address):
        pass
