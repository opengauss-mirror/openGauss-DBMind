# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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

_access_context = threading.local()


class ACCESS_CONTEXT_NAME:
    TSDB_FROM_SERVERS_REGEX = 'tsdb_from_servers_regex'
    INSTANCE_IP_WITH_PORT_LIST = 'instance_ip_with_port_list'
    INSTANCE_IP_LIST = 'instance_ip_list'
    AGENT_INSTANCE_IP_WITH_PORT = 'agent_instance_ip_with_port'


def set_access_context(**kwargs):
    """Since the web front-end login user can specify
    a cope, we should also pay attention
    to this context when returning data to the user.
    Through this function, set the effective visible field."""
    for k, v in kwargs.items():
        setattr(_access_context, k, v)


def get_access_context(name):
    return getattr(_access_context, name, None)

