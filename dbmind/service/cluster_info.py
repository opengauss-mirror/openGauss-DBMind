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

import json
from collections import defaultdict

from dbmind import constants, global_vars
from dbmind.common.utils.checking import prepare_ip, split_ip_port
from dbmind.components.cluster_diagnosis.cluster_diagnosis import WINDOW_IN_MINUTES
from dbmind.components.cluster_diagnosis.utils import ANSWER_ORDERS, ROLES
from dbmind.service import dai


def get_nic_ip_set(agents_set):
    """
    Acquire all nic ip based on agents set. :param agents_set: An agents set used to acquire all agents when DB
    instance is not deployed on the same NIC as opengauss exporter.
    :return: An agents set.
    """
    nic_state = dai.get_latest_metric_sequence(
        'opengauss_nic_state',
        WINDOW_IN_MINUTES
    ).fetchall()

    correlated_set = set()
    for state in nic_state:
        nic_list = json.loads(state.labels.get('ip', "[]"))
        if not nic_list:
            continue

        nic_set = {split_ip_port(ip)[0] for ip in nic_list}
        if nic_set & agents_set:
            correlated_set |= nic_set

    return correlated_set | agents_set


def get_all_agents():
    """
    global_vars.agent_proxy.agent_get_all() can not handle the situation when opengauss-exporter is not deployed on
    the same machine as the DB instance.
    :return: An agents set.
    """
    all_agents = set()

    for primary, instance_list in global_vars.agent_proxy.agent_get_all().items():
        all_agents.add(split_ip_port(primary)[0])
        for instance in instance_list:
            all_agents.add(split_ip_port(instance)[0])

    return get_nic_ip_set(all_agents)


def get_current_agents():
    """
    Obtain a complete current proxy IP cluster.
    :return: An agents set.
    """
    current_primary_agent = split_ip_port(global_vars.agent_proxy.current_agent_addr())[0]
    all_agents = global_vars.agent_proxy.agent_get_all()
    current_agents_set = set()
    for primary in all_agents:
        if split_ip_port(primary)[0] == current_primary_agent:
            current_agents_set |= set([split_ip_port(ip)[0] for ip in all_agents[primary]])
            current_agents_set.add(split_ip_port(primary)[0])
            return get_nic_ip_set(current_agents_set)

    return set()


def get_specific_agents(agent):
    """
    Given a primary agent ip, return an agent ip set whose ip belongs to the same cluster.
    :param agent: The given primary agent.
    :return: An agents set.
    """
    all_agents = global_vars.agent_proxy.agent_get_all()
    for primary in all_agents:
        if split_ip_port(primary)[0] == agent:
            agent_list = [split_ip_port(ip)[0] for ip in all_agents[primary]]
            agent_list.append(split_ip_port(primary)[0])
            return get_nic_ip_set(set(agent_list))

    return set()


def get_cn_dn_ip_set(agents_set):
    """
    Given an agents set, return corresponding cn and dn sets.
    :param agents_set: An agent set used to retrieve cn and dn ip.
    :return: A defaultdict, which default values is set(), keys are cn and dn, values are corresponding ip sets.
    """
    cluster_state = dai.get_latest_metric_sequence(
        'opengauss_cluster_state',
        WINDOW_IN_MINUTES
    ).filter_like(
        instance="|".join([prepare_ip(addr) + constants.PORT_SUFFIX for addr in agents_set])
    ).fetchall()

    cluster_ip = defaultdict(set)
    for role in ANSWER_ORDERS:
        for state in cluster_state:
            node_state_list = json.loads(state.labels.get(ROLES.get(role), "[]"))
            for role_state in node_state_list:
                instance = role_state.get("ip", None)
                if instance is not None:
                    cluster_ip[role].add(instance)
    return cluster_ip


def get_all_cn_dn_ip_set():
    """
    Get all CN and DN ip.
    :return: A defaultdict, which default values is set(), keys are cn and dn, values are corresponding ip sets.
    """
    all_agents = get_all_agents()
    return get_cn_dn_ip_set(all_agents)


def get_current_cn_dn_ip_set():
    """
    Get current CN and DN ip.
    :return: A defaultdict, which default values is set(), keys are cn and dn, values are corresponding ip sets.
    """
    current_agents = get_current_agents()
    return get_cn_dn_ip_set(current_agents)
