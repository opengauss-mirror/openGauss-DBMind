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

import datetime
import json
from collections import defaultdict
from datetime import timedelta
from functools import partial

from dbmind.common.utils.checking import prepare_ip, split_ip_port
from dbmind.components.cluster_diagnosis.utils import ROLES, CN_STATUS, DN_STATUS
from dbmind.constants import PORT_SUFFIX
from dbmind.service import dai

PING_STEP = 5  # second
LONG_FETCH_MINUTES = 7 * 24 * 60


def get_instance_ids(role_name, ip, states):
    res = set()
    for state in states:
        node_state_list = json.loads(state.labels.get(role_name, "[]"))
        ip_list = [node_state.get("ip") for node_state in node_state_list]
        # to determine if the state comes from the same cluster
        if ip not in ip_list:
            continue

        for line in node_state_list:
            if line.get("ip") == ip and line.get("instance_id"):
                res.add(line.get("instance_id"))

    return list(res)


def get_node_list(nic_states):
    """get the node list"""
    res = dict()
    for nic_state in nic_states:
        instance = nic_state.labels.get("instance")
        ip_list = json.loads(nic_state.labels.get("ip", "[]"))
        if isinstance(ip_list, list) and len(ip_list) > len(res.get(instance, [])):
            res[instance] = ip_list

    return list(res.values())


def get_ip_alias_list(ip, node_list):
    """get the ip alias"""
    res = list()
    for alias in node_list:
        if not alias or not isinstance(alias, (list, tuple)) or len(alias) < 2:
            continue
        if ip in alias and len(alias) > len(res):
            res = alias

    if res:
        return res
    return [ip]


def get_cms_primary(role_name, instance_alias, states):
    for state in states:
        node_state_list = json.loads(state.labels.get(role_name, "[]"))
        ip_list = [role_state.get("ip") for role_state in node_state_list]
        # to determine if the state comes from the same cluster
        if not any(ip in ip_list for ip in instance_alias):
            continue

        cms_state_list = json.loads(state.labels.get("cms_state", "[]"))
        for cms_state in cms_state_list:
            if cms_state.get("state") == "Primary":
                return cms_state.get("ip")

    raise ValueError(f"There is no 'Primary' CM Server.")


def get_dn_count(states, instance_alias):
    for state in states:
        node_state_list = json.loads(state.labels.get('dn_state', "[]"))
        ip_list = [role_state.get("ip") for role_state in node_state_list]
        # to determine if the state come from the same cluster
        if not any(ip in ip_list for ip in instance_alias):
            continue
        return len(set(ip_list))


def is_disconnected(instance_alias, status_list, history_ping_state):
    history_connections = set()
    for history_ip_status in history_ping_state:
        source = history_ip_status.labels.get("source")
        target = history_ip_status.labels.get("target")
        exporter_ip = split_ip_port(history_ip_status.labels.get("instance"))[0]
        if (
            source in instance_alias or  # connection with self is meaningless
            target not in instance_alias  # confirm the target is the right instance
        ):
            continue

        history_connections.add((source, target, exporter_ip))

    connection_dict = defaultdict(list)
    for cluster_ip_status in status_list:
        status = cluster_ip_status.values
        source = cluster_ip_status.labels.get("source")
        target = cluster_ip_status.labels.get("target")
        exporter_ip = split_ip_port(cluster_ip_status.labels.get("instance"))[0]
        name = (source, target, exporter_ip)
        if name not in history_connections:
            continue

        connection_dict[name] += list(status)

    disconnections = defaultdict(list)
    for (_, _, exporter_ip), status in connection_dict.items():
        if not all(status):  # means disconnection record within the sequence
            disconnections[exporter_ip].append(True)
        else:
            disconnections[exporter_ip].append(False)

    if not disconnections:
        if history_connections:
            return True  # no connection records means disconnection

        return False  # no history indicates that instances are starting

    return all(all(state) for state in disconnections.values())  # disconnection with all nodes is real disconnection


def get_instance_features(instance, role, start_datetime, end_datetime):
    get_metric = partial(
        dai.get_metric_sequence,
        start_time=start_datetime,
        end_time=end_datetime
    )

    def get_log_metric_status(metric_name, filters, filter_likes, window_in_minutes=None):
        if window_in_minutes is None:
            fetcher = get_metric(metric_name).filter(**filters).filter_like(**filter_likes)
        else:
            fetcher = dai.get_metric_sequence(
                metric_name,
                start_time=end_datetime - timedelta(minutes=window_in_minutes),
                end_time=end_datetime
            ).filter(**filters).filter_like(**filter_likes)

        return int(bool(fetcher.fetchall()))

    def get_log_metric_number(metric_name, filters, filter_likes):
        fetcher = get_metric(metric_name).filter(**filters).filter_like(**filter_likes)
        return len(fetcher.fetchall())

    def get_cluster_state(role_name, ip, node_status, states):
        offline = False
        status_list = set()
        for state in states:
            # to determine if the state comes from the same cluster
            node_state_list = json.loads(state.labels.get(role_name, "[]"))
            ip_list = [node_state.get("ip") for node_state in node_state_list]
            if ip not in ip_list:
                continue

            for line in node_state_list:
                if line.get("ip") != ip:
                    continue

                for status, value in node_status.items():
                    if line.get("state") in status:
                        status_list.add(value)
                        if not offline:
                            offline = line.get("role", "").strip().lower() == "offline"

                        break

        if not status_list:
            raise ValueError(f"'{ip}' is not a member of '{role_name}' at {end_datetime}.")

        return sorted(list(status_list)), offline

    cluster_states = get_metric("opengauss_cluster_state").fetchall()
    nic_states = get_metric("opengauss_nic_state").fetchall()
    node_list = get_node_list(nic_states)
    instance_alias = get_ip_alias_list(instance, node_list)
    instance_filter = "|".join([prepare_ip(addr) + PORT_SUFFIX for addr in instance_alias])
    cms_primary = get_cms_primary(ROLES.get(role), instance_alias, cluster_states)
    cms_primary_alias = get_ip_alias_list(cms_primary, node_list)
    cms_primary_filter = "|".join([prepare_ip(addr) + PORT_SUFFIX for addr in cms_primary_alias])
    num_of_dn = get_dn_count(cluster_states, instance_alias)
    is_stand_alone = num_of_dn == 1
    if is_stand_alone:
        ping = 0
    else:
        long_fetch_start = end_datetime - datetime.timedelta(minutes=LONG_FETCH_MINUTES)
        opengauss_ping_state = get_metric("opengauss_ping_state", step=PING_STEP * 1000).fetchall()
        history_ping_state = dai.get_metric_sequence(
            "opengauss_ping_state",
            long_fetch_start,
            end_datetime,
            step=60 * 1000  # 60 seconds
        ).fetchall()
        ping = int(is_disconnected(instance_alias, opengauss_ping_state, history_ping_state))

    instance_ids = get_instance_ids(ROLES.get(role), instance, cluster_states)
    instance_id_filter = {"instance_id": "|".join(instance_ids)} if instance_ids else {}

    if role == "cn":
        cn_status = get_cluster_state(ROLES.get(role), instance, CN_STATUS, cluster_states)
        bind_ip_failed = get_log_metric_status(
            "opengauss_log_bind_ip_failed",
            filters={},
            filter_likes={"instance": instance_filter}
        )
        panic = get_log_metric_status(
            "opengauss_log_panic",
            filters={},
            filter_likes={"instance": instance_filter}
        )
        ffic_updated = get_log_metric_status(
            "opengauss_log_ffic",
            filters={},
            filter_likes={"instance": instance_filter}
        )
        cms_heartbeat_restart = get_log_metric_status(
            "opengauss_log_cms_heartbeat_timeout_restart",
            filters={},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cms_phonydead_restart = get_log_metric_status(
            "opengauss_log_cms_phonydead_restart",
            filters={},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cn_dn_disconnected = get_log_metric_status(
            "opengauss_log_cms_cn_down",
            filters={"cn_dn_disconnected": "1"},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        er_delete_cn = get_log_metric_status(
            "opengauss_log_cms_cn_down",
            filters={"er_delete_cn": "1"},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cn_tp_net_deleted = get_log_metric_status(
            "opengauss_log_cms_cn_down",
            filters={"cn_tp_net_deleted": "1"},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cn_dn_disconnected_system_alarm = int(get_log_metric_number(
            "opengauss_log_cn_dn_disconnection",
            filters={"role": role},
            filter_likes={**{"instance": instance_filter}, **instance_id_filter}
        ) >= num_of_dn)
        cn_down_to_delete = get_log_metric_status(
            "opengauss_log_cms_cn_down",
            filters={"cn_down_to_delete": "1"},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cn_restart_time_exceed = get_log_metric_status(
            "opengauss_log_cn_restart_time_exceed",
            filters={},
            filter_likes={"instance": cms_primary_filter}
        )
        cn_read_only = get_log_metric_status(
            "opengauss_log_cms_read_only",
            filters={},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cn_restart = get_log_metric_status(
            "opengauss_log_node_restart",
            filters={},
            filter_likes={"role": "(?i)" + role, "instance": instance_filter}
        )
        cn_start = get_log_metric_status(
            "opengauss_log_node_start",
            filters={},
            filter_likes={"role": "(?i)" + role, "instance": instance_filter}
        )
        cn_manual_stop = get_log_metric_status(
            "opengauss_log_cn_status",
            filters={"cn_manual_stop": "1"},
            filter_likes={"instance": instance_filter}
        )
        cn_disk_damage_before_removed = get_log_metric_status(
            "opengauss_log_cn_status",
            filters={"cn_disk_damage": "1"},
            filter_likes={"instance": instance_filter}
        )
        cn_disk_damage_after_removed = get_log_metric_status(
            "opengauss_log_cn_disk_status_after_removed",
            filters={"cn_disk_damage_after_removed": "1"},
            filter_likes={"instance": instance_filter}
        )
        cn_nic_down = get_log_metric_status(
            "opengauss_log_cn_status",
            filters={"cn_nic_down": "1"},
            filter_likes={"instance": instance_filter}
        )
        cn_port_conflict = get_log_metric_status(
            "opengauss_log_cn_status",
            filters={"cn_port_conflict": "1"},
            filter_likes={"instance": instance_filter}
        )

        cn_disk_damage = cn_disk_damage_before_removed or cn_disk_damage_after_removed
        cn_dn_disconnected = cn_dn_disconnected or cn_dn_disconnected_system_alarm

        return {
            "ping": ping,
            "cn_status": cn_status,
            "bind_ip_failed": bind_ip_failed,
            "panic": panic,
            "ffic_updated": ffic_updated,
            "cms_heartbeat_restart": cms_heartbeat_restart,
            "cms_phonydead_restart": cms_phonydead_restart,
            "cn_dn_disconnected": cn_dn_disconnected or er_delete_cn or cn_tp_net_deleted,
            "cn_down_to_delete": (
                cn_down_to_delete and
                not (
                    not cn_nic_down and
                    not cn_port_conflict and
                    not cn_disk_damage and
                    not cn_manual_stop and
                    cms_phonydead_restart
                )
            ),
            "cn_restart_time_exceed": cn_restart_time_exceed,
            "cn_read_only": cn_read_only,
            "cn_restart": cn_restart or cms_heartbeat_restart or cms_phonydead_restart,
            "cn_start": cn_start,
            "cn_manual_stop": cn_manual_stop,
            "cn_disk_damage": cn_disk_damage,
            "cn_nic_down": cn_nic_down,
            "cn_port_conflict": cn_port_conflict,
        }

    elif role == "dn":
        dn_status = get_cluster_state(ROLES.get(role), instance, DN_STATUS, cluster_states)
        bind_ip_failed = get_log_metric_status(
            "opengauss_log_bind_ip_failed",
            filters={},
            filter_likes={"instance": instance_filter}
        )
        dn_ping_standby = get_log_metric_status(
            "opengauss_log_dn_ping_standby",
            filters={},
            filter_likes={"instance": instance_filter}
        )
        ffic_updated = get_log_metric_status(
            "opengauss_log_ffic",
            filters={},
            filter_likes={"instance": instance_filter}
        )
        cms_heartbeat_restart = get_log_metric_status(
            "opengauss_log_cms_heartbeat_timeout_restart",
            filters={},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cms_phonydead_restart = get_log_metric_status(
            "opengauss_log_cms_phonydead_restart",
            filters={},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        cms_restart_pending = get_log_metric_status(
            "opengauss_log_cms_restart_pending",
            filters={},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        dn_read_only = get_log_metric_status(
            "opengauss_log_cms_read_only",
            filters={},
            filter_likes={**{"instance": cms_primary_filter}, **instance_id_filter}
        )
        dn_manual_stop = get_log_metric_status(
            "opengauss_log_dn_status",
            filters={"dn_manual_stop": "1"},
            filter_likes={**{"instance": instance_filter}, **instance_id_filter}
        )
        dn_disk_damage = get_log_metric_status(
            "opengauss_log_dn_status",
            filters={"dn_disk_damage": "1"},
            filter_likes={**{"instance": instance_filter}, **instance_id_filter}
        )
        dn_nic_down = get_log_metric_status(
            "opengauss_log_dn_status",
            filters={"dn_nic_down": "1"},
            filter_likes={**{"instance": instance_filter}, **instance_id_filter}
        )
        dn_port_conflict = get_log_metric_status(
            "opengauss_log_dn_status",
            filters={"dn_port_conflict": "1"},
            filter_likes={**{"instance": instance_filter}, **instance_id_filter}
        )
        dn_writable = get_log_metric_status(
            "opengauss_log_dn_writable_failed",
            filters={},
            filter_likes={"instance": instance_filter}
        )

        return {
            "ping": ping,
            "dn_status": dn_status,
            "bind_ip_failed": bind_ip_failed,
            "dn_ping_standby": dn_ping_standby,
            "ffic_updated": ffic_updated,
            "cms_phonydead_restart": cms_heartbeat_restart or cms_phonydead_restart,
            "cms_restart_pending": cms_restart_pending,
            "dn_read_only": dn_read_only,
            "dn_manual_stop": dn_manual_stop,
            "dn_disk_damage": dn_disk_damage,
            "dn_nic_down": dn_nic_down,
            "dn_port_conflict": dn_port_conflict,
            "dn_writable": dn_writable
        }
