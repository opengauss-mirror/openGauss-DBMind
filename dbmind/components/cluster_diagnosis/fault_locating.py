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

from dbmind.components.cluster_diagnosis.utils import (
    CN_STATUS, CN_ANSWER, DN_STATUS, DN_ANSWER
)


def cn_diagnosis(features):
    ping = features.get("ping")
    cn_status = features.get("cn_status")
    bind_ip_failed = features.get("bind_ip_failed")
    panic = features.get("panic")
    ffic_updated = features.get("ffic_updated")
    cms_heartbeat_restart = features.get("cms_heartbeat_restart")
    cms_phonydead_restart = features.get("cms_phonydead_restart")
    cn_dn_disconnected = features.get("cn_dn_disconnected")
    cn_down_to_delete = features.get("cn_down_to_delete")
    cn_restart_time_exceed = features.get("cn_restart_time_exceed")
    cn_read_only = features.get("cn_read_only")
    cn_restart = features.get("cn_restart")
    cn_start = features.get("cn_start")
    cn_manual_stop = features.get("cn_manual_stop")
    cn_disk_damage = features.get("cn_disk_damage")
    cn_nic_down = features.get("cn_nic_down")
    cn_port_conflict = features.get("cn_port_conflict")

    def restart_checking():
        if cn_restart_time_exceed or cn_restart:
            if cms_heartbeat_restart:
                return CN_ANSWER["CN heartbeat timeout"]
            if cms_phonydead_restart:
                return CN_ANSWER["CN phony dead"]

        if cn_start:
            if bind_ip_failed:
                return CN_ANSWER["CN ip lost"]
            if panic:
                return CN_ANSWER["Core"]
            if ffic_updated:
                return CN_ANSWER["Core"]

        return CN_ANSWER["Unknown"]

    def cm_checking():
        if cn_nic_down:
            return CN_ANSWER["CN nic down"]
        if cn_port_conflict:
            return CN_ANSWER["CN port conflict"]
        if cn_disk_damage:
            return CN_ANSWER["CN disk Damage"]
        if cn_manual_stop:
            return CN_ANSWER["CN manual stop"]

        return CN_ANSWER["Unknown"]

    if cn_status == CN_STATUS["Normal"]:
        if cn_nic_down:
            return CN_ANSWER["CN nic down"]
        if ping:
            return CN_ANSWER["CN down/disconnection"]
        if cn_dn_disconnected:
            return CN_ANSWER["CN disconnected from dn"]
        if cn_port_conflict:
            return CN_ANSWER["CN port conflict"]
        if cn_disk_damage:
            return CN_ANSWER["CN disk Damage"]
        if cn_read_only:
            return CN_ANSWER["CN read only"]

        return restart_checking()

    elif cn_status == CN_STATUS["Down"]:
        return cm_checking()

    elif cn_status == CN_STATUS["Deleted"]:
        if cn_nic_down:
            return CN_ANSWER["CN nic down"]
        if ping:
            return CN_ANSWER["CN down/disconnection"]
        if cn_down_to_delete:
            return cm_checking()
        if cn_dn_disconnected:
            return CN_ANSWER["CN disconnected from dn"]

        return restart_checking()

    elif cn_status == CN_STATUS["ReadOnly"]:
        if cn_read_only:
            return CN_ANSWER["CN read only"]

    return CN_ANSWER["Unknown"]


def dn_diagnosis(features):
    ping = features.get("ping")
    dn_status = features.get("dn_status")
    bind_ip_failed = features.get("bind_ip_failed")
    dn_ping_standby = features.get("dn_ping_standby")
    ffic_updated = features.get("ffic_updated")
    cms_phonydead_restart = features.get("cms_phonydead_restart")
    cms_restart_pending = features.get("cms_restart_pending")
    dn_read_only = features.get("dn_read_only")
    dn_manual_stop = features.get("dn_manual_stop")
    dn_disk_damage = features.get("dn_disk_damage")
    dn_nic_down = features.get("dn_nic_down")
    dn_port_conflict = features.get("dn_port_conflict")
    dn_writable = features.get("dn_writable")

    if ffic_updated:
        return DN_ANSWER["Core"]

    if dn_status == DN_STATUS["Normal"]:
        if dn_nic_down:
            return DN_ANSWER["DN nic down"]
        if ping:
            return DN_ANSWER["DN down/disconnection"]
        if bind_ip_failed:
            return DN_ANSWER["DN ip lost"]
        if cms_restart_pending:
            return DN_ANSWER["DN restarted by cms"]
        if dn_ping_standby:
            return DN_ANSWER["DN Primary disconnected with Standby"]
        if dn_port_conflict:
            return DN_ANSWER["DN port conflict"]
        if dn_disk_damage or dn_writable:
            return DN_ANSWER["DN disk Damage"]
        if dn_read_only:
            return DN_ANSWER["DN read only"]
        if cms_phonydead_restart:
            return DN_ANSWER["DN phony dead"]
        if dn_manual_stop:
            return DN_ANSWER["DN manual stop"]

    elif dn_status == DN_STATUS["Unknown"]:
        if dn_nic_down:
            return DN_ANSWER["DN nic down"]
        if ping:
            return DN_ANSWER["DN down/disconnection"]
        if bind_ip_failed:
            return DN_ANSWER["DN ip lost"]
        if cms_phonydead_restart:
            return DN_ANSWER["DN phony dead"]

    elif dn_status == DN_STATUS["Need repair"]:
        if cms_restart_pending:
            return DN_ANSWER["DN restarted by cms"]
        if dn_ping_standby:
            return DN_ANSWER["DN Primary disconnected with Standby"]

    elif dn_status == DN_STATUS["Wait promoting, Promoting or Demoting"]:
        pass

    elif dn_status == DN_STATUS["Disk damaged"]:
        if dn_disk_damage or dn_writable:
            return DN_ANSWER["DN disk Damage"]

    elif dn_status == DN_STATUS["Port conflicting"]:
        if dn_port_conflict:
            return DN_ANSWER["DN port conflict"]

    elif dn_status == DN_STATUS["ReadOnly"]:
        if dn_read_only:
            return DN_ANSWER["DN read only"]

    elif dn_status == DN_STATUS["Manually stopped"]:
        if dn_manual_stop:
            return DN_ANSWER["DN manual stop"]

    return DN_ANSWER["Unknown"]
