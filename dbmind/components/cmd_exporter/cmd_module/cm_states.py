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

CM_ERROR = "abnormal_output_from_cm_ctl_query"

CLUSTER_ROLES = (
    "cms_state",
    "etcd_state",
    "cn_state",
    "central_cn_state",
    "gtm_state",
    "dn_state"
)

CMS_ROLES = (
    "UNKNOWN",
    "Primary",
    "Standby",
    "Init",
    "Down"
)

ETCD_ROLES = (
    "UNKNOWN",
    "StateFollower",
    "StateLeader",
    "Down"
)
# Considering the inclusion relationship, some longer words should be forward
CN_ROLES = (
    "Main Standby",
    "Cascade Standby",
    "Primary",
    "Standby",
    "Pending",
    "Normal",
    "Down",
    "Secondary",
    "Deleted",
    "ReadOnly",
    "Offline",
)

GTM_STATES = (
    "Connection ok",
    "Connection bad",
    "Connection started",
    "Connection made",
    "Connection awaiting response",
    "Connection authentication ok",
    "Connection prepare environment",
    "Connection prepare SSL",
    "Connection needed",
    "Unknown",
    "Manually stopped",
    "Disk damaged",
    "Port conflicting",
    "Nic down",
    "Starting",
)

SYNC_STATES = (
    "Async",
    "Sync",
    "Most available",
    "Potential",
    "Quorum",
)
# Considering the inclusion relationship, some longer words should be forward
DN_ROLES = (
    "Main Standby",
    "Cascade Standby",
    "Primary",
    "Standby",
    "Pending",
    "Normal",
    "Down",
    "Secondary",
    "Deleted",
    "ReadOnly",
    "Offline",
)

DN_STATES = (
    "Unknown",
    "Normal",
    "Need repair",
    "Starting",
    "Wait promoting",
    "Demoting",
    "Promoting",
    "Building",
    "Manually stopped",
    "Disk damaged",
    "Port conflicting",
    "Build failed",
    "Catchup",
    "CoreDump",
    "ReadOnly",
    "DISCONNECTED",
)


def process_status(state, status_list):
    """find the effective state in Error msg."""
    for status in status_list:
        if status in state:
            return status
    return CM_ERROR


def parse_cms_line(line):
    info = line.strip().split()
    return {
        "ip": info[2],
        "path": info[4],
        "state": process_status(" ".join(info[5:]), CMS_ROLES)
    }


def parse_etcd_line(line):
    info = line.strip().split()
    return {
        "ip": info[2],
        "instance_id": info[3],
        "path": info[4],
        "state": process_status(" ".join(info[5:]), ETCD_ROLES)
    }


def parse_cn_line(line):
    info = line.strip().split()
    return {
        "ip": info[2],
        "instance_id": info[3],
        "port": info[4],
        "path": info[5],
        "state": process_status(" ".join(info[6:]), CN_ROLES)
    }


def parse_central_cn_line(line):
    info = line.strip().split()
    return {
        "ip": info[2],
        "instance_id": info[3],
        "path": info[4],
        "state": process_status(" ".join(info[5:]), CN_ROLES)
    }


def parse_gtm_line(line):
    for gtm_role in DN_ROLES:
        if gtm_role in line:
            break
    else:
        gtm_role = CM_ERROR

    for gtm_state in GTM_STATES:
        if gtm_state in line:
            break
    else:
        gtm_state = CM_ERROR

    info = line.strip().split()
    sync_state = " ".join(info[5 + len(gtm_role.split()) + len(gtm_state.split()):])
    return {
        "ip": info[2],
        "instance_id": info[3],
        "path": info[4],
        "role": gtm_role,
        "state": gtm_state,
        "sync_state": process_status(sync_state, SYNC_STATES)
    }


def parse_dn_line(line):
    res = list()
    for subline in line.split("|"):
        if not subline.strip():
            continue

        for dn_role in DN_ROLES:
            if dn_role in subline:
                break
        else:
            dn_role = CM_ERROR

        info = subline.strip().split()
        res.append(
            {"ip": info[2],
             "instance_id": info[3],
             "port": info[4],
             "path": info[5],
             "role": dn_role,
             "state": process_status(" ".join(info[6 + len(dn_role.split()):]), DN_STATES)}
        )
    return res


PARSE_METHODS = {
    "cms_state": parse_cms_line,
    "etcd_state": parse_etcd_line,
    "cn_state": parse_cn_line,
    "central_cn_state": parse_central_cn_line,
    "gtm_state": parse_gtm_line,
    "dn_state": parse_dn_line,
}
