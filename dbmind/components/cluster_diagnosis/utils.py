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

from dbmind.common.types import ALARM_LEVEL, ALARM_TYPES

CM_ERROR = "abnormal_output_from_cm_ctl_query"

ROLES = {
    "cn": "cn_state",
    "dn": "dn_state",
    "etcd": "etcd_state",
    "gtm": "gtm_state",
    "cms": "cms_state"
}

METHOD = (
    "logical",
    "tree"
)

CN_STATUS = {
    CM_ERROR: -1,
    "Normal": 0,
    "Down": 1,
    "Deleted": 2,
    "ReadOnly": 3
}

DN_STATUS = {
    CM_ERROR: -1,
    "Normal": 0,
    "Unknown": 1,
    "Need repair": 2,
    "Wait promoting, Promoting or Demoting": 3,
    "Disk damaged": 4,
    "Port conflicting": 5,
    "Building": 6,
    "Build failed": 7,
    "CoreDump": 8,
    "ReadOnly": 9,
    "Manually stopped": 10,
    "Starting": 11,
}

CN_ANSWER = {
    CM_ERROR: -1,
    "Unknown": 0,
    "CN heartbeat timeout": 1,
    "CN phony dead": 2,
    "Core": 3,
    "CN ip lost": 4,
    "CN disk Damage": 5,
    "CN port conflict": 6,
    "CN nic down": 7,
    "CN manual stop": 8,
    "CN down/disconnection": 9,
    "CN disconnected from dn": 10,
    "CN read only": 11
}

DN_ANSWER = {
    CM_ERROR: -1,
    "Unknown": 0,
    "DN manual stop": 1,
    "DN disk Damage": 2,
    "DN nic down": 3,
    "DN port conflict": 4,
    "DN restarted by cms": 5,
    "DN phony dead": 6,
    "Core": 7,
    "DN read only": 8,
    "DN down/disconnection": 9,
    "DN Primary disconnected with Standby": 10,
    "DN ip lost": 11,
}

CN_INPUT_ORDER = (
    'cn_status',
    'cn_restart',
    'cn_start',
    'cms_heartbeat_restart',
    'cms_phonydead_restart',
    'bind_ip_failed',
    'panic',
    'ffic_updated',
    'ping',
    'cn_dn_disconnected',
    'cn_down_to_delete',
    'cn_restart_time_exceed',
    'cn_disk_damage',
    'cn_port_conflict',
    'cn_nic_down',
    'cn_manual_stop',
    'cn_read_only'
)

DN_INPUT_ORDER = (
    'dn_status',
    'dn_manual_stop',
    'dn_disk_damage',
    'dn_nic_down',
    'bind_ip_failed',
    'dn_port_conflict',
    'cms_phonydead_restart',
    'cms_restart_pending',
    'dn_read_only',
    'ping',
    'ffic_updated',
    'dn_ping_standby',
    'dn_writable'
)

ANSWER_ORDERS = {
    "cn": {
        -1: "Normal",
        0: "Unknown",
        1: "CN heartbeat timeout",
        2: "CN phony dead",
        3: "Core",
        4: "CN ip lost",
        5: "CN disk Damage",
        6: "CN port conflict",
        7: "CN NIC down",
        8: "CN manual stop",
        9: "CN down/disconnection",
        10: "CN disconnected from dn",
        11: "CN read only"
    },
    "dn": {
        -1: "Normal",
        0: "Unknown",
        1: "DN manual stop",
        2: "DN disk Damage",
        3: "DN NIC down",
        4: "DN port conflict",
        5: "DN restarted by cms",
        6: "DN phony dead",
        7: "Core",
        8: "DN read only",
        9: "DN down/disconnection",
        10: "DN Primary disconnected with Standby",
        11: "DN ip lost"
    }
}

STATUS_MAP = {
    "cn_status": {
        -1: CM_ERROR,
        0: "Normal",
        1: "Down",
        2: "Deleted",
        3: "ReadOnly",
    },
    "dn_status": {
        -1: CM_ERROR,
        0: "Normal",
        1: "Unknown",
        2: "Need repair",
        3: "Demoting, Promoting or Wait promoting",
        4: "Disk damaged",
        5: "Port conflicting",
        6: "Building",
        7: "Build failed",
        8: "CoreDump",
        9: "ReadOnly",
        10: "Manually stopped",
    }
}

ANSWER_MAP = {
    "cn_nic_down": "cn_NIC_down",
    "dn_nic_down": "dn_NIC_down",
}

TYPE_AND_LEVEL = {
    "cn": {
        "Normal": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.NOTSET.value
        },
        "Unknown": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.NOTICE.value
        },
        "CN heartbeat timeout": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN phony dead": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "Core": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN ip lost": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN disk Damage": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN port conflict": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN NIC down": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN manual stop": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN down/disconnection": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN disconnected from dn": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "CN read only": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
    },
    "dn": {
        "Normal": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.NOTSET.value
        },
        "Unknown": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.NOTICE.value
        },
        "DN manual stop": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN disk Damage": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN NIC down": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN port conflict": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN restarted by cms": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN phony dead": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "Core": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN read only": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN down/disconnection": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN Primary disconnected with Standby": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
        "DN ip lost": {
            "alarm_type": ALARM_TYPES.ALARM,
            "alarm_level": ALARM_LEVEL.WARNING.value
        },
    }
}
