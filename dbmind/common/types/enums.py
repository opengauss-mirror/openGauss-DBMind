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
from enum import Enum
from enum import IntEnum


class ALARM_LEVEL(IntEnum):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    NOTICE = INFO
    DEBUG = 10
    NOTSET = 0

    def __str__(self):
        return self._name_


class ALARM_TYPES:
    SYSTEM = 'SYSTEM'
    PERFORMANCE = 'PERFORMANCE'
    ALARM = 'ALARM'
    SECURITY = 'SECURITY'
    RESOURCE = 'RESOURCE'
    DISK_USAGE = 'DISK_USAGE'
    MEMORY = 'MEMORY'
    DISK_IO = 'DISK_IO'
    NETWORK = 'NETWORK'
    FD_LEAK = 'FD_LEAK'
    SLOW_QUERY = 'SLOW_QUERY'
    ALARM_LOG = 'ALARM_LOG'


class AnomalyTypes:
    """
    ANOMALY TYPES ENUM
    """
    SPIKE = 'SPIKE'
    INCREASE = 'INCREASE'
    GRADIENT = 'GRADIENT'
    LEVEL_SHIFT = 'LEVEL_SHIFT'
    THRESHOLD = 'THRESHOLD'


class ServiceState(Enum):
    FAIL = 'FAIL'
    SUCCESS = 'SUCCESS'
    NORMAL = 'NORMAL'
    ABNORMAL = 'ABNORMAL'


class CheckErrorMsg(Enum):
    LOG_FILE_MISSING = 'log file is missing.'
    PID_FILE_MISSING = 'pid file is missing.'
    PID_FILE_WRONG = 'the pid in pid file is wrong.'
    METADATABASE_INVALID = 'metadatabase is not valid.'
    CMD_INCORRECT = 'cmd is not correct.'
    MEM_USAGE_INVALID = 'can not get mem usage.'
    CPU_USAGE_INVALID = 'can not get cpu usage.'
    PERMISSION_DENIED = 'Permission denied.'
    DATABASE_INVALID = 'can not connect to database.'
    PID_FILE_INVALID = 'can not verify pid file status.'


class RepairErrorMsg(Enum):
    LOG_FILE_MISSING = 'log file is missing and can not be repaired.'
    PID_FILE_MISSING = 'pid file is created.'
    PID_FILE_WRONG = 'the wrong pid in pid file is fixed.'
    METADATABASE_INVALID = 'metadatabase is not valid and can not be repaired.'
    CMD_INCORRECT = 'cmd is not correct.'
    MEM_USAGE_INVALID = 'can not get mem usage and can not be repaired.'
    CPU_USAGE_INVALID = 'can not get cpu usage and can not be repaired.'


class ProcessSuggest(Enum):
    RESTART_PROCESS = "RESTART_PROCESS"
    RESTART_DATABASE = "RESTART_DATABASE"
    ALARM = "ALARM"
