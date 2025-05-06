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

"""Imply that the module is responsible for
transforming data into a specific format,
 which in this case is JSON."""

import logging
import os
import time

from dbmind import global_vars, constants
from dbmind.common import ha
from dbmind.metadatabase import dao
from dbmind.common.utils.checking import prepare_ip


def get_log_path_dbmind():
    log_directory = global_vars.configs.get('LOG', 'log_directory', fallback='logs')
    log_directory = os.path.realpath(log_directory)
    return os.path.join(log_directory, constants.LOGFILE_NAME)


def record_high_availability_info(interface_type, interface_info):
    ha.record_interface_info(interface_type, interface_info)
    host = global_vars.configs.get('WEB-SERVICE', 'host')
    port = global_vars.configs.get('WEB-SERVICE', 'port')
    now_time = int(time.time() * 1000)
    try:
        dao.high_availability_status.insert_high_availability_status('{}:{}'.format(prepare_ip(host), port),
                                                                     now_time, interface_type, interface_info)
    except Exception as e:
        logging.info('can not insert HA status into database, because: %s.', str(e))


def check_status_dbmind():
    log_path = get_log_path_dbmind()
    pid_file = os.path.join(global_vars.confpath, constants.PIDFILE_NAME)
    status_info = ha.check_status_impl(log_path, pid_file, 'DBMind', ())
    record_high_availability_info('check_status', status_info)
    return status_info


def repair_interface_dbmind():
    log_path = get_log_path_dbmind()
    pid_file = os.path.join(global_vars.confpath, constants.PIDFILE_NAME)
    repair_info = ha.repair_interface_impl(log_path, pid_file, 'DBMind', ())
    record_high_availability_info('repair', repair_info)
    return repair_info
