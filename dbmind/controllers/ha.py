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
"""This is only a template file that helps
 users implement the web interfaces for DBMind.
 And some implementations are only demonstrations.
"""
from dbmind.common.http import request_mapping
from dbmind.common.http import standardized_api_output
from dbmind.common.http.dbmind_protocol import HighAvailabilityConfig
from dbmind.service.web import runtime_status

latest_version = 'v1'
api_prefix = '/%s/api' % latest_version


@request_mapping(api_prefix + '/check-status', methods=['POST'], api=True)
@standardized_api_output
def check_status(high_availability_config: HighAvailabilityConfig = None):
    """
    Check the status of DBMind Service
    status list: ["NORMAL", "FAIL", "ABNORMAL"]

    - param cmd: The specified cmd commands to start DBMind Service
    - return: The status of DBMind Service
         e.g. {"error_msg": "","state": "NORMAL", "result": {}}
    """
    if high_availability_config:
        cmd = high_availability_config.cmd
    else:
        cmd = ''
    return runtime_status.check_status_dbmind()


@request_mapping(api_prefix + '/repair', methods=['POST'], api=True)
@standardized_api_output
def repair_interface(high_availability_config: HighAvailabilityConfig = None):
    """
    Repair DBMind Service
    status list: ["NORMAL", "FAIL", "ABNORMAL"]

    - param cmd: The specified cmd commands to start DBMind Service
    - return: The repair result of DBMind Service
         e.g. {"error_msg": "","state": "NORMAL", "result": {}}
    """
    if high_availability_config:
        cmd = high_availability_config.cmd
    else:
        cmd = ''
    return runtime_status.repair_interface_dbmind()
