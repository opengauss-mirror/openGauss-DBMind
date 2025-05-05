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
"""
alarm about self_security scenarios
"""
from dbmind.service import dai


def add_scenarios_alarms(scenarios, from_date, to_date, host):
    """
    Triggers alarms based on security scenarios
    @param scenarios: list of scenarios
    @param from_date: int ts in milliseconds
    @param to_date: int ts in milliseconds
    @param host: str server host
    @return float with score
    """
    alarms = []
    for scenario in scenarios:
        new_alarm = scenario.evaluate(from_date, to_date, host)
        if alarms is not None:
            alarms.append(new_alarm)
    if alarms:
        dai.save_history_alarms([alarms])
