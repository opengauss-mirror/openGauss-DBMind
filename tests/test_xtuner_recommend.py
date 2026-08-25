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
from dbmind.components.xtuner.tuner.recommend import instantiate_knob, ReportMsg


def test_instantiate_knob():
    assert instantiate_knob('test', 1, 'int', 0, 10).to_string(1) == '10'
    assert instantiate_knob('test', 1, 'float', 0, 10).to_string(1) == '10.0'
    assert instantiate_knob('test', True, 'bool', 0, 10).to_string(1) == 'on'
    assert instantiate_knob('test', 1, 'bool', 0, 10).to_string(1) == 'on'


def test_report_msg():
    msg = ReportMsg()
    assert msg.generate() == ''
    msg.print_info({'msg': 'info'})
    assert len(msg.generate()) > 1

