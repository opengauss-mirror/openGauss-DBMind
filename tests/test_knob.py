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
unittest case for knob
"""
from os import remove

from dbmind.components.xtuner.tuner import knob
from dbmind.components.xtuner.tuner import recommend


def test_recommended_knobs():
    """
    unittest for RecommendedKnobs
    """
    rec = knob.RecommendedKnobs()
    msg = recommend.ReportMsg()
    rec.reporter = msg
    assert rec.output_formatted_knobs() is None
    knob1 = knob.Knob('test')
    knob1.type = 'int'
    knob1.max = 10
    knob1.current = 1
    knob1.min = 1
    assert rec.append_need_tune_knobs(None, knob1) is None
    assert rec.append_only_report_knobs(None, knob1) is None
    assert rec.names() == ['test']
    assert len(rec) == 1
    assert rec[1] is None
    assert bool(rec)
    assert rec.__nonzero__() is True
    with open('test_file', 'w+') as test_file:
        assert rec.dump(test_file, dump_report_knobs=True) is None
    remove('test_file')


def test_knob():
    """
    unittest for Knob
    """
    knob1 = knob.Knob('test')
    knob1.max = 1
    knob1.current = 1
    knob1.min = 0
    assert knob1.min == 0
    assert knob1.max == 1
    knob1.type = 'bool'
    assert knob1.to_string(1) == 'on'
    assert knob1.to_numeric(1.23456) == 1.0

    knob1.max = 10
    knob1.current = 1
    knob1.min = 1
    knob1.type = 'int'
    assert knob1.to_string(1) == '10'
    assert knob1.to_numeric(1) == 0.0

    knob1.type = 'float'
    assert knob1.to_string(1.23456) == '12.11'
    assert knob1.to_numeric(1.234) == 0.026
    assert knob1.fresh_scale() is None
    assert knob1.to_dict() == {'test': {'default': None,
                                        'max': 10,
                                        'min': 1,
                                        'recommend': '10.0',
                                        'restart': False,
                                        'type': 'float'}}
    knob1.user_set = '1'
    assert knob1.fresh_scale() is None
    knob1.user_set = 1
    assert knob1.fresh_scale() is None
