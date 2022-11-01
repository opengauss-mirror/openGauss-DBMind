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

from dbmind.common.algorithm.forecasting import ForecastingFactory
from dbmind.common.types import Sequence

linear_seq = Sequence(tuple(range(1, 10)), tuple(range(1, 10)))


def roughly_compare(list1, list2, threshold=1):
    if len(list1) != len(list2):
        return False
    for v1, v2 in zip(list1, list2):
        if abs(v1 - v2) > threshold:
            return False
    return True


def test_linear_regression():
    linear = ForecastingFactory.get_instance(linear_seq)
    linear.fit(linear_seq)
    result = linear.forecast(10)
    assert len(result) == 10
    assert roughly_compare(result, range(10, 20))

    assert ForecastingFactory.get_instance(linear_seq) is linear
