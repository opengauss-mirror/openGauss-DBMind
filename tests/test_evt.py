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

import os

import numpy as np

from dbmind.common.algorithm.anomaly_detection.evt.spot import BiSPOT, BiDSPOT

# colors for plot
DEEP_SAFFRON = '#FF9933'
AIR_FORCE_BLUE = '#5D8AA8'


def single_test(spot_class, kwargs, init_data, data):
    s = spot_class(**kwargs)
    s.fit(init_data)
    upper_thresholds, lower_thresholds, res = s.simulate(data)
    return upper_thresholds, lower_thresholds, res


def test_rain():
    data_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "evt")
    rain_path = os.path.join(data_path, "rain")
    res1_path = os.path.join(data_path, "test_result1")
    res2_path = os.path.join(data_path, "test_result2")
    with open(rain_path, 'r') as f:
        r = f.read().split(',')

    x = np.array(list(map(float, r)))

    n_init = 1000
    init_data = x[:n_init]  # initial batch
    data = x[n_init:]  # stream

    kwargs = dict(
        probability=1e-3,
        depth=400,
        update_interval=3000,
        init_quantile=(0.02, 0.98)
    )
    _, _, res = single_test(BiSPOT, kwargs, init_data, data)
    with open(res1_path, 'r') as f:
        r = f.read().split(',')

    expected = np.array(list(map(int, r))).tolist()

    assert expected == [i for i, ans in enumerate(res) if ans]

    kwargs = dict(
        probability=1e-3,
        depth=400,
        update_interval=3000,
        init_quantile=(0.02, 0.98)
    )
    _, _, res = single_test(BiDSPOT, kwargs, init_data, data)
    with open(res2_path, 'r') as f:
        r = f.read().split(',')

    expected = np.array(list(map(int, r))).tolist()

    assert expected == [i for i, ans in enumerate(res) if ans]
