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

import numpy as np

from dbmind.common.algorithm import preprocessing


def test_min_max_scaler():
    scaler = preprocessing.MinMaxScaler(feature_range=(0, 1))
    x = [1, 2, 3, 4, np.nan, 5]
    y = [2, 3, 4, 5, 6]

    scaler.fit(x)
    scaled_y = scaler.transform(y)
    assert all(scaled_y == [0.25, 0.5, 0.75, 1., 1.25])
    inversed_y = scaler.inverse_transform(scaled_y)
    assert all(inversed_y == y)

    scaled_x = scaler.fit_transform(x)
    assert (
        np.isnan(scaled_x).any() and
        np.nanmax(scaled_x) == 1.0 and
        np.nanmin(scaled_x) == 0.0
    )
    scaled_y = scaler.fit_transform(y)
    assert all(scaled_y == [0.0, 0.25, 0.5, 0.75, 1.0])
