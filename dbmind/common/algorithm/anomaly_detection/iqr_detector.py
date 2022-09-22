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

from ._abstract_detector import AbstractDetector
from ...types import Sequence


class InterQuartileRangeDetector(AbstractDetector):
    def __init__(self, outliers=(3, 3)):
        self.outliers = outliers

    def _fit(self, s: Sequence) -> None:
        q1 = np.nanquantile(s.values, 0.25)
        q3 = np.nanquantile(s.values, 0.75)
        iqr = q3 - q1

        if isinstance(self.outliers[0], (int, float)):
            self.lower_bound = (q1 - iqr * self.outliers[0])
        else:
            self.lower_bound = -float("inf")

        if isinstance(self.outliers[1], (int, float)):
            self.upper_bound = (q3 + iqr * self.outliers[1])
        else:
            self.upper_bound = float("inf")

    def _predict(self, s: Sequence) -> Sequence:
        values = np.array(s.values)
        predicted_values = (values > self.upper_bound) | (values < self.lower_bound)
        return Sequence(timestamps=s.timestamps, values=predicted_values)
