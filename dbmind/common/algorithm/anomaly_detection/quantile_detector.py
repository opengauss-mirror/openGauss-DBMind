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


class QuantileDetector(AbstractDetector):
    def __init__(self, high=1, low=0):
        self.high = high
        self.low = low

    def _fit(self, s: Sequence) -> None:
        if len(s.values) == 0:
            raise RuntimeError("Valid values are not enough for training.")

        self.upper_bound = np.nanquantile(s.values, self.high)
        self.lower_bound = np.nanquantile(s.values, self.low)

    def _predict(self, s: Sequence) -> Sequence:
        np_values = np.array(s.values)
        predicted_values = (np_values > self.upper_bound) | (np_values < self.lower_bound)
        return Sequence(timestamps=s.timestamps, values=predicted_values)
