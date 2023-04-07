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


class ThresholdDetector(AbstractDetector):
    def __init__(self, high=float("inf"), low=-float("inf"), percentage: float = None):
        self.high = high
        self.low = low
        if isinstance(percentage, (int, float)) and 0 <= percentage <= 1:
            self.percentage = percentage
        else:
            self.percentage = None

    def _fit(self, s: Sequence) -> None:
        """Nothing to impl"""

    def _predict(self, s: Sequence) -> Sequence:
        n = len(s.values)
        np_values = np.array(s.values)
        predicted_values = (np_values > self.high) | (np_values < self.low)
        if self.percentage is None:
            return Sequence(timestamps=s.timestamps, values=predicted_values)
        elif np.count_nonzero(predicted_values) >= self.percentage * n:
            return Sequence(timestamps=s.timestamps, values=(True,) * n)
        else:
            return Sequence(timestamps=s.timestamps, values=(False,) * n)
