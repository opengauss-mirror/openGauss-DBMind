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
    def __init__(self, high=float("inf"), low=-float("inf"), percentage: float = None,
                 closed: bool = None):
        self.high = high
        self.low = low
        self.closed = closed
        if isinstance(percentage, (int, float)) and 0 <= percentage <= 1:
            self.percentage = percentage
        else:
            self.percentage = None

    def _fit(self, s: Sequence) -> None:
        """Nothing to impl"""

    def _predict(self, s: Sequence) -> Sequence:
        length = len(s.values)
        if self.least_length is not None and length < self.least_length:
            return Sequence(timestamps=s.timestamps, values=[False] * length)

        np_values = np.array(s.values)
        if self.closed:
            predicted_values = (np_values >= self.high) | (np_values <= self.low)
        else:
            predicted_values = (np_values > self.high) | (np_values < self.low)

        if self.percentage is None:
            return Sequence(timestamps=s.timestamps, values=predicted_values)
        elif np.count_nonzero(predicted_values) >= self.percentage * length:
            return Sequence(timestamps=s.timestamps, values=(True,) * length)
        else:
            return Sequence(timestamps=s.timestamps, values=(False,) * length)
