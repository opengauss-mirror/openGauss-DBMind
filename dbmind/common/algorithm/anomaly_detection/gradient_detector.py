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
from ._utils import over_max_coef
from .spike_detector import remove_spike
from ...types import Sequence


def linear_fitting(x, y):
    x = np.array(x)
    y = np.array(y, dtype='float')
    coef, intercept = np.polyfit(x, y, deg=1)
    return coef, intercept


class GradientDetector(AbstractDetector):
    def __init__(self, side="positive", max_coef=1):
        self.side = side
        self.max_coef = abs(max_coef)

    def do_gradient_detect(self, s: Sequence):
        coef, _ = linear_fitting(s.timestamps, s.values)
        if over_max_coef(coef, self.side, self.max_coef):
            return (True,) * len(s)
        else:
            return (False,) * len(s)

    def _fit(self, sequence: Sequence):
        """Nothing to impl"""

    def _predict(self, s: Sequence) -> Sequence:
        normal_sequence = remove_spike(s)  # remove spike points
        predicted = self.do_gradient_detect(normal_sequence)  # do detect for rapid change
        return Sequence(timestamps=normal_sequence.timestamps, values=predicted)
