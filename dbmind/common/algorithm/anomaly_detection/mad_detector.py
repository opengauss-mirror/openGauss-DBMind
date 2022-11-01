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


class MadDetector(AbstractDetector):
    def __init__(self, threshold=3, scale_factor=1.4826):
        """Median Absolute Deviation Detector.
        In statistics, the median absolute deviation (MAD) is a robust measure
            of the variability of a univariate sample of quantitative data.
        For a univariate data set X1, X2, ..., Xn, the MAD is defined as the median
            of the absolute deviations from the data's median:
            MAD = abs(x - x.median).median * scale_factor

        parameters:
        threshold (float, optional): threshold to decide a anomaly data. Defaults to 3.
        scale_factor (float, optional): Multiple relationship between standard deviation and absolute
            median difference under normal distribution.
        """
        self.threshold = threshold
        self.scale_factor = scale_factor

    def _fit(self, s: Sequence):
        """Nothing to impl"""

    def _predict(self, s: Sequence) -> Sequence:
        x = np.array(s.values)
        x_median = np.median(x)
        abs_diff_median = np.abs(x - x_median)
        mad = self.scale_factor * np.median(abs_diff_median)
        rel_median = abs_diff_median / mad
        return Sequence(timestamps=s.timestamps, values=rel_median > self.threshold)
