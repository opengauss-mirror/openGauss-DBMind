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
try:
    from scipy import stats
except ImportError:
    pass

from ._abstract_detector import AbstractDetector
from ...types import Sequence


class EsdTestDetector(AbstractDetector):
    """
    The Extreme Studentized Deviate (ESD) test is based on an assumption that
    the data (time series or not) follows a normal distribution. (In case of
    few samples, we use t-distribution instead.)

    While training we calculate the std and critical-value of the dataset and
    then pick out the very sample with the highest value/std and compares
    value/std to the critical-value. if value/std is higher than ppf, we remove
    it from the dataset and repeat the procedure until the value/std is lower
    than or equal to the ppf.

    While predicting, the detector adds each value in the testing
    series to the set of normal values from training series independently, and
    performs generalized ESD test to this set (all normal values from training
    series, plus one value from testing series) to evaluate if this value of
    interest is an outlier.

    Parameters
    alpha: float, optional, Significance level. Default: 0.05.

    """

    def __init__(self, alpha=0.05):
        self.alpha = alpha

    def _fit(self, s: Sequence) -> None:
        if len(s.values) == 0:
            raise RuntimeError("Valid values are not enough for training.")

        v = np.array(s.values).astype('float')
        v_copy = np.array(v)
        n = v_copy[~np.isnan(v_copy)].size
        r = np.zeros(n)
        c = np.zeros(n)
        i = 0
        while v_copy[~np.isnan(v_copy)].size > 0:
            i += 1
            idx = np.nanargmax(np.abs(v_copy - np.nanmean(v_copy)))
            r[idx] = (
                abs(v_copy[idx] - np.nanmean(v_copy)) / np.nanstd(v_copy, ddof=1)
                if np.nanstd(v_copy, ddof=1) > 0
                else 0
            )
            v_copy[idx] = np.nan
            p = 1 - self.alpha / (2 * (n - i + 1))
            c[idx] = (
                (n - i) * stats.t.ppf(p, n - i - 1) /
                np.sqrt((n - i - 1 + stats.t.ppf(p, n - i - 1) ** 2) * (n - i + 1))
            )
            if r[idx] <= c[idx]:
                break

        self._normal_sum = v[c >= r].sum()
        self._normal_squared_sum = (v[c >= r] ** 2).sum()
        self._normal_count = v[c >= r].size
        i = 1
        n = self._normal_count + 1
        p = 1 - self.alpha / (2 * (n - i + 1))
        self._lambda = (
            (n - i) * stats.t.ppf(p, n - i - 1) /
            np.sqrt((n - i - 1 + stats.t.ppf(p, n - i - 1) ** 2) * (n - i + 1))
        )

    def _predict(self, s: Sequence) -> Sequence:
        length = len(s.values)
        if self.least_length is not None and length < self.least_length:
            return Sequence(timestamps=s.timestamps, values=[False] * length)

        np_values = np.array(s.values)
        new_sum = np_values + self._normal_sum
        new_count = self._normal_count + 1
        new_mean = new_sum / new_count
        new_squared_sum = np_values ** 2 + self._normal_squared_sum
        new_std = np.sqrt(
            (new_squared_sum - 2 * new_mean * new_sum + new_count * new_mean ** 2) /
            (new_count - 1)
        )

        predicted = abs(np_values - new_mean) / new_std > self._lambda
        return Sequence(timestamps=s.timestamps, values=predicted)
