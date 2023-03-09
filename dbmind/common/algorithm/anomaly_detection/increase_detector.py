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
import scipy.stats

from ._abstract_detector import AbstractDetector
from ...types import Sequence

CDF_THRESHOLD = 1e-5


class IncreaseDetector(AbstractDetector):
    """
    COX STUART TEST
    Perform a Cox-Stuart test for data sequence.
    In many measurement processes, it is desirable to detect the prescence of
        trend. That is, if the data are assumed to be independent observations,
        we are interested in knowing if there is in fact a time dependent trend
        Given a set of ordered observations X1, X2, ..., Xn, let: half_n = n // 2,
        Then pair the data as X1,X1+half_n, X2,X2+half_n, ..., Xn-half_n,Xn.
    The Cox-Stuart test is then simply a sign test using the binomial distribution:
    n_positive = sum(sign(Xi+half_n - Xn) > 0)
    n_negative = sum(sign(Xi+half_n - Xn) < 0)
    n_diff = n_positive + n_negative
    p_value is a sum of n independent, identically distributed Bernoulli variables
        with parameter p.
    p_value_increase = binom.cdf(n_negative, n_diff, p=0.5)
    p_value_decrease = binom.cdf(n_positive, n_diff, p=0.5)
    (It's obvious that the sum of p_value_increase and p_value_decrease is always 1)
    If p_value is higher than the significant level (default: 0.05), we may reject the
        hypothesis that the sequence has a trend.

    parameters:
    side (str, optional): "positive" to identify a increase trend in data. "negative"
        to identify a decrease trend in data. Defaults to "positive".
    alpha (float, optional): the significant level to accept the hypothesis that the
        data sequence has a trend. Defaults to 0.05.
    """
    def __init__(self, side="positive", alpha=None):
        self.side = side
        self.alpha = alpha
        self.cdfs = None

    def _fit(self, s: Sequence):
        self.length = len(s.values)
        self.half_n = int(self.length / 2)
        if self.alpha is None:
            self.cdfs = 2 * scipy.stats.binom.cdf(range(self.half_n + 1), self.length, 0.5)
            idx = np.where(np.diff(self.cdfs) > CDF_THRESHOLD)[0][0]
            self.alpha = self.cdfs[idx]

    def _predict(self, s: Sequence) -> Sequence:
        x, y = s.timestamps, s.values
        coef = np.polyfit(x, y, deg=1)[0]

        n_pos = n_neg = 0
        for i in range(self.half_n):
            diff = y[i + self.half_n] - y[i]
            if diff > 0:
                n_pos += 1
            elif diff < 0:
                n_neg += 1
        n_diff = n_pos + n_neg

        if self.side == "positive":
            if n_neg > n_pos:
                return Sequence(timestamps=s.timestamps, values=[False] * self.length)

            if self.cdfs is None:
                p_value = 2 * scipy.stats.binom.cdf(n_neg, n_diff, 0.5)
            else:
                p_value = self.cdfs[n_neg]

            if p_value < self.alpha and coef > 0:
                return Sequence(timestamps=s.timestamps, values=[True] * self.length)

        elif self.side == "negative":
            if n_pos > n_neg:
                return Sequence(timestamps=s.timestamps, values=[False] * self.length)

            if self.cdfs is None:
                p_value = 2 * scipy.stats.binom.cdf(n_pos, n_diff, 0.5)
            else:
                p_value = self.cdfs[n_pos]

            if p_value < self.alpha and coef < 0:
                return Sequence(timestamps=s.timestamps, values=[True] * self.length)

        return Sequence(timestamps=s.timestamps, values=[False] * self.length)
