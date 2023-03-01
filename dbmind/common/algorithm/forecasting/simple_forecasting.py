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

from dbmind.common.types import Sequence
from dbmind.common.algorithm.data_statistic import least_square
from .forecasting_algorithm import ForecastingAlgorithm


class SimpleLinearFitting(ForecastingAlgorithm):
    def __init__(self, avoid_repetitive_fitting=False):
        self._a = None
        self._b = None
        self._r2 = None
        self._last_x = None
        self._step = None
        self._fitted = False
        self._avoid_repetitive_fitting = avoid_repetitive_fitting

    def refit(self):
        self._fitted = False

    def fit(self, sequence: Sequence):
        # `self._fitted` is a flag to control whether performing the fitting process because
        # this fitting algorithm can estimate the linear degree. And if the class has
        # estimated a sequence, it should not fit one more. So, we use this flag to
        # prevent fitting again.
        if self._avoid_repetitive_fitting and self._fitted:
            return

        if sequence.length < 2:
            raise ValueError('Unable to fit the sequence due to short length.')

        a, b, r2 = least_square(sequence.timestamps, sequence.values)

        self._a = a
        self._b = b
        self._r2 = r2
        self._last_x = sequence.timestamps[-1]
        self._step = sequence.step
        self._fitted = True

    def forecast(self, forecast_length):
        future = []
        for i in range(1, forecast_length + 1):
            t = self._last_x + i * self._step
            v = self._a * t + self._b
            future.append(v)
        return future

    @property
    def r2_score(self):
        return self._r2
