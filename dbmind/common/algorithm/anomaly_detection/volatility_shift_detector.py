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

from ._abstract_detector import AbstractDetector
from .agg import merge_with_and_operator
from .detector_params import THRESHOLD
from .iqr_detector import InterQuartileRangeDetector
from .threshold_detector import ThresholdDetector
from .. import stat_utils
from ...types import Sequence


class VolatilityShiftDetector(AbstractDetector):
    def __init__(self, outliers=(None, 6), side="both", window=10, agg='std'):
        self.outliers = outliers
        self.window = window
        self.side = side
        self.agg = agg

    def _fit(self, s: Sequence) -> None:
        self._iqr_detector = InterQuartileRangeDetector(outliers=self.outliers)
        self._sign_detector = ThresholdDetector(high=THRESHOLD.get(self.side)[0],
                                                low=THRESHOLD.get(self.side)[1])

    def _predict(self, s: Sequence) -> Sequence:
        abs_rel_diff_values = stat_utils.np_double_rolling(
            s.values,
            window1=self.window,
            window2=self.window,
            diff_mode="abs_rel_diff",
            agg=self.agg
        )
        diff_values = stat_utils.np_double_rolling(
            s.values,
            window1=self.window,
            window2=self.window,
            diff_mode="diff",
            agg=self.agg
        )
        iqr_result = self._iqr_detector.fit_predict(Sequence(s.timestamps, abs_rel_diff_values))
        sign_check_result = self._sign_detector.fit_predict(Sequence(s.timestamps, diff_values))
        return merge_with_and_operator([iqr_result, sign_check_result])
