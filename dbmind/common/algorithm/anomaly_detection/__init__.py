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

from ._utils import pick_out_anomalies
from .detector_params import *
from .gradient_detector import GradientDetector
from .increase_detector import IncreaseDetector
from .iqr_detector import InterQuartileRangeDetector
from .level_shift_detector import LevelShiftDetector
from .seasonal_detector import SeasonalDetector
from .spike_detector import SpikeDetector
from .threshold_detector import ThresholdDetector
from .volatility_shift_detector import VolatilityShiftDetector
from .quantile_detector import QuantileDetector
from .esd_test_detector import EsdTestDetector
from .spike_detector import remove_spike
from .evt_detector import EvtDetector
from .forecasting_anomaly_detector import ForecastingAnomalyDetector


detectors = {
    "GradientDetector": GradientDetector,
    "IncreaseDetector": IncreaseDetector,
    "InterQuartileRangeDetector": InterQuartileRangeDetector,
    "LevelShiftDetector": LevelShiftDetector,
    "SeasonalDetector": SeasonalDetector,
    "SpikeDetector": SpikeDetector,
    "ThresholdDetector": ThresholdDetector,
    "VolatilityShiftDetector": VolatilityShiftDetector,
    "QuantileDetector": QuantileDetector,
    "EsdTestDetector": EsdTestDetector,
    "ForecastingAnomalyDetector": ForecastingAnomalyDetector
}
