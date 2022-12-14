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

from dbmind.common.algorithm.anomaly_detection import (
    EsdTestDetector,
    GradientDetector,
    IncreaseDetector,
    InterQuartileRangeDetector,
    LevelShiftDetector,
    QuantileDetector,
    SeasonalDetector,
    SpikeDetector,
    ThresholdDetector,
    VolatilityShiftDetector
)
from dbmind.common.algorithm.anomaly_detection import pick_out_anomalies
from dbmind.common.algorithm.anomaly_detection.agg import merge_with_or_operator
from dbmind.common.algorithm.seasonal import is_seasonal_series
from dbmind.common.algorithm.stat_utils import sequence_interpolate
import dbmind.app.monitoring


class AnomalyDetections(object):
    __alg_func_name_map__ = {
        "level_shift": "do_level_shift_detect",
        "seasonal": "do_seasonal_detect",
        "spike": "do_spike_detect",
        "volatility_shift": "do_volatility_shift_detect",
    }

    # pure statistics detectors below
    @staticmethod
    def do_esd_test_detect(sequence, alpha=0.05):
        esd_test_detector = EsdTestDetector(alpha=alpha)
        anomalies = esd_test_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_iqr_detect(sequence, outliers=(3, 3)):
        iqr_detector = InterQuartileRangeDetector(outliers=outliers)
        anomalies = iqr_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_level_shift_detect(sequence, outliers=(None, 6), side="both", window=5):
        level_shift_detector = LevelShiftDetector(outliers=outliers, side=side, window=window)
        anomalies = level_shift_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_quantile_detect(sequence, high=0.95, low=0.05):
        quantile_detector = QuantileDetector(high=high, low=low)
        anomalies = quantile_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_seasonal_detect(sequence, outliers=(None, 3), side="positive", window=10):
        seasonal_detector = SeasonalDetector(outliers=outliers, side=side, window=window)
        anomalies = seasonal_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_spike_detect(sequence, outliers=(None, 3), side='positive'):
        spike_detector = SpikeDetector(outliers=outliers, side=side)
        anomalies = spike_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_volatility_shift_detect(sequence, outliers=(None, 6), side="both", window=10):
        volatility_shift_detector = VolatilityShiftDetector(outliers=outliers, side=side, window=window)
        anomalies = volatility_shift_detector.fit_predict(sequence)
        return anomalies

    # preset detectors below
    @staticmethod
    def do_gradient_detect(sequence, side='positive', max_coef=1, timed_window=300000):  # 300000 ms
        gradient_detector = GradientDetector(side=side, max_coef=max_coef, timed_window=timed_window)
        anomalies = gradient_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_increase_detect(sequence, side="positive", alpha=0.05):
        increase_detector = IncreaseDetector(side=side, alpha=alpha)
        anomalies = increase_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def do_threshold_detect(sequence, high=float("inf"), low=-float("inf")):
        threshold_detector = ThresholdDetector(high=high, low=low)
        anomalies = threshold_detector.fit_predict(sequence)
        return anomalies

    @staticmethod
    def choose_alg_func_automatically(sequence, func_name_list=None,
                                      high_ac_threshold=0.5, min_seasonal_freq=3):
        # func_name_list is a subset of ["persist", "level_shift", "volatility_shift"].
        func_name_list = func_name_list if func_name_list else ["spike", "level_shift"]
        is_seasonal, _ = is_seasonal_series(
            sequence.values,
            high_ac_threshold=high_ac_threshold,
            min_seasonal_freq=min_seasonal_freq
        )
        if is_seasonal:
            func_name_list = ["seasonal"]

        alg_func_list = [getattr(
            AnomalyDetections,
            AnomalyDetections.__alg_func_name_map__.get(func_name)
        ) for func_name in func_name_list]
        return alg_func_list

    @staticmethod
    def do_alg_process(func_list, sequence):
        result = list()
        for func in func_list:
            result_item = func(sequence)
            result.append(result_item)
        return merge_with_or_operator(result)


def tune_detector_in_targeted_params(metric_name, func_list):
    """Different anomaly detection algorithms are more suitable
     for different metrics, thus modify the hyper-parameters of
     these anomaly detection algorithms according to the Apriori rules."""
    # Add rules.
    return func_list


def detect(metric_name, sequence):
    """Return anomalies in Sequence format."""
    high_ac_threshold = dbmind.app.monitoring.get_param('high_ac_threshold')
    min_seasonal_freq = dbmind.app.monitoring.get_param('min_seasonal_freq')
    sequence = sequence_interpolate(sequence, strip_details=False)
    anomalies = AnomalyDetections.do_alg_process(
        tune_detector_in_targeted_params(
            metric_name,
            AnomalyDetections.choose_alg_func_automatically(
                sequence,
                high_ac_threshold=high_ac_threshold,
                min_seasonal_freq=min_seasonal_freq
            )
        ),
        sequence
    )
    return pick_out_anomalies(sequence, anomalies)
