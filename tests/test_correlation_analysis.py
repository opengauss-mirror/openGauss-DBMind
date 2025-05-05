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

from dbmind.common.algorithm.correlation import CorrelationAnalysis


def test_correlation_analysis():
    period = 20
    mean1 = 5000
    std1 = 40
    mean2 = 50000
    std2 = 3000
    start = 47000
    gradient = 10
    constant = 35000
    multiple = 0.03
    data_size = 300
    data_start = 500
    x = np.arange(data_start, data_start + data_size, 1)
    random = np.random.random(data_size)
    noise1 = np.random.normal(mean1, std1, data_size)
    noise2 = np.random.normal(mean2, std2, data_size)
    linear = start + gradient * x
    condition = np.array([0 if i % period else 1 for i in range(data_start, data_start + data_size)])
    y1 = linear + multiple * random - noise1 * condition
    y2 = constant + multiple * random + noise2 * condition

    preprocess_method_list = ['diff', 'historical_avg', 'historical_med', 'none']

    for p in preprocess_method_list:
        my_correlation_analysis = CorrelationAnalysis(preprocess_method=p,
                                                      analyze_method='coflux')
        y1_preprocessed, y2_preprocessed = my_correlation_analysis.preprocess(y1, y2)
        correlation_analysis_result = my_correlation_analysis.analyze(y1_preprocessed, y2_preprocessed)
        assert 0.7 < abs(correlation_analysis_result[0]) < 1

    holt_winters_smoothing_parameter_list = [0.2, 0.4, 0.6]
    holt_winters_period_list = [2, 4, 6, 8, 10]

    for a in holt_winters_smoothing_parameter_list:
        for b in holt_winters_smoothing_parameter_list:
            for c in holt_winters_smoothing_parameter_list:
                for p in holt_winters_period_list:
                    my_correlation_analysis = CorrelationAnalysis(preprocess_method='holt_winters',
                                                                  analyze_method='pearson',
                                                                  normalization_switch=True,
                                                                  holt_winters_parameters=(a, b, c, p))
                    y1_preprocessed, y2_preprocessed = my_correlation_analysis.preprocess(y1, y2)
                    correlation_analysis_result = my_correlation_analysis.analyze(y1_preprocessed, y2_preprocessed)
                    assert 0.7 < abs(correlation_analysis_result[0]) < 1

    wavelet_window_list = [1, 2, 3]
    sliding_length_list = [0, 200, 400]

    for w in wavelet_window_list:
        for s in sliding_length_list:
            my_correlation_analysis = CorrelationAnalysis(preprocess_method='wavelet',
                                                          analyze_method='coflux',
                                                          wavelet_window=w,
                                                          sliding_length=s)
            y1_preprocessed, y2_preprocessed = my_correlation_analysis.preprocess(y1, y2)
            correlation_analysis_result = my_correlation_analysis.analyze(y1_preprocessed, y2_preprocessed)
            assert 0.7 < abs(correlation_analysis_result[0]) < 1

