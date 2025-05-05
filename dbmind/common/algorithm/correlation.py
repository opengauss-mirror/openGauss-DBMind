# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

from functools import partial

import numpy as np
try:
    from scipy.stats import pearsonr, zscore
except ImportError:
    pass


def pearson(x, y):
    return pearsonr(x, y)[0]


def iter_shift(arr, num, fill_value=0):
    if num == 0:
        return arr
    if num > 0:
        return np.concatenate((np.full(num, fill_value), arr[:-num]))
    else:
        return np.concatenate((arr[-num:], np.full(-num, fill_value)))


def cross_correlation(data1, data2, shift_num):
    return pearson(data1, iter_shift(data2, shift_num))


class CorrelationAnalysis:
    """A class to analyze the correlation between two time series, including correlation coefficient,
    fluctuation direction and fluctuation order.

    Parameters
    ----------
    (A) preprocess_method ---- Algorithms for filtering major trends and preserving local fluctuations of a time series
        6 preprocessing methods to choose:
        - diff: Differential methods to eliminate the impact of major trends
            - diff_times: An integer setting the times of running the differential function
        - holt_winters: Predicts values using three smoothing equations
            - holt_winters_parameters: A length 4 tuple which looks like this: (holt_winters_alpha, holt_winters_beta,
              holt_winters_gamma, holt_winters_period)
        - historical_avg: Predicting values based on the average value of the historical data
        - historical_med: Predicting values based on the median value of the historical data
        - wavelet: Using wavelet decomposition to only keep the high-frequency part
            - wavelet_window: The length of wavelet filter
        - none: Directly returning the data after normalizing and feature amplifying
    (B) analyze_method ---- Algorithms for doing correlation analysis of two time series
        2 analyzing methods to choose:
        - pearson: Calculating Pearson correlation coefficient
        - coflux: A way of computing cross correlation based on inner product
            - sliding_length: Deciding the range of the sliding
    Additionally, you may turn on the normalization_switch if you want to get a more accurate result when doing Pearson,
    but it could be computationally expensive. On the other hand, if you decide not to turn on the normalization_switch,
    the result could be distortion when the original data has a large range. Note that this swich only works when
    choosing "pearson" as analyze_method, and normalization is mandatory for "coflux".

    Examples
    ----------
    my_correlation_analysis = CorrelationAnalysis(preprocess_method='diff', analyze_method='pearson', diff_times=1)
    x, y = my_correlation_analysis.preprocess(x, y)
    correlation_result = my_correlation_analysis.analyze(x, y)

    Returns
    ----------
    A tuple whose first element is correlation coefficient, and whose second element is the shift coefficient
     representing the fluctuation order.
    The closer the coefficient to 1 or -1, the stronger the correlation between x and y.
    Positive correlation coefficient means they move in the same direction and vice versa.
    Positive shift coefficient means x fluctuates later than y and vice versa.
    """

    def __init__(self,
                 preprocess_method='diff',
                 analyze_method='coflux',
                 normalization_switch=False,
                 diff_times=1,
                 wavelet_window=3,
                 holt_winters_parameters=(0.2, 0.2, 0.2, 7),
                 sliding_length=0):
        self.preprocess_method = preprocess_method
        self.analyze_method = analyze_method

        self.normalization_switch = normalization_switch

        self.diff_times = diff_times
        self.wavelet_window = wavelet_window

        (self.holt_winters_alpha, self.holt_winters_beta, self.holt_winters_gamma,
         self.holt_winters_period) = holt_winters_parameters
        self.sliding_length = int(abs(sliding_length))

    def _preprocess_execute(self, data, method):
        if self.normalization_switch or self.analyze_method == 'coflux':
            data = self.normalize(data)

        method_map = {'diff': partial(self.diff, diff_times=self.diff_times),
                      'holt_winters': partial(self.holt_winters,
                                              alpha=self.holt_winters_alpha,
                                              beta=self.holt_winters_beta,
                                              gamma=self.holt_winters_gamma,
                                              period=self.holt_winters_period),
                      'historical_avg': self.historical_mean,
                      'historical_med': self.historical_median,
                      'wavelet': partial(self.wavelet,
                                         window=self.wavelet_window),
                      'none': lambda x: x}
        return np.nan_to_num(method_map[method](data))

    @staticmethod
    def diff(data, diff_times=1):
        """Simply uses the value of last day
        or last week separately to predict the current one
        -------------------------------------------------------------------
        :param diff_times: the times doing differential
        :param data: original data
        :return: flux time series
        """
        return np.diff(data, diff_times)

    @staticmethod
    def holt_winters(data, alpha=0.2, beta=0.2, gamma=0.2, period=7):
        """Calculates forecast values using three smoothing equations (level,
        trend and seasonal components) with three parameters ranged from
        0 to 1
        -------------------------------------------------------------------
        :param data: original data
        :param period: flux period
        :param alpha: 0.2, 0.4, 0.6, 0.8
        :param beta: 0.2, 0.4, 0.6, 0.8
        :param gamma: 0.2, 0.4, 0.6, 0.8
        :return: flux time series
        """
        s, t, p = list(), list(), list()
        s_last = data[0]
        t_last = data[0]
        x_prediction = list()
        for i, x in enumerate(data):
            p_last = 0 if i - period < 0 else p[i - period]
            s_new = alpha * (x - p_last) + (1 - alpha) * (s_last + t_last)
            t_new = beta * (s_new - s_last) + (1 - beta) * t_last
            p_new = gamma * (x - s_new) + (1 - gamma) * p_last
            s.append(s_new)
            t.append(t_new)
            p.append(p_new)
            x_prediction.append(s_new + p_new)
            s_last = s_new
            t_last = t_new
        return data - np.array(x_prediction)

    @staticmethod
    def historical_mean(data):
        """Uses the average value of historical data to predict the current
        one
        -------------------------------------------------------------------
        :param data: original data
        :return: flux time series
        """
        return data - np.mean(data)

    @staticmethod
    def historical_median(data):
        """Uses the median value of historical data to predict the current
        one
        -------------------------------------------------------------------
        :param data: original data
        :return: flux time series
        """
        return data - np.median(data)

    @staticmethod
    def wavelet(data, window=3):
        """Wavelet decomposition can cover the entire frequency domain of time
        series, and we set the high-frequency part as predictions
        -------------------------------------------------------------------
        :param data: original data
        :param window: length of wavelet filter
        :return: flux time series
        """
        data_wavelet = np.convolve(data, data[:window], mode='valid')
        origin = len(data)
        diff = window - 1
        return data[diff // 2: origin - diff + diff // 2] - data_wavelet

    def _analyze_execute(self, x, y, method):
        method_map = {'pearson': self._pearson_correlation_analysis,
                      'coflux': self._coflux_correlation_analysis}
        return method_map[method](x, y)

    @staticmethod
    def _inner_product(x, y):
        """Compute the inner product of two time series

        parameters
        ----------
        x : array_like time series
        y : array_like time series
        """
        return np.inner(x, y) * 2 - x[0] * y[0] - x[-1] * y[-1]

    def _coflux_correlation_analysis(self, x, y):
        """Compute Cross-correlation based on CoFlux and return a tuple representing
        the correlation analysis of the 2 given time series.

        Parameters
        ----------
        x : array_like time series
        y : array_like time series

        References
        ----------
        .. [1] Y. Su et al., CoFlux: Robustly Correlating KPIs by Fluctuations for
               Service Troubleshooting, 2019 IEEE/ACM 27th International Symposium
               on Quality of Service (IWQoS), 2019, pp. 1-10.
        """
        data_length = len(x)
        inner_product_xx = self._inner_product(x, x)
        inner_product_yy = self._inner_product(y, y)
        max_correlation_coefficient = self._inner_product(x, y) / (
                (inner_product_xx * inner_product_yy) ** 0.5)

        self.sliding_length = min(self.sliding_length, data_length - 1)

        shift = 0
        for s in range(-self.sliding_length, self.sliding_length + 1):
            x_slide = iter_shift(x, s, 0)
            correlation_coefficient = self._inner_product(x_slide, y) / (
                    (inner_product_xx * inner_product_yy) ** 0.5)
            if abs(correlation_coefficient) > abs(max_correlation_coefficient):
                max_correlation_coefficient = correlation_coefficient
                shift = s
        return max_correlation_coefficient, shift

    def _pearson_correlation_analysis(self, x, y):
        x, y = np.nan_to_num(x), np.nan_to_num(y)
        if np.max(x) == np.min(x) or np.max(y) == np.min(y):
            return 0, 0

        left, right = min(self.sliding_length, len(y)), min(self.sliding_length, len(y))
        max_correlation, final_shift = 0, 0
        for shift in range(-left, right + 1):
            correlation = cross_correlation(x, y, shift)
            if abs(correlation) > abs(max_correlation):
                max_correlation = correlation
                final_shift = shift
        return max_correlation, final_shift

    @staticmethod
    def normalize(data):
        """Normalizes the data using z-score and then amplifies the feature
        -------------------------------------------------------------------
        :param data: original data
        :return: preprocessed data
        """
        data = zscore(data)
        alpha = 0.5
        beta = 10
        for index, value in enumerate(data):
            temp = np.exp(min(abs(value), beta) * alpha) - 1
            data[index] = temp if value >= 0 else -temp
        return data

    def preprocess(self, x, y):
        """
        Inputs
        ----------
        x: An array-like time series
        y: An array-like time series
        """
        x = np.asarray(x)
        y = np.asarray(y)
        if np.all(x == x[0]) or np.all(y == y[0]):
            return x, y

        return (self._preprocess_execute(x, self.preprocess_method),
                self._preprocess_execute(y, self.preprocess_method))

    def analyze(self, x, y):
        """
        Inputs
        ----------
        x: An array-like time series
        y: An array-like time series
        """
        if np.all(x == x[0]) or np.all(y == y[0]):
            return 0, 0

        return self._analyze_execute(x, y, self.analyze_method)
