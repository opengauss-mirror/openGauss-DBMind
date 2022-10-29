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

import warnings

import numpy as np
from scipy import signal

warnings.filterwarnings("ignore")

MIN_WINDOW = 9


def acovf(x):  # auto-covariances function
    x = np.array(x)
    n = x.shape[0]
    acov = np.correlate(x, x, "full")  # self auto-covariances of x
    acov = acov[n - 1:] / n  # full-acov is symmetric, so we only need half of it.
    return acov


def acf(x, nlags=None):  # auto-correlation function
    if nlags is None:
        nlags = len(x) - 1  # return the correlations of all the lags

    x = np.array(x)
    x_diff = x - x.mean()
    acov = acovf(x_diff)
    res = acov[: nlags + 1] / acov[0]  # partition and normalization.
    return res


def is_seasonal_series(x, high_ac_threshold: float = 0.5, min_seasonal_freq=3):
    """
    The method wants to find the period of the x through finding
    the peaks of the auto-correlation coefficients which are higher than
    their left and right value at the same time.

    Obviously, the first one and the last one don't meet the request.

    If some of auto-correlation coefficients are less than 'high_ac_threshold',
    The method thinks these peaks are the consequences of the noise and ignore them.

    At last if the peaks found is fewer than 'min_seasonal_freq', The method thinks
    the input x sequence is not seasonal.
    """
    # The periodic analysis is independent of the trend. If the trend components
    # are not separated, the autocorrelation will be significantly affected and
    # it is difficult to identify the period. So we extract the trend components.
    window = max(MIN_WINDOW, len(x) // (min_seasonal_freq + 1))
    detrended = x - decompose_trend(x, np.ones(window) / window)

    ac_coef = acf(detrended, nlags=len(x) - 1)  # auto-correlation coefficient
    valleys = signal.find_peaks(-ac_coef, height=(0, None))[0]
    lower_bound = valleys[0] if valleys.size else 0
    high_ac_peak_pos = signal.find_peaks(ac_coef, height=(0, None))[0]

    beyond_threshold = np.argwhere(ac_coef >= high_ac_threshold).flatten()
    high_ac_peak_pos = np.intersect1d(high_ac_peak_pos, beyond_threshold)
    # Noise in autocorrelation coefficients may be mistaken for peaks.
    # According to experience, we think that a sequence with obvious periodicity
    # will have a minimum value of the autocorrelation coefficient when moving
    # for half a period. Therefore, we think that the period can only appear
    # after the first minimum value.
    high_ac_peak_pos = high_ac_peak_pos[
        (high_ac_peak_pos < len(ac_coef) // 2) &
        (high_ac_peak_pos > lower_bound)
    ]
    if len(high_ac_peak_pos) >= min_seasonal_freq:
        return True, int(high_ac_peak_pos[np.argmax(ac_coef[high_ac_peak_pos])])

    return False, None


def get_seasonal_period(values, high_ac_threshold: float = 0.5, min_seasonal_freq=3):
    return is_seasonal_series(values, high_ac_threshold, min_seasonal_freq)[1]


def _conv_kernel(period):
    """
    If period is even, convolution kernel is [0.5, 1, 1, ... 1, 0.5] with the size of (period + 1)
    else if period is odd, convolution kernel is [1, 1, ... 1] with the size of period
    Make sure the the size of convolution kernel is odd.
    """

    if period % 2 == 0:
        return np.array([0.5] + [1] * (period - 1) + [0.5]) / period
    else:
        return np.ones(period) / period


def extrapolate(x, head, tail, length):
    head_template = x[head:head + length]
    k = np.polyfit(np.arange(1, len(head_template) + 1), head_template, deg=1)
    head = k[0] * np.arange(head) + x[0] - head * k[0]
    tail_template = x[-tail - length:-tail]
    k = np.polyfit(np.arange(1, len(tail_template) + 1), tail_template, deg=1)
    tail = k[0] * np.arange(tail) + x[-1] + k[0]
    x = np.r_[head, x, tail]
    return x


def decompose_trend(x, conv_kernel):
    """
    To decompose the trend component from x, the method convolve x with 'valid' mode.
    The size of the convlolution result is (x - len(conv_kernel) + 1),
    so the method then pads both ends of result.
    """

    length = len(conv_kernel)
    tail = (length - 1) // 2
    head = length - 1 - tail
    result = np.convolve(x, conv_kernel, mode='valid')
    result = extrapolate(result, head, tail, length)
    return result


def decompose_seasonal(x, detrended, period):
    """
    To decompose the seasonal component from detrended data, the method overlays
    the detrended data into one period and calculate its average to minimize
    the influence of the residuals and duplicates the period_averages as
    the seasonal component.
    """

    n = len(x)
    period_averages = np.array([np.mean(detrended[i::period]) for i in range(period)])
    period_averages -= np.mean(period_averages)
    seasonal = np.tile(period_averages, n // period + 1)[:n]
    return seasonal


def seasonal_decompose(x, period=None):
    """
    Decompose the input array x into three components: seasonal, trend, residual
    """

    if np.ndim(x) > 1:
        raise ValueError("The input data must be 1-D numpy.array.")

    if not isinstance(period, int):
        raise ValueError("You must specify a period.")

    if not all(np.isfinite(x)):
        raise ValueError("The input data has infinite value or nan value.")

    if x.shape[0] < 2 * period:
        raise ValueError(f"The input data should be longer than two periods:{2 * period} at least.")

    trend = decompose_trend(x, _conv_kernel(period))
    detrended = x - trend
    seasonal = decompose_seasonal(x, detrended, period)
    resid = detrended - seasonal
    return seasonal, trend, resid
