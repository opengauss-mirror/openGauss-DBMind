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

import numpy as np
from scipy.stats import pearsonr, zscore


def dtw_distance(x, y, max_length=10):
    """Compute Dynamic Time Warping (DTW) similarity measure between
    time series and return it.
    DTW is computed as the Euclidean distance between aligned time series,
    i.e., if :math:`\pi` is the optimal alignment path:

    .. math::

        DTW(X, Y) = \sqrt{\sum_{(i, j) \in \pi} \|X_{i} - Y_{j}\|^2}
    Parameters
    ----------
    x : array_like
    y : array_like
    max_length : int
        Maximum warping path length.
    References:
    ----------
    .. [1] H. Sakoe, S. Chiba, "Dynamic programming algorithm optimization for
           spoken word recognition," IEEE Transactions on Acoustics, Speech and
           Signal Processing, vol. 26(1), pp. 43--49, 1978.
    """
    matrix = {}
    length_delta = abs(len(x) - len(y))
    max_length = max_length if max_length > length_delta else length_delta
    for i in range(-1, len(x)):
        for j in range(-1, len(y)):
            matrix[(i, j)] = float('inf')
    matrix[(-1, -1)] = 0
    for i in range(len(x)):
        for j in range(max(0, i - max_length), min(len(y), i + max_length)):
            dist = (x[i] - y[j]) ** 2
            matrix[(i, j)] = dist + min(matrix[(i - 1, j)], matrix[(i, j - 1)], matrix[(i - 1, j - 1)])
    return np.sqrt(matrix[len(x) - 1, len(y) - 1])


def lb_keogh(x, y, radius=10):
    """Compute LB_Keogh and return it.

    LB_Keogh is to compute distance between the time series and the envelope of another time series.

    Parameters
    ----------
    x : array_like
    y : array_like
    radius : int
        Radius to be used for the envelope generation.
    References
    ----------
    .. [1] Keogh, E. Exact indexing of dynamic time warping. In International
       Conference on Very Large Data Bases, 2002. pp 406-417.
    """
    lb_sum = 0
    for ind, i in enumerate(x):
        lower_bound = min(y[(ind - radius if ind - radius >= 0 else 0):(ind + radius)])
        upper_bound = max(y[(ind - radius if ind - radius >= 0 else 0):(ind + radius)])
        if i > upper_bound:
            lb_sum = lb_sum + (i - upper_bound) ** 2
        elif i < lower_bound:
            lb_sum = lb_sum + (i - lower_bound) ** 2
    return np.sqrt(lb_sum)


def pearson(x, y):
    return pearsonr(x, y)[0]


def amplify_feature(data):
    alpha = 0.5
    beta = 10
    data_zscore = zscore(data)
    res = []
    for x in list(data_zscore):
        if x < 0:
            res.append(-np.exp(min(abs(x), beta) * alpha) + 1)
        else:
            res.append(np.exp(min(x, beta) * alpha) - 1)
    return res


def iter_shift(arr, num, fill_value=0):
    if num == 0:
        return arr
    if num > 0:
        return np.concatenate((np.full(num, fill_value), arr[:-num]))
    else:
        return np.concatenate((arr[-num:], np.full(-num, fill_value)))


def cross_correlation(data1, data2, shift_num):
    return pearson(data1, iter_shift(data2, shift_num))


def max_cross_correlation(data1, data2, left=0, right=0):
    data1, data2 = np.nan_to_num(data1), np.nan_to_num(data2)
    if np.max(data1) == np.min(data1) or np.max(data2) == np.min(data2):
        return 0, 0

    left, right = min(left, len(data2)), min(right, len(data2))
    max_correlation, final_shift = 0, 0
    for shift in range(-left, right + 1):
        correlation = cross_correlation(data1, data2, shift)
        if abs(correlation) > abs(max_correlation):
            max_correlation = correlation
            final_shift = shift
    return max_correlation, final_shift
