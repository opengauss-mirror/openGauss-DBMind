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


def get_statistic_data(values):
    avg_val, max_val, min_val, the_95th_val = 0, 0, 0, 0
    if values:
        avg_val = round(sum(values) / len(values), 4)
        max_val = round(max(values), 4)
        min_val = round(min(values), 4)
        the_95th_val = round(np.nanpercentile(values, 95), 4)
    return avg_val, min_val, max_val, the_95th_val


def box_plot(values, n=1.5):
    upper, lower = -np.inf, np.inf
    if values:
        the_75th_per = round(np.percentile(values, 75), 4)
        the_25th_per = round(np.percentile(values, 25), 4)
        iqr = the_75th_per - the_25th_per
        upper = the_75th_per + n * iqr
        lower = the_25th_per - n * iqr
    return upper, lower


def n_sigma(values, n=3):
    upper, lower = -np.inf, np.inf
    if values:
        mean = round(np.mean(values), 4)
        std = round(np.std(values), 4)
        upper = mean + n * std
        lower = mean - n * std
    return upper, lower


def least_square(x, y):
    x, y = np.asarray(x), np.asarray(y)
    if np.unique(x).size == 1:
        raise ValueError("The values on x axis are the same.")
    elif np.unique(y).size == 1:
        return 0, y[0], 1

    n = len(x)
    scale_x, scale_y = np.abs(x).max(), np.abs(y).max()
    x = x / scale_x
    y = y / scale_y

    sx = np.sum(x)
    sy = np.sum(y)
    sxx = np.sum(np.multiply(x, x))
    syy = np.sum(np.multiply(y, y))
    sxy = np.sum(np.multiply(x, y))
    a = (n * sxy - sx * sy) / (n * sxx - sx * sx)
    b = (sy - a * sx) / n

    numerator = syy + a * a * sxx + b * b * n + 2 * a * b * sx - 2 * a * sxy - 2 * b * sy
    denominator = syy - sy * sy / n
    r2 = 1 - numerator / denominator

    a, b = a * scale_y / scale_x, b * scale_y

    return a, b, r2
