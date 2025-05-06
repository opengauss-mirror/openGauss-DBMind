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

from math import log

import numpy as np
try:
    from scipy.optimize import minimize
except ImportError:
    pass


def back_mean(x, depth):
    x = np.array(x)
    x = x[~(np.isnan(x) | np.isinf(x))]
    means = []
    summary = x[:depth].sum()
    means.append(summary / depth)
    for i in range(depth, len(x)):
        summary += x[i] - x[i - depth]
        means.append(summary / depth)

    return np.array(means)


def ewma(x, alpha=0.1):
    x = np.array(x)
    x = x[~(np.isnan(x) | np.isinf(x))]
    s = [x[0]]
    for i in range(1, len(x)):
        temp = alpha * x[i] + (1 - alpha) * s[-1]
        s.append(temp)

    return s[-1]


def log_likelihood(y, gamma, sigma):
    """
    Compute the log-likelihood for the Generalized Pareto Distribution (μ=0)

    Parameters
    ----------
    y : numpy.array
        observations
    gamma : float
        GPD index parameter
    sigma : float
        GPD scale parameter (>0)
    Returns
    ----------
    float
        log-likelihood of the sample Y to be drawn from a GPD(γ,σ,μ=0)
    """
    n = y.size
    if gamma != 0:
        tau = gamma / sigma
        likelihood = -n * log(sigma) - (1 + (1 / gamma)) * (np.log(1 + tau * y)).sum()
    else:
        likelihood = n * (1 + log(y.mean()))

    return likelihood


def roots_finder(func, jac, bounds, n_points, method):
    """
    Find possible roots of a scalar function

    Parameters
    ----------
    func : function
        scalar function
    jac : jacobian function
        first order derivative of the function
    bounds : tuple
        (min,max) interval for the roots search
    n_points : int
        maximum number of roots to output
    method : str
        'regular' : regular sample of the search interval,
        'random' : uniform (distribution) sample of the search interval

    Returns
    ----------
    numpy.array
        possible roots of the function
    """
    def obj_func(x, funtion, jacobian):
        g = 0
        j = np.zeros(x.shape)
        i = 0
        for n in x:
            fx = funtion(n)
            g += fx ** 2
            j[i] = 2 * fx * jacobian(n)
            i += 1

        return g, j

    if bounds[0] == bounds[1]:
        return np.array([0.0])

    if method == 'regular':
        step = (bounds[1] - bounds[0]) / (n_points + 1)
        x0 = np.arange(bounds[0] + step, bounds[1], step)
    else:
        x0 = np.random.uniform(bounds[0], bounds[1], n_points)

    opt = minimize(
        lambda endog: obj_func(endog, func, jac), x0,
        method='L-BFGS-B',
        jac=True,
        bounds=[bounds] * len(x0)
    )
    optimized_x = opt.x

    return np.unique(optimized_x)


def grimshaw(peaks, epsilon=1e-8, n_points=10):
    """
    Compute the GPD parameters estimation with the Grimshaw's trick

    Parameters
    ----------
    peaks: np.ndarray
        outlier peaks
    epsilon : float
        numerical parameter to perform (default : 1e-8)
    n_points : int
        maximum number of candidates for maximum likelihood (default : 10)
    Returns
    ----------
    gamma_best,sigma_best,ll_best
        gamma estimates, sigma estimates and corresponding log-likelihood
    """

    def u(s):
        return 1 + np.log(s).mean()

    def v(s):
        return np.mean(1 / s)

    def w(y, t):
        s = 1 + t * y
        us = u(s)
        vs = v(s)
        return us * vs - 1

    def jac_w(y, t):
        s = 1 + t * y
        us = u(s)
        vs = v(s)
        jac_us = (1 / t) * (1 - vs)
        jac_vs = (1 / t) * (-vs + np.mean(1 / s ** 2))
        return us * jac_vs + vs * jac_us

    if not peaks.size:
        return 0, 0, 0

    y_min = np.nanmin(peaks)
    y_max = np.nanmax(peaks)
    y_mean = float(np.nanmean(peaks))

    a = -1 / y_max
    if abs(a) < 2 * epsilon:
        epsilon = abs(a) / n_points

    b = 2 * (y_mean - y_min) / (y_mean * y_min)
    c = 2 * (y_mean - y_min) / (y_min ** 2)

    # We look for possible roots
    left_zeros = roots_finder(
        lambda t: w(peaks, t),
        lambda t: jac_w(peaks, t),
        (a + epsilon, -epsilon),
        n_points,
        'regular'
    )

    right_zeros = roots_finder(
        lambda t: w(peaks, t),
        lambda t: jac_w(peaks, t),
        (b, c),
        n_points,
        'regular'
    )

    # all the possible roots
    zeros = np.concatenate((left_zeros, right_zeros))

    # 0 is always a solution so we initialize with it
    gamma_best = 0
    sigma_best = y_mean
    ll_best = log_likelihood(peaks, gamma_best, sigma_best)

    # we look for better candidates
    for z in zeros:
        gamma = u(1 + z * peaks) - 1
        sigma = gamma / z
        ll = log_likelihood(peaks, gamma, sigma)
        if ll > ll_best:
            gamma_best = gamma
            sigma_best = sigma
            ll_best = ll

    return gamma_best, sigma_best, ll_best
