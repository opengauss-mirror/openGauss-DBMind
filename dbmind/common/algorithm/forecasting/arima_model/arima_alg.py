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

import itertools
import logging
from types import SimpleNamespace

import numpy as np
try:
    from scipy import optimize
    from scipy.signal import lfilter
except ImportError:
    pass

from ..adf import adfuller
from ..forecasting_utils import (
    InvalidParameter,
    yule_walker,
    hannan_rissanen,
    diff_heads,
    un_diff,
    ar_trans_params,
    ar_inv_trans_params,
    ma_trans_params,
    ma_inv_trans_params,
    is_invertible,
)
from ..forecasting_algorithm import ForecastingAlgorithm

from dbmind.common.utils import dbmind_assert

MIN_AR_ORDER, MAX_AR_ORDER = 0, 6
MIN_MA_ORDER, MAX_MA_ORDER = 0, 6
MIN_DIFF_TIMES, MAX_DIFF_TIMES = 0, 3
P_VALUE_THRESHOLD = 0.05


def trans_params(params, p, q):
    """
    transform the params to make it easier to be fitted in LBFGS model.
    :param params: type -> ndarray  The ARIMA model params.
    :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                         how many historical data the AR procedure uses.
    :param q: type->int  Moving Average order of the ARIMA model which indicates
                         how many historical resid the MA procedure uses.
    """

    newparams = np.zeros_like(params)
    if p:
        newparams[:p] = ar_trans_params(params[:p].copy())

    if q:
        newparams[p: p + q] = ma_trans_params(params[p: p + q].copy())

    return newparams


def inv_trans_params(params, p, q):
    """
    Inverse transform the params to recover params from transform.
    fit p, q for ARIMA model.
    :param params: type -> ndarray  The ARIMA model params.
    :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                         how many historical data the AR procedure uses.
    :param q: type->int  Moving Average order of the ARIMA model which indicates
                         how many historical resid the MA procedure uses.
    """

    newparams = np.zeros_like(params)
    if p:
        newparams[:p] = ar_inv_trans_params(params[:p])

    if q:
        newparams[p: p + q] = ma_inv_trans_params(params[p: p + q])

    return newparams


class ARIMA(ForecastingAlgorithm):
    """
    ARIMA is a method which forecast series‘s future according to its own history
    ARIMA = AR(Auto-Regressive) + I(Integrated) + MA(Moving Average)
    """

    def __init__(self, is_transparams=False, given_parameters=None):
        self.is_transparams = is_transparams
        self.given_parameters = given_parameters
        self.original_data = None
        self.order = None
        self.endog = None
        self.nobs = None
        self.params = None
        self.resid = None

    def fit(self, sequence):
        self.original_data = np.array(sequence.values).astype('float64')
        if self.given_parameters is None:
            # To determine d by Augmented-Dickey-Fuller method.
            n_diff = MIN_DIFF_TIMES
            for n_diff in range(MIN_DIFF_TIMES, MAX_DIFF_TIMES + 1):
                diff_data = np.diff(self.original_data, n=n_diff)
                adf_res = adfuller(diff_data, max_lag=None)
                if adf_res[1] < P_VALUE_THRESHOLD and adf_res[0] < adf_res[4]['5%']:
                    d = n_diff
                    break
            else:
                d = n_diff

            orders = []
            p_q_pairs = itertools.product(
                range(MIN_AR_ORDER, MAX_AR_ORDER + 1, 2),
                range(MIN_MA_ORDER, MAX_MA_ORDER + 1, 2)
            )
            for p, q in p_q_pairs:  # Look for the optimal parameters (p, q).
                if p == 0 and q == 0:
                    continue

                try:
                    self.fit_once(p, d, q)
                    if not np.isnan(self.bic):
                        orders.append((self.bic, p, q))
                except InvalidParameter:
                    continue

            sorted_orders = sorted(orders)
            if len(sorted_orders) == 0:
                raise InvalidParameter(
                    'Cannot get proper parameters for the sequence: %s.' % str(sequence.values)
                )

            _, p0, q0 = sorted_orders[0]
            for p, q in [(p0 - 1, q0), (p0, q0 - 1), (p0 + 1, q0), (p0, q0 + 1)]:
                if p < 0 or q < 0:
                    continue

                try:
                    self.fit_once(p, d, q)
                    if not np.isnan(self.bic):
                        orders.append((self.bic, p, q))
                except InvalidParameter:
                    continue

            for _, p, q in sorted(orders):
                try:
                    self.fit_once(p, d, q)
                    break
                except InvalidParameter:
                    continue
            else:
                raise AttributeError('Not any (p, d, q) combination is available.')

        else:
            p, d, q = self.given_parameters
            self.fit_once(p, d, q)

        self.resid = self.get_resid()

    def fit_once(self, p, d, q):
        """
        fit p, q for ARIMA model.
        :param p: type->int  Auto-Correlation order of the ARIMA model which indicates
                             how many historical data the AR procedure uses.
        :param d: type->int  Integration times which indicate how many times to diff
                             the data to make it stationary.
        :param q: type->int  Moving Average order of the ARIMA model which indicates
                             how many historical resid the MA procedure uses.
        """

        def loglike(params):
            if self.is_transparams:
                params = trans_params(params, p, q)
            return -self.loglike_css(params) / self.nobs

        y = np.diff(self.original_data, n=d).copy()
        self.nobs = len(y)
        self.endog = y
        self.order = SimpleNamespace(ar=p, diff=d, ma=q)

        old_hash = hash(self.endog.tobytes())
        start_params = self._fit_start_params()
        new_hash = hash(self.endog.tobytes())
        dbmind_assert(old_hash == new_hash)

        lbfgs_attributes = {
            'disp': 0,
            'm': 12,
            'pgtol': 1e-08,
            'factr': 100.0,
            'approx_grad': True,
            'maxiter': 500
        }
        res = optimize.fmin_l_bfgs_b(loglike, start_params, **lbfgs_attributes)
        self.params = res[0]
        if self.is_transparams:
            self.params = trans_params(self.params, p, q)

    def _fit_start_params(self):
        """
        compute start coeffs of ar and ma for optimize.
        :return start_params: type->np.array
        """

        p = self.order.ar
        q = self.order.ma
        y = self.endog
        start_params = np.zeros(p + q)
        ar_params, ma_params = np.zeros(p), np.zeros(q)

        if p and q:
            ar_ma_params = hannan_rissanen(y, p, q)
            ar_params = ar_ma_params[:p]
            ma_params = ar_ma_params[-q:]
        elif not p and q:  # Better algorithm?
            ar = yule_walker(y, order=q)
            ar_coeffs = np.r_[[1], -ar.squeeze()]
            impulse = np.r_[[1], np.zeros(q)]
            ma_params = lfilter([1], ar_coeffs, impulse)[1:]  # ar empty or ma empty?
        elif p and not q:
            ar_params = yule_walker(y, order=p)

        if p > 0 and is_invertible(ar_params):
            logging.debug('Non-stationary starting autoregressive parameters found. '
                          'Using zeros as starting parameters.')
            ar_params *= 0

        if q > 0 and is_invertible(ma_params):
            logging.debug('Non-invertible starting moving-average parameters found. '
                          'Using zeros as starting parameters.')
            ma_params *= 0

        start_params[:p] = ar_params
        start_params[p: p + q] = ma_params

        if self.is_transparams:
            start_params = inv_trans_params(start_params, p, q)

        return start_params

    def forecast(self, steps):
        """
        return the forecast data form history data with ar coeffs,
        ma coeffs and diff order.
        :param steps: type->int
        :return forecast: type->np.array
        """

        p = self.order.ar
        q = self.order.ma
        ar_params = self.params[:p]
        ma_params = self.params[p: p + q]
        ar = np.r_[1, -ar_params]
        ma = np.r_[1, ma_params]

        eta = np.r_[self.resid, np.zeros(steps)]
        predicted = lfilter(ma, ar, eta)[-steps:]

        if self.order.diff:
            heads = diff_heads(self.original_data[-self.order.diff:], self.order.diff)
            predicted = un_diff(predicted, heads)[self.order.diff:]
        else:
            predicted += self.original_data[-1] - predicted[0]

        return predicted

    def loglike_css(self, params):
        """
        return the log-likelihood function to compute BIC.
        The ancillary parameter is assumed to be the last element of
        the params vector
        :param params: type->np.array
        :return llf: type->float
        """

        resid = self.get_resid(params)
        nobs = len(resid)
        l2 = np.linalg.norm(resid)
        sigma2 = np.maximum(params[-1] ** 2, 1e-6)
        llf = -0.5 * nobs * (
            np.log(2 * np.pi) +
            l2 ** 2 / sigma2 / nobs +  # when AR only: 2 * np.log(l2) - np.log(nobs)
            np.log(sigma2)  # constant
        )
        return llf

    @property
    def llf(self):
        """
        the llf for residuals estimated is used to compute BIC
        """

        return self.loglike_css(self.params)

    @property
    def bic(self):
        """
        Bayesian Infomation Criterion
        the BIC is for measuring the criterion of the model.
        """

        dof_model = self.order.ar + self.order.ma
        return -2 * self.llf + np.log(self.nobs) * dof_model

    def get_resid(self, params=None):
        """
        return the resid related to moving average
        :param params: type->np.array
        :return resid: type->np.array
        """

        if params is None:
            params = self.params

        params = np.asarray(params)
        p = self.order.ar
        q = self.order.ma
        y = self.endog

        ar_params = np.r_[1, -params[:p]]
        ma_params = np.r_[1, params[p: p + q]]
        # parameter zi was commented out
        # zi from lfilter_zi(armax, mamax) requires same length for ar and ma
        resid = lfilter(ar_params, ma_params, y)
        return resid
