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

import logging
import threading
from types import SimpleNamespace
from typing import Union, List

import numpy as np

from dbmind.common.utils import dbmind_assert
from .. import seasonal as seasonal_interface
from ..stat_utils import sequence_interpolate, trim_head_and_tail_nan
from ...types import Sequence

LINEAR_THRESHOLD = 0.9


class ForecastingAlgorithm:
    """abstract forecast alg class"""

    def fit(self, sequence: Sequence):
        """the subclass should implement, tarin model param"""
        pass

    def forecast(self, forecast_length: int) -> Union[List, np.array]:
        """the subclass should implement, forecast series according history series"""
        pass


class ForecastingFactory:
    """the ForecastingFactory can create forecast model"""
    _CACHE = threading.local()  # Reuse an instantiated object.

    @staticmethod
    def _get(algorithm_name):
        if not hasattr(ForecastingFactory._CACHE, algorithm_name):
            if algorithm_name == 'linear':
                from .simple_forecasting import SimpleLinearFitting
                setattr(ForecastingFactory._CACHE, algorithm_name, SimpleLinearFitting(avoid_repetitive_fitting=True))
            elif algorithm_name == 'arima':
                from .arima_model.arima_alg import ARIMA
                setattr(ForecastingFactory._CACHE, algorithm_name, ARIMA())
            else:
                raise NotImplementedError(f'Failed to load {algorithm_name} algorithm.')

        return getattr(ForecastingFactory._CACHE, algorithm_name)

    @staticmethod
    def get_instance(sequence) -> ForecastingAlgorithm:
        """Return a forecast model according to the feature of given sequence."""
        linear = ForecastingFactory._get('linear')
        linear.refit()
        linear.fit(sequence)
        if linear.r2_score >= LINEAR_THRESHOLD:
            logging.debug('Choose linear fitting algorithm to forecast.')
            return linear
        logging.debug('Choose ARIMA algorithm to forecast.')
        return ForecastingFactory._get('arima')


def _check_forecasting_time(forecasting_time):
    """
    check whether input params forecasting_time is valid.
    :param forecasting_time: int or float
    :return: None
    :exception: raise ValueError if given parameter is invalid.
    """
    check_result = True
    message = ""
    if not isinstance(forecasting_time, (int, float)):
        check_result = False
        message = "forecasting_time value type must be int or float"
    elif forecasting_time < 0:
        check_result = False
        message = "forecasting_time value must >= 0"
    elif forecasting_time in (np.inf, -np.inf, np.nan, None):
        check_result = False
        message = f"forecasting_time value must not be:{forecasting_time}"

    if not check_result:
        raise ValueError(message)


def decompose_sequence(sequence):
    seasonal_data = None
    raw_data = np.array(sequence.values)
    is_seasonal, period = seasonal_interface.is_seasonal_series(
        raw_data,
        high_ac_threshold=0.1,
        min_seasonal_freq=2
    )
    if is_seasonal:
        seasonal, trend, residual = seasonal_interface.seasonal_decompose(raw_data, period=period)
        train_sequence = Sequence(timestamps=sequence.timestamps, values=trend)
        train_sequence = sequence_interpolate(train_sequence)
        seasonal_data = SimpleNamespace(
            is_seasonal=is_seasonal,
            seasonal=seasonal,
            trend=trend,
            resid=residual,
            period=period
        )
    else:
        train_sequence = sequence
    return seasonal_data, train_sequence


def compose_sequence(seasonal_data, train_sequence, forecast_values):
    forecast_length = len(forecast_values)
    if seasonal_data and seasonal_data.is_seasonal:
        seasonal = seasonal_data.seasonal
        resid = seasonal_data.resid
        resid[np.abs(resid - np.mean(resid)) > np.std(resid) * 3] = np.mean(resid)
        dbmind_assert(len(seasonal) == len(resid))
        period = seasonal_data.period
        latest_period = seasonal[-period:] + resid[-period:]
        if len(latest_period) < forecast_length:  # pad it.
            padding_length = forecast_length - len(latest_period)
            addition = np.pad(latest_period, (0, padding_length), mode='wrap')
        else:
            addition = latest_period[:forecast_length]

        forecast_values = forecast_values + addition

    forecast_timestamps = [train_sequence.timestamps[-1] + train_sequence.step * i
                           for i in range(1, forecast_length + 1)]
    return forecast_timestamps, forecast_values


def quickly_forecast(sequence, forecasting_minutes, lower=0, upper=float('inf'),
                     given_model=None, return_model=False):
    """
    Return forecast sequence in forecasting_minutes from raw sequence.
    :param sequence: type->Sequence
    :param forecasting_minutes: type->int or float
    :param lower: The lower limit of the forecast result
    :param upper: The upper limit of the forecast result.
    :param given_model: type->ARIMA or SimpleLinearFitting
    :param return_model: type->bool
    :return: forecast sequence: type->Sequence
    """

    try:
        # 1. check for sequence length and forecasting minutes
        if len(sequence) <= 1:
            raise ValueError("The sequence length is too short.")
        _check_forecasting_time(forecasting_minutes)
        forecasting_length = int(forecasting_minutes * 60 * 1000 / sequence.step)
        if forecasting_length == 0 or forecasting_minutes == 0:
            raise ValueError("The forecasting minutes is too short.")

        # 2. interpolate
        interpolated_sequence = sequence_interpolate(sequence)

        # 3. decompose sequence
        seasonal_data, train_sequence = decompose_sequence(interpolated_sequence)

        # 4. get model from ForecastingFactory or given model
        if given_model is None:
            model = ForecastingFactory.get_instance(train_sequence)
            model.fit(train_sequence)
        else:
            model = given_model

        forecast_data = model.forecast(forecasting_length)
        forecast_data = trim_head_and_tail_nan(forecast_data)
        dbmind_assert(len(forecast_data) == forecasting_length)

        # 5. compose sequence
        forecast_timestamps, forecast_values = compose_sequence(
            seasonal_data,
            train_sequence,
            forecast_data
        )

        for i in range(len(forecast_values)):
            forecast_values[i] = min(forecast_values[i], upper)
            forecast_values[i] = max(forecast_values[i], lower)

        result_sequence = Sequence(
            timestamps=forecast_timestamps,
            values=forecast_values,
            name=sequence.name,
            labels=sequence.labels
        )
    except ValueError as e:
        logging.warning(f"An Exception was raised while quickly forecasting: {e}")
        result_sequence, model = Sequence(), None

    if not return_model:
        return result_sequence
    else:
        return result_sequence, model
