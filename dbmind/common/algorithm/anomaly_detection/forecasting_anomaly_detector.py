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
import math
import statistics

import numpy as np

from dbmind.common.algorithm.forecasting.arima_model.arima_alg import ARIMA
from dbmind.common.algorithm.forecasting.forecasting_algorithm import ForecastingFactory
from dbmind.common.algorithm.forecasting.forecasting_algorithm import compose_sequence, decompose_sequence
from dbmind.common.algorithm.forecasting.forecasting_algorithm import sequence_interpolate
from dbmind.common.algorithm.forecasting.forecasting_algorithm import trim_head_and_tail_nan
from dbmind.common.types import Sequence
from dbmind.common.algorithm.anomaly_detection.detector_params import THRESHOLD
from ._abstract_detector import AbstractDetector
from .threshold_detector import ThresholdDetector

# Create map for the sign of the anomaly direction (positive, negative, both)
DEFAULT_CONF_FACTOR = 2.5
not_allowed_values = frozenset(["", None])
SUSPICIOUS_RESULT_FACTOR = 3


class ForecastingAnomalyDetector(AbstractDetector):

    def __init__(self,
                 side="both",
                 low_threshold=0.5,
                 medium_threshold=1.5,
                 high_threshold=3,
                 min_forecast_length=60,
                 detection_area_in_minutes: int = 20,
                 max_z_score=None,
                 std_errors=None,
                 avg_errors=None,
                 max_ratio=None,
                 z_score_conf_factor: float = DEFAULT_CONF_FACTOR,
                 ratio_conf_factor: float = DEFAULT_CONF_FACTOR):
        """
        constructor for the class ForecastingAnomalyDetector
        :param side: type->string
        :param detection_area_in_minutes: type->int
        :param low_threshold: type->float
        :param medium_threshold: type->float
        :param high_threshold: type->float
        :param min_forecast_length: type->int
        :param max_z_score: type->float
        :param std_errors: type->float
        :param avg_errors: type->float
        :param max_ratio: type->float
        :param z_score_conf_factor: type->float
        :param ratio_conf_factor: type->float
        """

        forecast_only_params = [max_z_score, std_errors, avg_errors, max_ratio]
        forecast_only_params_set = frozenset(forecast_only_params)
        intersect = not_allowed_values.intersection(forecast_only_params_set)
        if len(intersect) != 0 and len(intersect) != len(forecast_only_params_set):
            raise ValueError(f"the values of max_z_score, std_errors, avg_errors, max_ratio :"
                             f"are {forecast_only_params}. They must be all exists or all None")
        self._side = side
        self._low_threshold = low_threshold
        self._medium_threshold = medium_threshold
        self._high_threshold = high_threshold
        self._min_forecast_length = min_forecast_length
        self._detection_area_in_minutes = int(detection_area_in_minutes)
        self._detection_area_size = 0
        self._predictions = None
        self._max_z_score = max_z_score
        self._std_errors = std_errors
        self._avg_errors = avg_errors
        self._max_ratio = max_ratio
        self._z_scores = None
        self._ratio_errors = None
        self._model = None
        self._z_score_conf_factor = float(z_score_conf_factor)
        self._ratio_conf_factor = float(ratio_conf_factor)

    def _fit(self, s: Sequence):
        # 1 interpolate (might be as the fourth stage after dividing to train and test sets)
        sequence_interpolated = sequence_interpolate(s)
        pure_sequence = sequence_interpolated

        step_in_seconds = int(s.step / 1000)  # it is in milliseconds
        # Calc the detection size by multiple the detection area in minutes in 60 (convert to seconds)
        # and divide by the step
        if step_in_seconds == 0:
            raise NotImplementedError(f'Difference in timestamp is zero')
        elif self._detection_area_in_minutes == 0:
            raise NotImplementedError(f'Parameter detection_area_in_minutes is zero')
        self._detection_area_size = int(self._detection_area_in_minutes * 60 / step_in_seconds)
        # 2 decompose the sequence to train and detection_area
        self._pure_train_area, self._pure_detection_area = decompose_train_detection_area(
            pure_sequence, self._detection_area_size)

        # 3 get the model to predict with
        self._model = self._get_model_for_anomaly_detection()

    def _get_model_for_anomaly_detection(self):
        """
        return the correct model
        :return: model: type->model

        """
        model = ForecastingFactory.get_instance(self._pure_train_area)
        return model

    def _predict(self, s: Sequence):
        length = len(s.values)
        if self.least_length is not None and length < self.least_length:
            return Sequence(timestamps=s.timestamps, values=[False] * length)

        # 1 do the forecast and create predictions sequence
        self._predictions = self.forecast_predictions(self._pure_train_area, self._pure_detection_area,
                                                      self._min_forecast_length, s.name)

        # 2 compose forecast sequence
        forecast_timestamps, forecast_values = compose_sequence(None, self._pure_train_area, self._predictions)

        forecast_sequence = Sequence(timestamps=forecast_timestamps, values=forecast_values)
        # 3 Compute squared errors and ratio

        computed_errors, self._ratio_errors = _compute_errors(
            list(s.values)[-self._detection_area_size:], list(forecast_sequence.values))

        # 4 Square all elements in the error list
        squared_errors = np.square(computed_errors)

        # 5 create sign check sequence
        sign_check_sequence = ThresholdDetector(high=THRESHOLD.get(self._side)[0], low=THRESHOLD.get(
            self._side)[1]).fit_predict(Sequence(s.timestamps[-self._detection_area_size:], computed_errors))

        # 6 anomalies  detect according to thresholds
        threshold_check_list = self.find_anomalies(squared_errors, self._ratio_errors, self._high_threshold)
        # 7 agg_and for results (multiply the directed bool list with the anomaly results)
        agg_list = [a * b for a, b in zip(list(sign_check_sequence.values), threshold_check_list)]

        return agg_list

    def get_predictions(self):
        return self._predictions

    def get_model(self):
        return self._model

    def get_max_z_score(self):
        return self._max_z_score

    def get_max_ratio(self):
        return self._max_ratio

    def get_avg_errors(self):
        return self._avg_errors

    def get_std_errors(self):
        return self._std_errors

    def find_anomalies(self, squared_errors, ratio_errors, high_threshold):
        """
        detect anomalies according to 3 severity levels: HIGH, MEDIUM, LOW
        :param squared_errors: type->list
        :param ratio_errors: type->list
        :param high_threshold: type->float
        :return: predictions_with_anomalies list: type->list
        """
        if self._avg_errors is None:
            self._avg_errors = np.mean(squared_errors)
            self._std_errors = np.std(squared_errors)
            self._z_scores = np.divide(np.abs(np.subtract(squared_errors, self._avg_errors)), self._std_errors)
            self._max_z_score = np.max(self._z_scores)
            self._max_ratio = (max(ratio_errors))
            threshold = self._avg_errors + high_threshold * self._std_errors
            predictions_with_anomalies = (squared_errors > threshold)
        else:
            self._z_scores = np.divide(np.abs(np.subtract(squared_errors, self._avg_errors)), self._std_errors)
            predictions_with_anomalies = np.logical_or(
                (self._z_scores > self._max_z_score * self._z_score_conf_factor),
                (ratio_errors > self._max_ratio * self._ratio_conf_factor))

        return list(predictions_with_anomalies)

    def get_z_scores(self):
        """
        :return: the model z_score np array
        """
        return self._z_scores

    def get_ratio_errors(self):
        """
        :return: the model ratio_errors np array
        """
        return self._ratio_errors

    def forecast_predictions(self, train_area, detection_area, min_forecast_length, metric_name):
        """
        return prediction list according to detection_area
        :param train_area: type->Sequence
        :param detection_area: type->Sequence
        :param min_forecast_length: type->Int
        :param metric_name: str
        :return: predictions: type->list
        """

        predictions = list()
        detection_area_length = detection_area.length
        detection_lst = list(detection_area.values)
        history = [x for x in list(train_area.values)]
        number_of_iterations = math.ceil(detection_area_length / min_forecast_length)
        curr_forecast_length = min_forecast_length
        logging.info("sliding window for %s, detection_area_length: %s len(history): %s, "
                     "min_forecast_length: %s number_of_iterations:%s",
                     metric_name, detection_area_length, len(history),
                     min_forecast_length, number_of_iterations)
        # Walk forward validation:
        for t in range(number_of_iterations):
            if t == number_of_iterations - 1:
                curr_forecast_length = len(detection_lst)
            # Find the model and forecast
            forecast_list = fit_and_forecast_for_anomaly_detection(self._model, history, curr_forecast_length,
                                                                   metric_name)
            # Append the forecast sequence to the predictions list
            predictions.extend(forecast_list)
            # append the relative part from the detection area
            history.extend(detection_lst[:curr_forecast_length])
            # shorten the beginning of the history in curr_forecast_length in order to create a sliding window
            history = history[curr_forecast_length:]
            # shorten the detection area
            detection_lst = detection_lst[curr_forecast_length:]

        return predictions


def decompose_train_detection_area(sequence: Sequence, detection_area_size):
    """
    return train and detection_area sets according to detection_area_in_minutes proportion
    :param sequence: type->Sequence
    :param detection_area_size: type->int
    :return: train : type->Sequence
    :return: detection_area : type-> Sequence
    """

    sequence_length = len(sequence)
    if detection_area_size >= sequence_length:
        raise NotImplementedError(
            f'detection_area_size: {detection_area_size} is greater than sequence_length: {sequence_length}.')
    train_length = int(sequence_length - detection_area_size)
    train = Sequence(timestamps=sequence.timestamps[0:train_length], values=sequence.values[0:train_length])
    detection_area = Sequence(timestamps=sequence.timestamps[train_length:sequence_length],
                              values=sequence.values[train_length:sequence_length])
    return train, detection_area


def fit_and_forecast_for_anomaly_detection(model, train_lst, detection_area_length, metric_name):
    """
    return forecast sequence in forecasting_minutes from training sequence
    :param train_lst: type->lst
    :param model: type->class
    :param detection_area_length: type->int
    :param metric_name: str
    :return: forecast_data: type->np.array
    """

    # 1 create a sequence
    train_lst_sequence = Sequence(timestamps=range(len(train_lst)), values=train_lst, name=metric_name)

    # 2 decompose sequence from seasonality if any
    seasonal_data, train_sequence = decompose_sequence(train_lst_sequence)
    the_model = model
    if isinstance(model, ARIMA) and model.order is not None:
        the_model = ARIMA(given_parameters=(model.order.ar, model.order.diff, model.order.ma))

    # 3 model fit
    the_model.fit(train_sequence)

    # 4 model forecast
    forecast_data = the_model.forecast(detection_area_length)
    # sometimes, there might be slight changes to the arima basic attributes which would make really bad predictions
    # like very large numbers or negative numbers which make no sense for security metrics
    # therefore, we should re run fit with no parameters
    std = statistics.stdev(list(train_sequence.values))
    if isinstance(model, ARIMA) and max(forecast_data) > max(list(train_sequence.values)) + \
            SUSPICIOUS_RESULT_FACTOR * std or \
            min(forecast_data) < -SUSPICIOUS_RESULT_FACTOR:
        logging.info("Re doing arima for for %s", metric_name)
        the_model = model
        the_model.fit(train_sequence)
        forecast_data = the_model.forecast(detection_area_length)

    # 5 compose the prediction with seasonality if any
    _, forecast_values = compose_sequence(seasonal_data, train_sequence, forecast_data)
    forecast_values = trim_head_and_tail_nan(forecast_values)

    return forecast_values


def _compute_errors(detection_area, predictions):
    """
    return difference between actual and predicted items
    :param predictions: type->Sequence
    :param detection_area: type->list
    :return: error_list: type->list
    :return: ratio_list: type->list
    """

    # Create list for difference between actual and predicted items
    error_list = np.subtract(detection_area, predictions)

    # Create list for ratio between (difference between actual and predicted (error_list)) and predicted items
    for i in range(len(predictions)):
        # If the denominator is lower than 1, replace it with 1, in order to avoid from large numbers
        if predictions[i] < 1:
            predictions[i] = 1
    ratio_list = np.divide(error_list, predictions)
    return error_list, ratio_list
