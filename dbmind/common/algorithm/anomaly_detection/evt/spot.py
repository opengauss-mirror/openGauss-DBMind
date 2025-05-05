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

import math
from collections import deque

import numpy as np

from dbmind.common.types import Sequence

from .utils import back_mean, grimshaw, ewma


class SPOT:
    """
    This class allows to run SPOT algorithm on univariate dataset (upper and lower bounds)

    Attributes
    ----------
    probability : float
        Detection level (risk), chosen by the user

    depth : int
        Number of observations to compute the moving average

    update_interval : int
        retrain interval

    init_quantile : (float, float)
        initial threshold computed during the calibration step
    """

    def __init__(self, probability=1e-4, depth=40, update_interval=0, side="both",
                 init_quantile=(0.02, 0.98)):
        self.probability = probability
        self.depth = depth
        self.update_interval = update_interval
        self.side = side
        self.init_quantile = init_quantile

        self.window = deque(maxlen=depth)
        self.history = deque(maxlen=update_interval * 3)
        self.n_samples = 0
        self.counter = 0
        self.latest = 0

        self.extreme_quantile = {}
        self.init_threshold = {}
        self.peaks = {}

    def fit(self, data):
        """
        Initial batch to calibrate the algorithm ()

        Parameters
        ----------
        data : np.array
            An array, any object exposing the array interface, an object whose
            __array__ method returns an array, or any (nested) sequence.
            If object is a scalar, a 0-dimensional array containing object is
            returned.
        """

        data = np.array(data)
        self.window.extend(data)
        self.counter = 0

        if self.depth:
            means = back_mean(data, self.depth)
            data = data[-means.size + 1:] - means[:-1]

        self.n_samples = data.size

        if self.side in ["up", "both"]:
            self.init_threshold["up"] = np.nanquantile(data, self.init_quantile[1])
            self.peaks["up"] = data[data > self.init_threshold["up"]] - self.init_threshold["up"]
            gamma_up, sigma_up, _ = grimshaw(self.peaks["up"])
            self.extreme_quantile["up"] = self.quantile("up", gamma_up, sigma_up)

        if self.side in ["down", "both"]:
            self.init_threshold["down"] = np.nanquantile(data, self.init_quantile[0])
            self.peaks["down"] = self.init_threshold["down"] - data[data < self.init_threshold["down"]]
            gamma_down, sigma_down, _ = grimshaw(self.peaks["down"])
            self.extreme_quantile["down"] = self.quantile("down", gamma_down, sigma_down)

        if self.side == "up":
            self.init_threshold["down"] = -np.inf
            self.extreme_quantile["down"] = -np.inf

        if self.side == "down":
            self.init_threshold["up"] = np.inf
            self.extreme_quantile["up"] = np.inf

    def update_peak(self, peak, side):
        self.peaks[side] = np.append(self.peaks[side], peak)
        g, s, _ = grimshaw(self.peaks[side])
        self.extreme_quantile[side] = self.quantile(side, g, s)

    def update(self, obs):
        self.counter += 1
        if obs > self.extreme_quantile["up"] or obs < self.extreme_quantile["down"]:
            return True

        self.n_samples += 1
        if self.side in ["up", "both"] and obs > self.init_threshold["up"]:
            peak = obs - self.init_threshold["up"]
            self.update_peak(peak, "up")
        elif self.side in ["down", "both"] and obs < self.init_threshold["down"]:
            peak = -(obs - self.init_threshold["down"])
            self.update_peak(peak, "down")

        return False

    def run(self, data):
        raise NotImplementedError

    def predict(self, sequence: Sequence):
        length = len(sequence)
        data = np.array(sequence.values)
        if sequence.timestamps[-1] <= self.latest:
            return [False] * length

        new_length = min(length, math.ceil((sequence.timestamps[-1] - self.latest) / sequence.step))
        batch_data = data[-new_length:]
        self.history.extend(batch_data)
        self.latest = sequence.timestamps[-1]
        res = self.run(batch_data)

        if self.update_interval and self.counter >= self.update_interval:
            self.fit(self.history)

        return res

    def quantile(self, side, gamma, sigma):
        """
        Compute the quantile at level 1-q for a given side

        Parameters
        ----------
        side : str
            "up" or "down"
        gamma : float
            GPD parameter
        sigma : float
            GPD parameter
        Returns
        ----------
        float
            quantile at level 1-q for the GPD(γ,σ,μ=0)
        """

        if not self.peaks[side].size:
            return self.init_threshold[side]

        r = self.n_samples * self.probability / len(self.peaks[side])
        delta = (sigma / gamma) * (pow(r, -gamma) - 1) if gamma else -sigma * math.log(r)
        if side == "up":
            return self.init_threshold[side] + delta
        else:
            return self.init_threshold[side] - delta


class BiDSPOT(SPOT):
    def run(self, data):
        res = [False] * data.size
        for i in range(data.size):
            mean = np.nanmean(self.window)
            deviation = data[i] - mean
            if self.update(deviation):
                res[i] = True
            else:
                self.window.append(data[i])

        if len(self.history) < self.update_interval * 3:
            return [False] * data.size

        return res

    def simulate(self, data):
        upper_thresholds = []
        lower_thresholds = []
        res = [False] * data.size
        for i in range(data.size):
            mean = np.nanmean(self.window)
            deviation = data[i] - mean
            if self.update(deviation):
                res[i] = True
            else:
                self.window.append(data[i])

            if self.update_interval and self.counter >= self.update_interval:
                self.fit(data[max(i - self.update_interval, 0): i])

            if isinstance(upper_thresholds, list) and isinstance(lower_thresholds, list):
                upper_thresholds.append(self.extreme_quantile["up"] + mean)
                lower_thresholds.append(self.extreme_quantile["down"] + mean)

        return upper_thresholds, lower_thresholds, res


class ESPOT(SPOT):
    def run(self, data):
        res = [False] * data.size
        for i in range(data.size):
            mean = ewma(self.window)  # Dawnson in your area
            deviation = data[i] - mean
            if self.update(deviation):
                res[i] = True
            else:
                self.window.append(data[i])

        if len(self.history) < self.update_interval * 3:
            return [False] * data.size

        return res

    def simulate(self, data):
        upper_thresholds = []
        lower_thresholds = []
        res = [False] * data.size
        for i in range(data.size):
            mean = ewma(self.window)  # Dawnson in your area
            deviation = data[i] - mean
            if self.update(deviation):
                res[i] = True
            else:
                self.window.append(data[i])

            if self.update_interval and self.counter >= self.update_interval:
                self.fit(data[max(i - self.update_interval, 0): i])

            if isinstance(upper_thresholds, list) and isinstance(lower_thresholds, list):
                upper_thresholds.append(self.extreme_quantile["up"] + mean)
                lower_thresholds.append(self.extreme_quantile["down"] + mean)

        return upper_thresholds, lower_thresholds, res


class BiSPOT(SPOT):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.depth = 0

    def run(self, data):
        res = [False] * data.size
        for i in range(data.size):
            if self.update(data[i]):
                res[i] = True

        if len(self.history) < self.update_interval * 3:
            return [False] * data.size

        return res

    def simulate(self, data):
        upper_thresholds = []
        lower_thresholds = []
        res = [False] * data.size
        for i in range(data.size):
            if self.update(data[i]):
                res[i] = True

            if self.update_interval and self.counter >= self.update_interval:
                self.fit(data[max(i - self.update_interval, 0): i])

            if isinstance(upper_thresholds, list) and isinstance(lower_thresholds, list):
                upper_thresholds.append(self.extreme_quantile["up"])
                lower_thresholds.append(self.extreme_quantile["down"])

        return upper_thresholds, lower_thresholds, res
