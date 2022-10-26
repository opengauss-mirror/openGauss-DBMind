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


class MinMaxScaler:
    def __init__(self, feature_range=(0, 1)):
        self.feature_range = feature_range

    def _reset(self):
        # Checking one attribute is enough, because they are all set together
        # in partial_fit
        if hasattr(self, "scale_"):
            del self.scale_
            del self.min_
            del self.data_min_
            del self.data_max_
            del self.data_range_

    def fit(self, x):
        # Reset internal state before fitting
        self._reset()
        return self.partial_fit(x)

    def partial_fit(self, x):
        feature_range = self.feature_range
        if feature_range[0] >= feature_range[1]:
            raise ValueError(
                "Minimum of desired feature range must be smaller than maximum. Got %s."
                % str(feature_range)
            )

        x = check_array(x, copy=True, dtype=np.float64)
        data_min = np.nanmin(x, axis=0)
        data_max = np.nanmax(x, axis=0)
        if hasattr(self, "data_min_"):
            data_min = np.minimum(self.data_min_, data_min)
        if hasattr(self, "data_max_"):
            data_max = np.maximum(self.data_max_, data_max)

        data_range = data_max - data_min
        self.scale_ = (feature_range[1] - feature_range[0]) / _handle_zeros_in_scale(
            data_range, copy=True
        )
        self.min_ = feature_range[0] - data_min * self.scale_
        self.data_min_ = data_min
        self.data_max_ = data_max
        self.data_range_ = data_range
        return self

    def transform(self, x):
        self.check_is_fitted()
        x = check_array(x, copy=True, dtype=np.float64)
        x *= self.scale_
        x += self.min_
        return x

    def inverse_transform(self, x):
        self.check_is_fitted()
        x = check_array(x, copy=True, dtype=np.float64)
        x -= self.min_
        x /= self.scale_
        return x

    def fit_transform(self, x):
        self.fit(x)
        return self.transform(x)

    def check_is_fitted(self):
        for v in vars(self):
            if v.endswith("_") and not v.startswith("__"):
                return

        raise ValueError("This MinMaxScaler instance is not fitted yet. Call 'fit' "
                         "with appropriate arguments before using this MinMaxScaler.")


def _handle_zeros_in_scale(scale, copy=True, constant_mask=None):
    """Set scales of near constant features to 1.

    The goal is to avoid division by very small or zero values.

    Near constant features are detected automatically by identifying
    scales close to machine precision unless they are precomputed by
    the caller and passed with the `constant_mask` kwarg.

    Typically for standard scaling, the scales are the standard
    deviation while near constant features are better detected on the
    computed variances which are closer to machine precision by
    construction.
    """
    # if we are fitting on 1D arrays, scale might be a scalar
    if np.isscalar(scale):
        if scale == 0.0:
            scale = 1.0
        return scale
    elif isinstance(scale, np.ndarray):
        if constant_mask is None:
            # Detect near constant values to avoid dividing by a very small
            # value that could lead to surprising results and numerical
            # stability issues.
            constant_mask = scale < 10 * np.finfo(scale.dtype).eps

        if copy:
            # New array to avoid side-effects
            scale = scale.copy()
        scale[constant_mask] = 1.0
        return scale


def check_array(array, dtype, copy=False):
    if np.isinf(array).any():
        raise ValueError("Input contains Infinite value.")
    if copy:
        return np.array(array, dtype=dtype)
    else:
        return array.astype(dtype)
