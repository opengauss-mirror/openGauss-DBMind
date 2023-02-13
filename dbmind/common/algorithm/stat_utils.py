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
from scipy.interpolate import interp1d

from dbmind.common.types import Sequence


def double_padding(values, window):
    """remove the side effect"""
    window = 1 if len(values) < window else window
    left_idx = window - 1 - (window - 1) // 2
    right_idx = len(values) - 1 - (window - 1) // 2
    values[:left_idx] = values[left_idx]  # padding left
    values[right_idx + 1:] = values[right_idx]  # padding right
    return values


def np_shift(values, shift_distance=1, fill_value=np.nan):
    """shift values a shift_distance"""
    if len(values) < 2:
        return values
    shifted_values = np.roll(values, shift_distance).astype('float')
    for i in range(shift_distance):
        shifted_values[i] = fill_value
    return shifted_values


def np_nanstd(values):
    if len(values) == 1:
        return 0.0
    else:
        return np.nanstd(values, ddof=1)


def np_rolling(values, window=1, trim=False, agg='median'):
    """Transformer that rolls a sliding window along a time series, and
    aggregates using a user-selected operation.
    """
    funcs = {
        'median': np.nanmedian,
        'mean': np.nanmean,
        'std': np_nanstd
    }
    func = funcs[agg]
    sequence_length = len(values)
    res = np.zeros(sequence_length)
    left_idx = window - 1 - (window - 1) // 2
    for i in range(sequence_length):
        segment = values[max(0, i - left_idx):i + window - left_idx]
        res[i] = func(segment)
    if trim:
        res = double_padding(res, window)
    return res


def np_double_rolling(values, window=(1, 1), diff_mode="diff", agg='median', trim=True):
    values_length = len(values)
    window1 = 1 if values_length < window[0] else window[0]
    window2 = 1 if values_length < window[1] else window[1]

    left_rolling = np_rolling(np_shift(values, 1), window=window1, agg=agg)
    # Polish later: this `values[::-1]` can be replaced by reverse scan.
    right_rolling = np_rolling(values[::-1], window=window2, agg=agg)[::-1]
    r_data = right_rolling - left_rolling

    functions = {
        'abs': lambda x: np.abs(x),
        'rel': lambda x: x / left_rolling
    }
    methods = diff_mode.split('_')[:-1]
    for method in methods:
        r_data = functions[method](r_data)
    if trim:
        r_data = double_padding(r_data, max(window1, window2))
    return r_data


def measure_head_and_tail_nan(data):
    data_not_nan = -1 * np.isnan(data)
    left = data_not_nan.argmax()
    right = data_not_nan[::-1].argmax()
    return left, right


def trim_head_and_tail_nan(data):
    """
    when there are nan value at head or tail of forecast_data,
    this function will fill value with near value
    :param data: type->np.array or list
    :return data: type->same type as the input 'data'
    """
    length = len(data)
    if length == 0:
        return data

    data_not_nan = np.isnan(data)
    if data_not_nan.all():
        data[:] = [0] * length
        return data

    left, right = measure_head_and_tail_nan(data)

    data[:left] = [data[left]] * left
    data[length - right:] = [data[length - right - 1]] * right
    return data


def tidy_up_sequence(sequence):
    """Fill up missing values for sequence and
    align sequence's timestamps.
    """
    if sequence.step <= 0:
        return sequence

    def estimate_error(a, b):
        return (a - b) / b

    timestamps = list(sequence.timestamps)
    values = list(sequence.values)

    i = 1
    while i < len(timestamps):
        real_interval = timestamps[i] - timestamps[i - 1]
        error = estimate_error(real_interval, sequence.step)
        if error < 0:
            # This is because the current timestamp is lesser than the previous one.
            # We should remove one to keep monotonic.
            if not np.isfinite(values[i - 1]):
                values[i - 1] = values[i]
            timestamps.pop(i)
            values.pop(i)
            i -= 1  # We have removed an element so we have to decrease the cursor.
        elif error == 0:
            """Everything is normal, skipping."""
        elif 0 < error < 1:
            # Align the current timestamp.
            timestamps[i] = timestamps[i - 1] + sequence.step
        else:
            # Fill up missing value with NaN.
            next_ = timestamps[i - 1] + sequence.step
            timestamps.insert(i, next_)
            values.insert(i, float('nan'))
        i += 1

    return Sequence(timestamps, values)


def sequence_interpolate(sequence: Sequence, fit_method="linear", strip_details=True):
    """interpolate with scipy interp1d"""
    filled_sequence = tidy_up_sequence(sequence)
    has_defined = np.isfinite(filled_sequence.values)

    if all(has_defined):
        if strip_details:
            return filled_sequence
        else:
            return Sequence(
                timestamps=filled_sequence.timestamps,
                values=filled_sequence.values,
                name=sequence.name,
                step=sequence.step,
                labels=sequence.labels
            )

    if not any(has_defined):
        raise ValueError("All of sequence values are undefined.")

    y_raw = np.array(filled_sequence.values)
    x_raw = np.arange(len(y_raw))
    x_nona, y_nona = x_raw[has_defined], y_raw[has_defined]

    fit_func = interp1d(x_nona, y_nona, kind=fit_method, bounds_error=False,
                        fill_value=(y_nona[0], y_nona[-1]))
    y_new = fit_func(x_raw)

    if strip_details:
        return Sequence(timestamps=filled_sequence.timestamps, values=y_new)
    else:
        return Sequence(
            timestamps=filled_sequence.timestamps,
            values=y_new,
            name=sequence.name,
            step=sequence.step,
            labels=sequence.labels
        )
