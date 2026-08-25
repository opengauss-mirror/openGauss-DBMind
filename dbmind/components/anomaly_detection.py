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

import argparse
import os
import sys
from datetime import datetime

import numpy as np
from prettytable import PrettyTable
try:
    from scipy import interpolate
except ImportError:
    pass

from dbmind import constants
from dbmind.app import monitoring
from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.algorithm import anomaly_detection
from dbmind.common.exceptions import InvalidSequenceException
from dbmind.common.utils.exporter import set_logger
from dbmind.common.utils.component import initialize_tsdb_param
from dbmind.common.algorithm.stat_utils import sequence_interpolate
from dbmind.common.utils.checking import date_type, path_type, CheckAddress
from dbmind.common.utils.cli import (
    raise_fatal_and_exit, RED_FMT, GREEN_FMT
)
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils

DISTRIBUTION_LENGTH = 50
try:
    PLOT_WIDTH = os.get_terminal_size()[0] - 5
except OSError:
    PLOT_WIDTH = 100

PLOT_HEIGHT = PLOT_WIDTH // 4
INTERPOLATE_THRESHOLD = 0.9
STEP = 15000

ANOMALY_DETECTORS = {
    'level_shift': anomaly_detection.LevelShiftDetector(
        outliers=(
            monitoring.get_detection_param("level_shift_outliers_1"),
            monitoring.get_detection_param("level_shift_outliers_2"),
        ),
        side=monitoring.get_detection_param("level_shift_side"),
        window=monitoring.get_detection_param("level_shift_window")
    ),
    'seasonal': anomaly_detection.SeasonalDetector(
        outliers=(
            monitoring.get_detection_param("seasonal_outliers_1"),
            monitoring.get_detection_param("seasonal_outliers_2")
        ),
        side=monitoring.get_detection_param("seasonal_side"),
        window=monitoring.get_detection_param("seasonal_window")
    ),
    'spike': anomaly_detection.SpikeDetector(
        outliers=(
            monitoring.get_detection_param("spike_outliers_1"),
            monitoring.get_detection_param("spike_outliers_2")
        ),
        side=monitoring.get_detection_param("spike_side")
    ),
    'volatility_shift': anomaly_detection.VolatilityShiftDetector(
        outliers=(
            monitoring.get_detection_param("volatility_shift_outliers_1"),
            monitoring.get_detection_param("volatility_shift_outliers_2")
        ),
        side=monitoring.get_detection_param("volatility_shift_side"),
        window=monitoring.get_detection_param("volatility_shift_window")
    ),
}


def coloring(col, color_fmt):
    for i, c in enumerate(col):
        col[i] = color_fmt.format(c)


def transpose(col_row):
    n_row, n_col = len(col_row[0]), len(col_row)
    row_col = []
    for i in range(n_row):
        row = []
        for j in range(n_col):
            row.append(col_row[j][n_row - i - 1])
        row_col.append(row)
    return row_col


def index(y, y_min, y_max, height):
    if y_min == y_max:
        return height // 2

    idx = round((y - y_min) / (y_max - y_min) * height)
    idx = max(idx, 0)
    idx = min(idx, height - 1)
    return idx


def bash_plot(y, x=None, w=100, h=20, label=None, color_format=RED_FMT,
              marker='o', title=None, x_range=None):
    if label is None:
        label = []

    y_min, y_max = min(y), max(y)

    y = np.asarray(y)
    length = y.shape[0]
    if x is None:
        x = np.arange(1, length + 1)
    else:
        x = np.asarray(x)

    if x.ndim != 1 or y.ndim != 1:
        raise ValueError('x and y must be 1-D vector.')

    left_col, empty_col, right_col = ['|'] * h, [' '] * h, [' '] * h
    zero = 0 if y_min == y_max else index(0, y_min, y_max, h)
    left_col[zero], empty_col[zero], right_col[zero] = '+', '—', '>'
    title_line = '^' + title.center(w) if title else '^' + ' ' * w
    x_range_line = x_range.center(w + 1) if x_range else ' ' * (w + 1)

    step = (x[-1] - x[0]) / (w - 1)
    x_axis = np.arange(x[0], x[-1] + 0.5 * step, step)
    x_axis[-1] = min(x[-1], x_axis[-1])
    f = interpolate.interp1d(x, y, kind='zero')
    y_axis = f(x_axis)

    res = [left_col]
    fills = list()
    last_y_idx = None
    for i, value in enumerate(y_axis):
        y_idx = index(value, y_min, y_max, h)
        col = empty_col[:]
        col[y_idx] = marker
        if label and i in label:
            coloring(col, color_format)

        res.append(col)
        if last_y_idx is None or last_y_idx == y_idx:
            fills.append(None)
        else:
            step = int((last_y_idx - y_idx) / abs(last_y_idx - y_idx))
            fills.append([y_idx, last_y_idx, step])

        last_y_idx = y_idx

    for i in range(len(fills)):
        if not fills[i]:
            continue

        y_idx, last_y_idx, step = fills[i]
        for j in range(y_idx, last_y_idx, step):
            res[i][j] = marker

    res.append(right_col)

    plot_table = transpose(res)
    print(title_line)
    third_line = ['|'] + [' '] * w
    str_max = '(max: ' + str(y_max) + ')'
    third_line[2:len(str_max) + 2] = list(str_max)
    print(''.join(third_line))
    for i, row in enumerate(plot_table):
        print(''.join(row))

    str_min = '(min: ' + str(y_min) + ')'
    row = [' '] * (w + 1)
    row[2:len(str_min) + 2] = list(str_min)
    print(''.join(row))
    print(x_range_line)


def overview(anomalies_set, metric, start_time, end_time):
    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

    output_table = PrettyTable(title=f'{metric} {start_str} to {end_str}')
    output_table.field_names = (
        'host', 'anomaly',
        'anomaly_distribute (normal: ' + GREEN_FMT.format('-') + ', abnormal: ' + RED_FMT.format('*') + ')'
    )
    output_table.align = "l"

    distribution = [GREEN_FMT.format('-')] * DISTRIBUTION_LENGTH
    for host, anomalies in anomalies_set.items():
        for anomaly_type, seq in anomalies.items():
            anomaly_distribution = distribution[:]
            for i, ts in enumerate(seq.timestamps):
                if seq.values[i]:
                    idx = index(ts, seq.timestamps[0], seq.timestamps[-1], DISTRIBUTION_LENGTH)
                    anomaly_distribution[idx] = RED_FMT.format('*')

            output_table.add_row((host, anomaly_type, ''.join(anomaly_distribution)))

    output_table = output_table.get_string(sortby="host")
    print(output_table)


def plot(sequences_set, anomalies_set, metric, start_time, end_time):
    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

    table = {}
    for host, sequence in sequences_set.items():
        for anomaly_type, seq in anomalies_set[host].items():
            title = f'{anomaly_type} for {metric} from {host}'
            x_range = f'{start_str} to {end_str}'
            label = []
            for i, ts in enumerate(sequence.timestamps):
                if seq.values[i]:
                    idx = index(ts, sequence.timestamps[0], sequence.timestamps[-1], PLOT_WIDTH)
                    label.append(idx)
                    time_str = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    table[idx] = (time_str, sequence.values[i])

            bash_plot(y=sequence.values, x=sequence.timestamps, w=PLOT_WIDTH, h=PLOT_HEIGHT,
                      label=label, color_format=RED_FMT, marker='o', title=title, x_range=x_range)

    output_table = PrettyTable(title='Anomalies')
    output_table.field_names = ('time', 'value')
    output_table.align = "l"
    for r in table.values():
        output_table.add_row(r)
    print(output_table)


def main(argv):

    def anomaly_detect(sequence, anomaly_name):
        try:
            detector = ANOMALY_DETECTORS[anomaly_name]
            return detector.fit_predict(sequence)

        except Exception as e:
            raise_fatal_and_exit(str(e))

    parser = argparse.ArgumentParser(description="Workload Anomaly detection: "
                                                 "Anomaly detection of monitored metric.")
    parser.add_argument('--action', required=True, choices=('overview', 'plot'),
                        help='choose a functionality to perform')
    parser.add_argument('-c', '--conf', required=True, type=path_type,
                        help='set the directory of configuration files')
    parser.add_argument('-m', '--metric', required=True,
                        help='set the metric name you want to retrieve')
    parser.add_argument('-s', '--start-time', required=True, type=date_type,
                        help='set the start time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format')
    parser.add_argument('-e', '--end-time', required=True, type=date_type,
                        help='set the end time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format')
    parser.add_argument('-H', '--host', action=CheckAddress,
                        help='set a host of the metric, ip only or ip and port.')
    parser.add_argument('-a', '--anomaly', choices=("level_shift", "spike", "seasonal", "volatility_shift"),
                        help='set a anomaly detector of the metric from: '
                             '"level_shift", "spike", "seasonal", "volatility_shift"')
    args = parser.parse_args(argv)
    # Initialize
    os.chdir(args.conf)
    log_path = os.path.join('logs', constants.ANOMALY_DETECTION_LOG_NAME)
    set_logger(log_path, "info")
    init_global_configs(args.conf)
    if not initialize_tsdb_param():
        parser.exit(1, "TSDB service does not exist, exiting...\n")

    metric = args.metric
    start_time = args.start_time
    end_time = args.end_time
    host = args.host
    anomaly = args.anomaly
    if end_time - start_time < 30000:
        parser.exit(1, "The start time must be at least 30 seconds earlier than the end time.\n")
    elif end_time - start_time >= STEP * 11000:  # The Prometheus's length limit is up to 11000.
        parser.exit(1,
                    f"The time between the start time and the end time is too long. The maximum "
                    f"time window is {STEP * 11000 // 1000} seconds.\n")

    if args.action == 'plot' and None in (host, anomaly):
        parser.exit(1, "Quitting plot action due to missing parameters. "
                       "(--host and --anomaly)\n")

    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

    sequences = dai.get_metric_sequence(metric, start_datetime, end_datetime, step=STEP).fetchall()

    if not sequences:
        parser.exit(1, f"No data retrieved for {metric} from {start_str} to {end_str}.\n")

    if host:
        sequences = [sequence for sequence in sequences if SequenceUtils.from_server(sequence) == host]
        if not sequences:
            parser.exit(1, f"No data retrieved for {metric} from host: {host}. (If the metric {metric} "
                           " is a DB metric, check if you have enter the host with the port.)\n")

    anomalies_set = dict()
    sequences_set = dict()
    for sequence in sequences:
        metric_host = SequenceUtils.from_server(sequence)
        if len(sequence.values) >= (end_time - start_time) / sequence.step * INTERPOLATE_THRESHOLD:
            try:
                sequence = sequence_interpolate(sequence, fit_method="zero", strip_details=False)
            except InvalidSequenceException:
                continue

        elif len(sequence.values) <= 1:
            parser.exit(1, "The length of the scraped data from TSDB is too short.\n")

        if not all(np.isfinite(sequence.values)):
            parser.exit(1, "Non-numeric data format was found in sequence values.\n")

        sequences_set[metric_host] = sequence
        anomalies_set[metric_host] = {}
        if anomaly:
            if anomaly not in ANOMALY_DETECTORS:
                parser.exit(1, f"Not found anomaly in {list(ANOMALY_DETECTORS.keys())}.\n")
            anomalies_set[metric_host][anomaly] = anomaly_detect(sequence, anomaly)
        else:
            for anomaly_type in ANOMALY_DETECTORS:
                anomalies_set[metric_host][anomaly_type] = anomaly_detect(sequence, anomaly_type)

    if args.action == 'overview':
        overview(anomalies_set, metric, start_time, end_time)
    elif args.action == 'plot':
        plot(sequences_set, anomalies_set, metric, start_time, end_time)


if __name__ == '__main__':
    main(sys.argv[1:])
