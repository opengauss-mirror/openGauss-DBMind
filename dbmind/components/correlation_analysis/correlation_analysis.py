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

import argparse
import os
import sys
from datetime import datetime
from collections import defaultdict, OrderedDict
from functools import partial

import numpy as np
from prettytable import PrettyTable

from dbmind import constants
from dbmind import global_vars
from dbmind.cmd.config_utils import DynamicConfig, load_sys_configs
from dbmind.common import utils
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.utils.checking import date_type, path_type
from dbmind.service import dai
from dbmind.service.utils import DISTINGUISHING_INSTANCE_LABEL
from dbmind.common.algorithm.correlation import amplify_feature, max_cross_correlation


def seq_outer_join(sequences, empty=np.nan):
    time_stamps = set()
    for sequence in sequences:
        time_stamps |= set(sequence.timestamps)

    time_stamps = sorted(list(time_stamps))
    res = [{ts: empty for ts in time_stamps} for _ in sequences]
    for i, sequence in enumerate(sequences):
        for j, ts in enumerate(sequence.timestamps):
            res[i][ts] = sequence.values[j]

    return np.array([list(seq.values()) for seq in res]), np.array(time_stamps)


def main(argv):
    metric_choices = ['os_cpu_usage', 'os_mem_usage', 'gaussdb_qps_by_instance']
    parser = argparse.ArgumentParser(description="Correlation analysis among multiple indicators.")
    parser.add_argument('-c', '--conf', required=True, type=path_type,
                        help='set the directory of configuration files')
    parser.add_argument('-m', '--primary-metric', choices=metric_choices, required=True,
                        help='set the metric name you want to retrieve')
    parser.add_argument('--metrics', default=['pg_wait_event_spike'] + metric_choices,
                        type=partial(str.split, sep=','),
                        help='Specify multiple indicators as input for correlation analysis.')
    parser.add_argument('--host', required=True,
                        help='set a host of the metric, ip only or ip and port.')
    parser.add_argument('-s', '--start-time', required=True, type=date_type,
                        help='set the start time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format')
    parser.add_argument('-e', '--end-time', required=True, type=date_type,
                        help='set the end time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format')
    args = parser.parse_args(argv)

    # Initialize
    os.chdir(args.conf)
    global_vars.metric_map = utils.read_simple_config_file(constants.METRIC_MAP_CONFIG)
    global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
    global_vars.dynamic_configs = DynamicConfig

    TsdbClientFactory.set_client_info(
        global_vars.configs.get('TSDB', 'name'),
        global_vars.configs.get('TSDB', 'host'),
        global_vars.configs.get('TSDB', 'port'),
        global_vars.configs.get('TSDB', 'username'),
        global_vars.configs.get('TSDB', 'password'),
        global_vars.configs.get('TSDB', 'ssl_certfile'),
        global_vars.configs.get('TSDB', 'ssl_keyfile'),
        global_vars.configs.get('TSDB', 'ssl_keyfile_password'),
        global_vars.configs.get('TSDB', 'ssl_ca_file')
    )

    # Parameters
    primary_metric = args.primary_metric
    metrics = args.metrics
    start_time = args.start_time
    end_time = args.end_time
    host = args.host

    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    wait_events = defaultdict(OrderedDict)
    # Gathering metric data
    metric_sequences = dai.get_metric_sequence(primary_metric, start_datetime, end_datetime).fetchall()
    for metric_sequence in metric_sequences:
        address = metric_sequence.labels.get(DISTINGUISHING_INSTANCE_LABEL)
        host = address.split(':')[0]
        wait_events[host][primary_metric] = metric_sequence

    # Gathering wait events
    for metric in metrics:
        events = dai.get_metric_sequence(metric, start_datetime, end_datetime).fetchall()
        for event in events:
            address = event.labels.get(DISTINGUISHING_INSTANCE_LABEL)
            if ':' in address:
                host, port = address.split(':')
            else:
                host = address
                port = None
            if event.labels.get('event'):
                event_name = event.labels.get('event')
                if port:
                    event_name += ', port: ' + port
            else:
                event_name = metric
            wait_events[host][event_name] = event

    merged_events, timestamps = seq_outer_join(wait_events[host].values(), empty=0)

    # Filter empty indicator.
    event_names = np.array(list(wait_events[host].keys()))[merged_events.any(axis=1)]
    merged_events = merged_events[merged_events.any(axis=1), :]

    # feature amplification and correlation calculation
    amplified_merged_events = np.apply_along_axis(amplify_feature, axis=1, arr=merged_events)
    correlation_func = partial(max_cross_correlation, amplified_merged_events[0])
    correlations = np.apply_along_axis(correlation_func, axis=1, arr=amplified_merged_events[1:])

    # Get the top5 according to the correlation coefficient, and output in chronological order.
    event_res = []
    for i, event_name in enumerate(event_names[1:]):
        event_res.append([event_name, correlations[i]])
    correlation_coefficient_idx = 0
    shift_idx = 1
    event_res.sort(key=lambda x: -abs(x[1][correlation_coefficient_idx]))
    top5 = sorted(event_res[:5], key=lambda x: -x[1][shift_idx])

    output_table = PrettyTable()
    output_table.field_names = ['KPI', 'Cross Correlation Coefficient', 'Temporal Shift']
    output_table.align = "l"
    for res in top5:
        output_table.add_row([res[0], res[1][correlation_coefficient_idx], -res[1][shift_idx]])
    print(output_table)


if __name__ == '__main__':
    main(sys.argv[1:])
