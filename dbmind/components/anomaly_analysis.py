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
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime

from scipy import signal

from dbmind import constants
from dbmind import global_vars
from dbmind.cmd.config_utils import DynamicConfig, load_sys_configs
from dbmind.common import utils
from dbmind.common.algorithm.anomaly_detection import WAIT_EVENT_GRAPH
from dbmind.common.algorithm.correlation import max_cross_correlation
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.utils.checking import date_type, path_type
from dbmind.common.utils.cli import write_to_terminal
from dbmind.service import dai

LEAST_WINDOW = int(7.2e3) * 1000
LOOK_BACK = 0
LOOK_FORWARD = 0


def find_peaks(array, threshold=-float('inf')):
    peaks = signal.find_peaks(array)[0]
    return tuple(True if i in peaks and array[i] > threshold else False for i in range(len(array)))


def main(argv):
    parser = argparse.ArgumentParser(description="Workload Anomaly detection: "
                                                 "Anomaly detection of monitored metric.")
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
    parser.add_argument('-H', '--host', required=True,
                        help='set a host of the metric, ip only or ip and port.')
    parser.add_argument('--csv-dump-path', help='dump the result csv file to the dump path if it is specified.')

    args = parser.parse_args(argv)

    metric = args.metric
    start_time = args.start_time
    end_time = args.end_time
    host = args.host

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

    actual_start_time = min(start_time, end_time - LEAST_WINDOW)
    start_datetime = datetime.fromtimestamp(actual_start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)

    # Gathering wait events
    wait_events = dai.get_metric_sequence('pg_wait_event_spike', start_datetime, end_datetime).fetchall()
    wait_event_storage = defaultdict()
    for wait_event in wait_events:
        address = wait_event.labels.get('from_instance').strip()
        if address in host or host in address:
            event_name = wait_event.labels.get('event')
            wait_event_storage[event_name] = wait_event

    # Gathering other metrics
    other_metrics = defaultdict()
    for other_metric in WAIT_EVENT_GRAPH[metric].get('other_metrics'):
        seqs = dai.get_metric_sequence(other_metric, start_datetime, end_datetime).fetchall()
        for seq in seqs:
            address = seq.labels.get('from_instance').strip()
            if address in host or host in address:
                other_metric_name = other_metric
                if seq.labels.get('datname'):
                    other_metric_name += ' on ' + seq.labels.get('datname')
                elif seq.labels.get('device'):
                    other_metric_name += ' on ' + seq.labels.get('device')

                other_metric_name += ' from ' + address
                other_metrics[other_metric_name] = seq

    # Gathering anomalies of the metric
    target = dai.get_metric_sequence(metric, start_datetime, end_datetime).fetchall()
    for seq in target:
        address = seq.labels.get('from_instance').strip()
        if address in host or host in address:
            values = seq.values
            threshold = WAIT_EVENT_GRAPH[metric].get('threshold')
            anomaly = find_peaks(values, threshold)
            sequence = seq
            duration = list()
            t, flag = 0, 0
            for i, v in enumerate(anomaly):
                if v and not flag:
                    t = i
                    flag = 1
                elif not v and flag:
                    duration.append(t)
                    flag = 0

            break
    else:
        return

    # Find wait events
    n_reasons = 1
    res = defaultdict(list)
    res[metric] = [1, [(metric, 1, 0, sequence.values)]]
    for event_name, wait_event in wait_event_storage.items():
        if len(sequence) != len(wait_event):
            write_to_terminal(f"The length of {metric} and wait event {event_name} don't match.")
            continue

        x, y = sequence.values, wait_event.values
        corr, delay = max_cross_correlation(x, y, LOOK_BACK, LOOK_FORWARD)
        if abs(corr) > WAIT_EVENT_GRAPH[metric].get('correlation'):
            n_reasons += 1
            res[event_name] = [abs(corr), [(event_name, abs(corr), delay, wait_event.values)]]

    # Find other metrics
    for other_metric_name, other_metric_seq in other_metrics.items():
        if len(sequence) != len(other_metric_seq):
            write_to_terminal(f"The length of {metric} and {other_metric_name} don't match.")
            continue

        x, y = sequence.values, other_metric_seq.values
        corr, delay = max_cross_correlation(x, y, LOOK_BACK, LOOK_FORWARD)
        if abs(corr) > WAIT_EVENT_GRAPH[metric].get('correlation'):
            n_reasons += 1
            if ' on ' in other_metric_name:
                k = other_metric_name.split(' on ')[0]
            else:
                k = other_metric_name.split(' from ')[0]

            if k in res:
                res[k][0] = max(abs(corr), res[k][0])
                res[k][1].append((other_metric_name, abs(corr), delay, other_metric_seq.values))
            else:
                res[k] = [abs(corr), [(other_metric_name, abs(corr), delay, other_metric_seq.values)]]

    for k, (max_corr, res_list) in res.items():
        res[k] = [max_corr, sorted(res_list, key=lambda x: -x[1])]

    if 'gaussdb_locks' not in res and 'io_queue_number' not in res:
        write_to_terminal('The cpu high usage has nothing to do with the gaussdb.')
    else:
        if 'gaussdb_locks' in res:
            write_to_terminal(f"database: {res['gaussdb_locks'][1][0][0].split(' on ')[1]}")

        if 'io_queue_number' in res:
            write_to_terminal(f"device: {res['io_queue_number'][1][0][0].split(' on ')[1]}")

    if args.csv_dump_path:
        with open(args.csv_dump_path, 'w+') as f:
            writer = csv.writer(f)
            for k, v in res.items():
                for l in v[1]:
                    writer.writerow(l[0:3])
                    writer.writerow(l[3])


if __name__ == '__main__':
    main(sys.argv[1:])
