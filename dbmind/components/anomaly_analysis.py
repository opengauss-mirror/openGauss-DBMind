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
import multiprocessing as mp
import os
import sys
from collections import defaultdict
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from scipy.interpolate import interp1d

from dbmind import constants
from dbmind import global_vars
from dbmind.cmd.config_utils import DynamicConfig, load_sys_configs
from dbmind.common import utils
from dbmind.common.algorithm.correlation import max_cross_correlation
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.utils.checking import date_type, path_type
from dbmind.common.utils.cli import write_to_terminal
from dbmind.service import dai

LEAST_WINDOW = int(7.2e3) * 1000
INTERVAL = 15000
LOOK_BACK = 0
LOOK_FORWARD = 0


def get_sequences(arg):
    metric, host, start_datetime, end_datetime, length = arg
    result = []
    seqs = dai.get_metric_sequence(metric, start_datetime, end_datetime).fetchall()
    for seq in seqs:
        if 'from_instance' not in seq.labels or len(seq) < 0.9 * length:
            continue

        address = seq.labels.get('from_instance').strip()
        if address in host or host in address:
            if seq.labels.get('event'):
                name = 'wait event-' + seq.labels.get('event')
            else:
                name = metric
                if seq.labels.get('datname'):
                    name += ' on ' + seq.labels.get('datname')
                elif seq.labels.get('device'):
                    name += ' on ' + seq.labels.get('device')
                name += ' from ' + address

            result.append((name, seq))

    return result


def get_correlations(arg):
    name, sequence, the_sequence = arg
    f = interp1d(
        sequence.timestamps,
        sequence.values,
        kind='linear',
        bounds_error=False,
        fill_value=(sequence.values[0], sequence.values[-1])
    )
    y = f(the_sequence.timestamps)
    corr, delay = max_cross_correlation(the_sequence.values, y, LOOK_BACK, LOOK_FORWARD)
    return name, corr, delay, sequence.values


def main(argv):
    parser = argparse.ArgumentParser(description="Workload Anomaly analysis: "
                                                 "Anomaly analysis of monitored metric.")
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

    url = (
        'http://' + global_vars.configs.get('TSDB', 'host') +
        ':' + global_vars.configs.get('TSDB', 'port') +
        '/api/v1/label/__name__/values'
    )
    s = requests.Session()  # suitable for both http and https
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))
    response = s.get(url, headers={"Content-Type": "application/json"}, verify=False, timeout=5)
    other_metrics = eval(response.content.decode())['data']

    actual_start_time = min(start_time, end_time - LEAST_WINDOW)
    start_datetime = datetime.fromtimestamp(actual_start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    length = (end_time - start_time) // INTERVAL

    sequence_args = [(other_metric, host, start_datetime, end_datetime, length) for other_metric in other_metrics]
    pool = mp.Pool()
    ans = pool.map(get_sequences, iterable=sequence_args)
    pool.close()
    pool.join()

    the_sequence = None
    for sequences in ans:
        for name, sequence in sequences:
            if metric in name:
                the_sequence = sequence
                break
        if the_sequence:
            break
    else:
        write_to_terminal('The metric not found.')
        return

    correlation_args = list()
    for sequences in ans:
        for name, sequence in sequences:
            correlation_args.append((name, sequence, the_sequence))

    pool = mp.Pool()
    ans = pool.map(get_correlations, iterable=correlation_args)
    pool.close()
    pool.join()

    res = defaultdict(tuple)
    for name, corr, delay, values in ans:
        res[name] = max(res[name], (abs(corr), name, corr, delay, values))

    res[metric] = (1, metric, 1, 0, the_sequence.values)

    if args.csv_dump_path:
        with open(args.csv_dump_path, 'w+') as f:
            writer = csv.writer(f)
            for v in sorted(res.values(), reverse=True):
                writer.writerow(v[0:3])
                writer.writerow(v[3])


if __name__ == '__main__':
    main(sys.argv[1:])
