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
import http.client
import multiprocessing as mp
import os
import platform
import re
import sys
from collections import defaultdict
from datetime import datetime

from scipy.interpolate import interp1d

from dbmind import global_vars
from dbmind.common.algorithm.correlation import max_cross_correlation
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.utils.checking import (
    check_ip_valid, check_port_valid, date_type, path_type
)
from dbmind.common.utils.cli import write_to_terminal
from dbmind.cmd.edbmind import init_global_configs, init_tsdb_with_config
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils
from dbmind.constants import DISTINGUISHING_INSTANCE_LABEL


LEAST_WINDOW = int(7.2e3) * 1000
LOOK_BACK = 0
LOOK_FORWARD = 0

# The "chunk" is used in package--urllib3 which is only supported in HTTP/1.1 or later.
# It will cause ChunkedEncodingError if the server only supports the request of HTTP/1.0.
# To avoid the ChunkedEncodingError, fix the http connection to HTTP/1.0.
http.client.HTTPConnection._http_vsn = 10
http.client.HTTPConnection._http_vsn_str = "HTTP/1.0"


def get_sequences(arg):
    metric, instance, start_datetime, end_datetime = arg
    result = []
    seqs = []
    if global_vars.configs.get('TSDB', 'name') == "prometheus":
        if ":" in instance and check_ip_valid(instance.split(":")[0]) and check_port_valid(instance.split(":")[1]):
            seqs = dai.get_metric_sequence(metric, start_datetime, end_datetime).from_server(instance).fetchall()
            if not seqs:
                host = instance.split(":")[0]
                seqs = dai.get_metric_sequence(metric, start_datetime, end_datetime).from_server(host).fetchall()
        elif check_ip_valid(instance):
            instance_like = {DISTINGUISHING_INSTANCE_LABEL: instance + "(:[0-9]{4,5}|)"}
            seqs = dai.get_metric_sequence(metric, start_datetime, end_datetime).filter_like(**instance_like).fetchall()
        else:
            raise ValueError(f"Invalid instance: {instance}.")
    else:
        raise

    step = dai.get_metric_sequence(metric, start_datetime, end_datetime).step / 1000
    start_time = datetime.timestamp(start_datetime)
    end_time = datetime.timestamp(end_datetime)
    length = (end_time - start_time) // step
    for seq in seqs:
        if DISTINGUISHING_INSTANCE_LABEL not in seq.labels or len(seq) < 0.6 * length:
            continue

        from_instance = SequenceUtils.from_server(seq).strip()
        if seq.labels.get('event'):
            name = 'wait event-' + seq.labels.get('event')
        else:
            name = metric
            if seq.labels.get('datname'):
                name += ' on ' + seq.labels.get('datname')
            elif seq.labels.get('device'):
                name += ' on ' + seq.labels.get('device')
            name += ' from ' + from_instance

        result.append((name, seq))

    return result


def get_correlations(arg):
    name, sequence, this_sequence = arg
    f = interp1d(
        sequence.timestamps,
        sequence.values,
        kind='linear',
        bounds_error=False,
        fill_value=(sequence.values[0], sequence.values[-1])
    )
    y = f(this_sequence.timestamps)
    corr, delay = max_cross_correlation(this_sequence.values, y, LOOK_BACK, LOOK_FORWARD)
    return name, corr, delay, sequence.values, sequence.timestamps


def multi_process_correlation_calculation(metric, sequence_args, corr_threshold=0, topk=100):
    with mp.Pool() as pool:
        sequence_result = pool.map(get_sequences, iterable=sequence_args)

        _, host, start_datetime, end_datetime = sequence_args[0]
        these_sequences = get_sequences((metric, host, start_datetime, end_datetime))

        if not these_sequences:
            write_to_terminal('The metric was not found.')
            return

        correlation_result = dict()
        for this_name, this_sequence in these_sequences:
            correlation_args = list()
            for sequences in sequence_result:
                for name, sequence in sequences:
                    correlation_args.append((name, sequence, this_sequence))
            correlation_result[this_name] = pool.map(get_correlations, iterable=correlation_args)
   
    pool.join()

    for this_name, this_sequence in correlation_result.items():
        this_sequence.sort(key=lambda item: item[1], reverse=True)
        del(this_sequence[topk:])
        
    return correlation_result


def single_process_correlation_calculation(metric, sequence_args, corr_threshold=0, topk=100):
    sequence_result = list()
    these_sequences = list()
    for sequence_arg in sequence_args:
        for name, sequence in get_sequences(sequence_arg):
            if name.startswith(f"{metric} "):
                these_sequences.append((name, sequence))
            sequence_result.append((name, sequence))

    if not these_sequences:
        write_to_terminal('The metric was not found.')
        return

    correlation_result = defaultdict(list)
    for this_name, this_sequence in these_sequences:
        for name, sequence in sequence_result:
            name, corr, delay, values, timestamps = get_correlations((name, sequence, this_sequence))
            if abs(corr) >= corr_threshold:
                correlation_result[this_name].append((name, corr, delay, values, timestamps))
        correlation_result[this_name].sort(key=lambda item: item[1], reverse=True)
        correlation_result[this_name] = correlation_result[this_name][:topk]
    return correlation_result


def main(argv):
    parser = argparse.ArgumentParser(description="Workload Anomaly analysis: "
                                                 "Anomaly analysis of monitored metric.")
    parser.add_argument('-c', '--conf', required=True, type=path_type,
                        help='set the directory of configuration files.')
    parser.add_argument('-m', '--metric', required=True,
                        help='set the metric name you want to retrieve.')
    parser.add_argument('-s', '--start-time', required=True, type=date_type,
                        help='set the start time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format.')
    parser.add_argument('-e', '--end-time', required=True, type=date_type,
                        help='set the end time of for retrieving in ms, '
                             'supporting UNIX-timestamp with microsecond or datetime format.')
    parser.add_argument('-H', '--host', required=True,
                        help='set a host of the metric, ip only or ip and port.')
    parser.add_argument('--csv-dump-path', help='dump the result csv file to the dump path if it is specified.')

    args = parser.parse_args(argv)

    metric = args.metric
    start_time = args.start_time
    end_time = args.end_time
    host = args.host

    os.chdir(args.conf)
    init_global_configs(args.conf)
    init_tsdb_with_config()
  
    client = TsdbClientFactory.get_tsdb_client()
    all_metrics = client.all_metrics
    actual_start_time = min(start_time, end_time - LEAST_WINDOW)
    start_datetime = datetime.fromtimestamp(actual_start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)

    sequence_args = [(metric_name, host, start_datetime, end_datetime) for metric_name in all_metrics]

    if platform.system() != 'Windows':
        correlation_result = multi_process_correlation_calculation(metric, sequence_args)
    else:
        correlation_result = single_process_correlation_calculation(metric, sequence_args)

    result = dict()
    for this_name in correlation_result:
        this_result = defaultdict(tuple)
        for name, corr, delay, values, timestamps in correlation_result[this_name]:
            this_result[name] = max(this_result[name], (abs(corr), name, corr, delay, values, timestamps))
        result[this_name] = this_result

    if args.csv_dump_path:
        for this_name in result:
            new_name = re.sub(r'[\\/:*?"<>|]', '_', this_name)
            csv_path = os.path.join(args.csv_dump_path, new_name + ".csv")
            with open(csv_path, 'w+', newline='') as f:
                writer = csv.writer(f)
                for _, name, corr, delay, values, timestamps in sorted(result[this_name].values(), key=lambda t: (t[3], -t[0])):
                    writer.writerow((name, corr, delay) + values)  # Discard the first element abs(corr) after sorting.


if __name__ == '__main__':
    main(sys.argv[1:])
