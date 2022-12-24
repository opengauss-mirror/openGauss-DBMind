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
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from math import inf

from prettytable import PrettyTable

from dbmind.cmd.edbmind import init_tsdb_with_config, init_global_configs
from dbmind.common.algorithm.forecasting import quickly_forecast
from dbmind.common.utils import write_to_terminal
from dbmind.common.utils.checking import path_type, date_type
from dbmind.common.utils.cli import keep_inputting_until_correct
from dbmind.common.utils.exporter import KVPairAction
from dbmind.metadatabase.dao import forecasting_metrics
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils


def _get_sequences(metric, host, labels, start_datetime, end_datetime):
    result = []
    if labels is None:
        sequences = dai.get_metric_sequence(metric, start_datetime, end_datetime). \
            from_server(host).fetchall()
    else:
        sequences = dai.get_metric_sequence(metric, start_datetime, end_datetime). \
            from_server(host).filter(**labels).fetchall()
    for sequence in sequences:
        name = metric
        address = SequenceUtils.from_server(sequence).strip()
        if labels is None:
            name += str(sequence.labels)
        else:
            name += str(labels)
        name += ' from ' + address
        result.append((name, sequence))
    return result


def _initialize_tsdb_param():
    try:
        tsdb = init_tsdb_with_config()
        return tsdb.check_connection()
    except Exception as e:
        logging.error(e)
        return False


def _save_forecast_result(result, save_path):
    dirname = os.path.dirname(save_path)
    os.makedirs(dirname, exist_ok=True)
    with open(save_path, mode='w+', newline='') as fp:
        writer = csv.writer(fp)
        for name, value, timestamp in result:
            writer.writerow([name])
            writer.writerow([time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(int(item / 1000))) for item in timestamp]
            )
            writer.writerow(value)

    os.chmod(save_path, 0o600)


def show(metric, host, start_time, end_time):
    field_names = (
        'rowid', 'metric_name',
        'host', 'metric_time',
        'metric_value'
    )
    output_table = PrettyTable()
    output_table.field_names = field_names

    result = forecasting_metrics.select_forecasting_metric(
        metric_name=metric, instance=host,
        min_metric_time=start_time, max_metric_time=end_time
    ).all()
    for row_ in result:
        row = [getattr(row_, field) for field in field_names]
        output_table.add_row(row)

    nb_rows = len(result)
    if nb_rows > 50:
        write_to_terminal('The number of rows is greater than 50. '
                          'It seems too long to see.')
        char = keep_inputting_until_correct('Do you want to dump to a file? [Y]es, [N]o.', ('Y', 'N'))
        if char == 'Y':
            dump_file_name = 'metric_forecast_%s.csv' % int(time.time())
            with open(dump_file_name, 'w+') as fp:
                csv_writer = csv.writer(fp)
                for row_ in result:
                    row = [str(getattr(row_, field)).strip() for field in field_names]
                    csv_writer.writerow(row)
            write_to_terminal('Dumped file is %s.' % os.path.realpath(dump_file_name))
        elif char == 'N':
            print(output_table)
            print('(%d rows)' % nb_rows)
    else:
        print(output_table)
        print('(%d rows)' % nb_rows)


def clean(retention_days):
    if retention_days is None:
        forecasting_metrics.truncate_forecasting_metrics()
    else:
        start_time = int((time.time() - float(retention_days) * 24 * 60 * 60) * 1000)
        forecasting_metrics.delete_timeout_forecasting_metrics(start_time)
    write_to_terminal('Success to delete redundant results.')


def early_warning(metric, host, start_time, end_time, warning_hours, labels=None,
                  upper=None, lower=None, save_path=None):
    output_table = PrettyTable()
    output_table.field_names = ('name', 'warning information')
    output_table.align = "l"
    if not _initialize_tsdb_param():
        logging.error("TSDB initialization failed.")
        return
    upper = inf if upper is None else upper
    lower = -inf if lower is None else lower
    warning_minutes = warning_hours * 60  # convert hours to minutes
    if end_time is None:
        end_time = int(time.time() * 1000)
    if start_time is None:
        # The default historical sequence is 3 times the length of the predicted sequence
        start_time = end_time - warning_minutes * 60 * 1000 * 3
    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    sequences = _get_sequences(metric, host, labels, start_datetime, end_datetime)
    rows = []
    summary_sequence = []
    for name, sequence in sequences:
        if sequence.values[-1] >= upper:
            warning_information = "metric has exceeded the warning value."
            rows.append((name, warning_information))
            continue
        if sequence.values[-1] <= lower:
            warning_information = "metric has been less than the warning value."
            rows.append((name, warning_information))
            continue
        forecast_result = quickly_forecast(sequence, warning_minutes)
        if save_path is not None:
            summary_sequence.append((name, sequence.values + forecast_result.values,
                                     sequence.timestamps + forecast_result.timestamps))
        if not forecast_result.values:
            continue
        lower_flag, upper_flag = False, False
        for val, timestamp in zip(forecast_result.values, forecast_result.timestamps):
            if lower_flag and upper_flag:
                break
            if not upper_flag and val >= upper:
                current_timestamp = int(time.time() * 1000)
                remaining_hours = round((timestamp - current_timestamp) / 1000 / 60 / 24, 4)
                string_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp / 1000)))
                warning_information = "exceed the warning value %s at %s(remaining %s hours)." \
                                      % (upper, string_time, remaining_hours)
                rows.append((name, warning_information))
                upper_flag = True
            if not lower_flag and val <= lower:
                current_timestamp = int(time.time() * 1000)
                remaining_hours = round((timestamp - current_timestamp) / 1000 / 60 / 24, 4)
                string_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp / 1000)))
                warning_information = "lower than the warning value %s at %s(remaining %s hours)." \
                                      % (upper, string_time, remaining_hours)
                rows.append((name, warning_information))
                lower_flag = True
    if not rows:
        rows.append((metric, 'No warning found.'))
    output_table.add_rows(rows)
    print(output_table)
    if save_path is not None:
        _save_forecast_result(summary_sequence, save_path=save_path)


def main(argv):
    parser = argparse.ArgumentParser(description='Workload Forecasting: Forecast monitoring metrics')
    parser.add_argument('action', choices=('show', 'clean', 'early-warning'), help='Choose a functionality to perform')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=path_type,
                        help='Set the directory of configuration files')
    parser.add_argument('--metric-name', metavar='METRIC_NAME',
                        help='Set a metric name you want to retrieve')
    parser.add_argument('--host', metavar='HOST',
                        help="Set a host you want to retrieve. IP only or IP with port.")
    parser.add_argument('--labels', metavar='LABELS', action=KVPairAction,
                        help='A list of label (format is label=name) separated by comma(,). '
                             'Using in warning.')
    parser.add_argument('--start-time', metavar='TIMESTAMP_IN_MICROSECONDS',
                        type=date_type, help='Set a start time for retrieving, '
                                             'supporting UNIX-timestamp with microsecond or datetime format')
    parser.add_argument('--end-time', metavar='TIMESTAMP_IN_MICROSECONDS',
                        type=date_type, help='Set an end time for retrieving, '
                                             'supporting UNIX-timestamp with microsecond or datetime format')
    parser.add_argument('--retention-days', metavar='DAYS', type=float,
                        help='Clear historical diagnosis results and set '
                             'the maximum number of days to retain data')
    parser.add_argument('--upper', metavar='UPPER', type=float,
                        help='The upper value of early-warning. Using in warning.')
    parser.add_argument('--lower', metavar='LOWER', type=float,
                        help='The lower value of early-warning. Using in warning.')
    parser.add_argument('--warning-hours', metavar='WARNING-HOURS', type=int, help='warning length, unit is hour.')
    parser.add_argument('--csv-dump-path', type=os.path.realpath,
                        help='Dump the result CSV file to the path if it is specified. Use in warning.')

    args = parser.parse_args(argv)

    if not os.path.exists(args.conf):
        parser.exit(1, 'Not found the directory %s.' % args.conf)

    if args.action == 'show':
        if None in (args.metric_name, args.host, args.start_time, args.end_time):
            write_to_terminal('There may be a lot of results because you did not use all filter conditions.',
                              color='red')
            inputted_char = keep_inputting_until_correct('Press [A] to agree, press [Q] to quit:', ('A', 'Q'))
            if inputted_char == 'Q':
                parser.exit(0, "Quitting due to user's instruction.")
    elif args.action == 'clean':
        if args.retention_days is None:
            write_to_terminal('You did not specify retention days, so we will delete all historical results.',
                              color='red')
            inputted_char = keep_inputting_until_correct('Press [A] to agree, press [Q] to quit:', ('A', 'Q'))
            if inputted_char == 'Q':
                parser.exit(0, "Quitting due to user's instruction.")
    elif args.action == 'early-warning':
        if args.upper is None and args.lower is None:
            parser.exit(1, 'You did not specify the upper or lower.')
        if args.warning_hours is None:
            parser.exit(1, 'You did not specify warning hours.')
    # Set the global_vars so that DAO can login the meta-database.
    os.chdir(args.conf)
    init_global_configs(args.conf)
    try:
        if args.action == 'show':
            show(args.metric_name, args.host, args.start_time, args.end_time)
        elif args.action == 'clean':
            clean(args.retention_days)
        elif args.action == 'early-warning':
            early_warning(args.metric_name, args.host, args.start_time, args.end_time, args.warning_hours,
                          labels=args.labels, upper=args.upper, lower=args.lower, save_path=args.csv_dump_path)
    except Exception as e:
        write_to_terminal('An error occurred probably due to database operations, '
                          'please check database configurations. For details:\n' +
                          str(e), color='red', level='error')
        traceback.print_tb(e.__traceback__)
        return 2
    return args


if __name__ == '__main__':
    main(sys.argv[1:])
