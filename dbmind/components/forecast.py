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
from collections import defaultdict
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
from dbmind.app.diagnosis.query.slow_sql.query_info_source import is_sequence_valid


def _get_sequences(metric, instance, labels, start_datetime, end_datetime):
    if labels is None:
        sequences = dai.get_metric_sequence(metric, start_datetime, end_datetime). \
            from_server(instance).fetchall()
    else:
        sequences = dai.get_metric_sequence(metric, start_datetime, end_datetime). \
            from_server(instance).filter(**labels).fetchall()
    return sequences


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


def show(metric, instance, start_time, end_time):
    field_names = (
        'rowid', 'metric_name',
        'instance', 'metric_time',
        'metric_value'
    )
    output_table = PrettyTable()
    output_table.field_names = field_names

    result = forecasting_metrics.select_forecasting_metric(
        metric_name=metric, instance=instance,
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


def risk_analysis(sequence, upper, lower, warning_minutes):
    current_timestamp = int(time.time() * 1000)
    upper = inf if upper is None else upper
    lower = -inf if lower is None else lower
    if sequence.values[-1] >= upper:
        return {'timestamps': None, 'values': None, 'risk': 'upper'}
    if sequence.values[-1] <= lower:
        return {'timestamps': None, 'values': None, 'risk': 'lower'}
    forecast_sequence = quickly_forecast(sequence, warning_minutes)
    if is_sequence_valid(forecast_sequence):
        for timestamp, value in zip(forecast_sequence.timestamps, forecast_sequence.values):
            if value >= upper or value <= lower:
                flag = 'future upper' if value >= upper else 'future lower'
                remaining_hours = round((timestamp - current_timestamp) / 1000 / 60 / 24, 4)
                occur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp / 1000)))
                return {'timestamps': forecast_sequence.timestamps,
                        'values': forecast_sequence.values,
                        'remaining_hours': remaining_hours,
                        'occur_time': occur_time,
                        'risk': flag}
    else:
        # unable to get valid data
        return {'timestamps': None, 'values': None, 'risk': 'unknown'}
    return {'timestamps': None, 'values': None, 'risk': 'normal'}


def early_warning(metric, instance, start_time, end_time, warning_hours, upper=None, lower=None, labels=None):
    # convert hours to minutes
    warnings = defaultdict(list)
    warning_minutes = warning_hours * 60
    if end_time is None:
        end_time = int(time.time() * 1000)
    if start_time is None:
        # the default historical sequence is 3 times the length of the predicted sequence
        start_time = end_time - warning_minutes * 60 * 1000 * 3
    start_datetime = datetime.fromtimestamp(start_time / 1000)
    end_datetime = datetime.fromtimestamp(end_time / 1000)
    sequences = _get_sequences(metric, instance, labels, start_datetime, end_datetime)
    for sequence in sequences:
        risk_analysis_result = risk_analysis(sequence, upper, lower, warning_minutes)
        if risk_analysis_result['risk'] == 'future upper':
            abnormal_detail = "exceed the warning value %s at %s(remaining %s hours)." % \
                              (upper, risk_analysis_result['occur_time'], risk_analysis_result['remaining_hours'])
        elif risk_analysis_result['risk'] == 'lower upper':
            abnormal_detail = "lower than the warning value %s at %s(remaining %s hours)." % \
                              (upper, risk_analysis_result['occur_time'], risk_analysis_result['remaining_hours'])
        elif risk_analysis_result['risk'] == 'normal':
            abnormal_detail = 'No risk find.'

        elif risk_analysis_result['risk'] == 'upper':
            abnormal_detail = 'metric has exceeded the warning value'
        elif risk_analysis_result['risk'] == 'lower':
            abnormal_detail = 'metric has been less than the warning value.'
        else:
            abnormal_detail = 'Trend prediction failed, risk unknown.'
        warnings[sequence.name].append({'labels': sequence.labels,
                                        'abnormal_detail': abnormal_detail,
                                        'values': sequence.values,
                                        'timestamps': sequence.timestamps,
                                        'forecast_values': risk_analysis_result['values'],
                                        'forecast_timestamps': risk_analysis_result['timestamps']
                                        })
    return warnings


def display_warnings(warnings):
    output_table = PrettyTable()
    output_table.field_names = ('name', 'label', 'warning information')
    output_table.align = "l"
    for name, details in warnings.items():
        for detail in details:
            output_table.add_row([name, str(detail['labels']), detail['abnormal_detail']])
    print(output_table)


def main(argv):
    parser = argparse.ArgumentParser(description='Workload Forecasting: Forecast monitoring metrics')
    parser.add_argument('action', choices=('show', 'clean', 'early-warning'), help='Choose a functionality to perform')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=path_type,
                        help='Set the directory of configuration files')
    parser.add_argument('--metric-name', metavar='METRIC_NAME',
                        help='Set a metric name you want to retrieve')
    parser.add_argument('--instance', metavar='INSTANCE',
                        help="Set a instance you want to retrieve. IP only or IP with port.")
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
        if None in (args.metric_name, args.instance, args.start_time, args.end_time):
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
            show(args.metric_name, args.instance, args.start_time, args.end_time)
        elif args.action == 'clean':
            clean(args.retention_days)
        elif args.action == 'early-warning':
            if not _initialize_tsdb_param():
                write_to_terminal("TSDB initialization failed.", color='red')
                return 0
            warnings = early_warning(args.metric_name, args.instance, args.start_time, args.end_time, args.warning_hours,
                                     labels=args.labels, upper=args.upper, lower=args.lower)
            display_warnings(warnings)
    except Exception as e:
        write_to_terminal('An error occurred probably due to database operations, '
                          'please check database configurations. For details:\n' +
                          str(e), color='red', level='error')
        traceback.print_tb(e.__traceback__)
        return 2
    return args


if __name__ == '__main__':
    main(sys.argv[1:])
