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
import time
import logging
import traceback
from collections import defaultdict
from datetime import datetime

from prettytable import PrettyTable

from dbmind import global_vars
from dbmind.app.monitoring.monitoring_constants import LONG_TERM_METRIC_STATS
from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.utils.component import initialize_tsdb_param
from dbmind.common.algorithm.forecasting import quickly_forecast
from dbmind.common.utils import adjust_timezone, cast_to_int_or_float, write_to_terminal
from dbmind.common.utils.checking import path_type
from dbmind.common.utils.exporter import KVPairAction, set_logger
from dbmind.service import dai
from dbmind.service.dai import is_sequence_valid

PROMETHEUS_INTERVAL = 15000
PROMETHEUS_MAX_LEN = 1000


def _get_sequences(metric, instance, labels, start_datetime, end_datetime):
    fetcher = dai.get_metric_sequence(
        metric,
        start_datetime,
        end_datetime,
        step=PROMETHEUS_INTERVAL
    ).from_server(instance)

    if labels is not None:
        fetcher = fetcher.filter(**labels)

    return fetcher.fetchall()


def check_over_threshold(timestamps, values, upper, lower, now, tz):
    for timestamp, value in zip(timestamps, values):
        if lower < value < upper:
            continue

        remaining_hours = round((timestamp - now) / 1000 / 60 / 60, 4)
        occur_time = datetime.fromtimestamp(int(timestamp) / 1000, tz=tz)
        return {
            'remaining_hours': remaining_hours,
            'occur_time': occur_time,
            'risk': 'future upper' if value >= upper else 'future lower',
        }

    return {'risk': 'normal'}


def risk_analysis(sequence, upper, lower, warning_minutes, tz=None):
    now = int(time.time() * 1000)
    metric_name = sequence.name
    normal_range = global_vars.metric_value_range_map.get(metric_name, '0,inf').split(',')
    upper_bound = cast_to_int_or_float(normal_range[1].strip())
    lower_bound = cast_to_int_or_float(normal_range[0].strip())
    forecast_sequence = quickly_forecast(
        sequence, warning_minutes, lower_bound, upper_bound,
        given_model="new", given_parameters=(2, 1, 2)
    )

    result = {
        'forecast_timestamps': forecast_sequence.timestamps,
        'forecast_values': forecast_sequence.values,
        'timestamps': sequence.timestamps,
        'values': sequence.values,
        'risk': 'normal',
        'name': sequence.name,
        'labels': sequence.labels
    }

    if sequence.values[-1] >= upper:
        result['risk'] = 'upper'
    elif sequence.values[-1] <= lower:
        result['risk'] = 'lower'
    elif not is_sequence_valid(forecast_sequence):  # unable to get valid data
        result['risk'] = 'unknown'
    else:
        result.update(
            check_over_threshold(
                forecast_sequence.timestamps, forecast_sequence.values,
                upper, lower, now, tz
            )
        )

    return result


def early_warning(metric, instance, retroactive_period, warning_hours, upper, lower,
                  labels=None, tz=None, long_term_statistics_method='mean'):
    # convert hours to minutes
    warnings = defaultdict(list)

    end_time = int(time.time()) * 1000
    max_window = PROMETHEUS_INTERVAL * PROMETHEUS_MAX_LEN
    if retroactive_period is None:
        start_time = end_time - min(max_window, warning_hours * 60 * 60 * 1000 * 3)
    else:
        start_time = end_time - min(max_window, retroactive_period * 60 * 60 * 1000 * 3)

    tz = adjust_timezone(tz)

    if 0 < warning_hours <= 48:
        if instance is None:
            raise ValueError(f'Incorrect value for parameter instance.')

        start_datetime = datetime.fromtimestamp(start_time / 1000, tz=tz)
        end_datetime = datetime.fromtimestamp(end_time / 1000, tz=tz)
        sequences = _get_sequences(metric, instance, labels, start_datetime, end_datetime)
        if not sequences:
            raise ValueError(f"Empty query result for {metric} with "
                             f"instance: {instance} and labels: {labels}.")

    else:
        metric_long_term = metric + '_' + long_term_statistics_method
        if instance is not None:
            source_flag = dai.get_metric_source_flag(metric)
            if labels is None:
                labels = {source_flag: instance}
            elif isinstance(labels, dict):
                labels[source_flag] = instance
            else:
                labels = {source_flag: instance}

        sequences = dai.get_meta_metric_sequence(metric_name=metric_long_term,
                                                 metric_filter=labels if labels is not None else {},
                                                 metric_filter_like={})

        if len(sequences) > 1:
            logging.exception("Metric statistics sequences %s for risk analysis is not unique, "
                              "further filtering is required.", metric_long_term)
            current_labels = [sequence.labels for sequence in sequences]
            raise ValueError("Query result for %s with label %s is not unique. Current sequences labels are %s. "
                             "Please add more filters accordingly.", metric_long_term, labels, current_labels)

        if not sequences:
            if metric not in [metric_statistics.rsplit('_', 1)[0] for metric_statistics in LONG_TERM_METRIC_STATS]:
                logging.warning("Long term forecasting for metric %s is not supported yet.", metric)
            else:
                logging.warning("Empty query result for long term metric statistics %s with label %s.",
                                metric_long_term, labels)

            logging.warning("Trying to switch the source data to short term sequences stored in TSDB, "
                            "the forecasting result might be unreliable.")

            start = datetime.fromtimestamp(start_time / 1000, tz=tz)
            end = datetime.fromtimestamp(end_time / 1000, tz=tz)
            sequences = _get_sequences(metric, instance, labels, start, end)
            if not sequences:
                logging.exception("No sequences are fetched for metric %s with labels %s, "
                                  "risk analysis is aborted.", metric, labels)
                raise ValueError("Risk analysis is aborted since no sequences for metric %s are fetched "
                                 "with labels %s.", metric, labels)

        sequence_duration_in_hours = len(sequences[0]) * sequences[0].step // (1000 * 3600)
        if sequence_duration_in_hours < warning_hours:
            logging.warning("Forecasting a %s hour sequence with a %s hour sequence will not "
                            "cause algorithmic errors, but the results could be very unreliable.",
                            warning_hours, sequence_duration_in_hours)

        start_datetime = datetime.fromtimestamp(sequences[0].timestamps[0] // 1000, tz=tz)
        end_datetime = datetime.fromtimestamp(sequences[0].timestamps[-1] // 1000, tz=tz)

    risk_analysis_params_set = set()
    for sequence in sequences:
        if sequence.name in LONG_TERM_METRIC_STATS:
            sequence.name = sequence.name.rsplit('_', 1)[0]

        risk_analysis_params_set.add((sequence, upper, lower, warning_hours * 60, tz))

    risk_analysis_results = global_vars.worker.parallel_execute(
        risk_analysis, risk_analysis_params_set
    ) or []
    for risk_analysis_result in risk_analysis_results:
        if risk_analysis_result['risk'] == 'future upper':
            if risk_analysis_result['remaining_hours'] <= 0:
                abnormal_detail = (
                    f"The metric might have already exceeded the warning value {upper} "
                    f"at {risk_analysis_result['occur_time']}"
                    f"({-1 * risk_analysis_result['remaining_hours']} hours ago)."
                )
            else:
                abnormal_detail = (
                    f"The metric may exceed the warning value {upper} "
                    f"at {risk_analysis_result['occur_time']}"
                    f"(remaining {risk_analysis_result['remaining_hours']} hours)."
                )

        elif risk_analysis_result['risk'] == 'future lower':
            if risk_analysis_result['remaining_hours'] <= 0:
                abnormal_detail = (
                    f"The metric might have already been lower than the warning value {lower} "
                    f"at {risk_analysis_result['occur_time']}"
                    f"({-1 * risk_analysis_result['remaining_hours']} hours ago)."
                )
            else:
                abnormal_detail = (
                    f"The metric may be lower than the warning value {lower} "
                    f"at {risk_analysis_result['occur_time']}"
                    f"(remaining {risk_analysis_result['remaining_hours']} hours)."
                )

        elif risk_analysis_result['risk'] == 'normal':
            abnormal_detail = 'No risk identified.'
        elif risk_analysis_result['risk'] == 'upper':
            abnormal_detail = 'The metric has exceeded the warning value.'
        elif risk_analysis_result['risk'] == 'lower':
            abnormal_detail = 'The metric has been lower than the warning value.'
        else:
            abnormal_detail = 'Trend prediction failed, risk unknown.'

        warnings[risk_analysis_result['name']].append(
            {'labels': risk_analysis_result['labels'],
             'abnormal_detail': abnormal_detail,
             'values': risk_analysis_result['values'],
             'timestamps': risk_analysis_result['timestamps'],
             'forecast_values': risk_analysis_result['forecast_values'],
             'forecast_timestamps': risk_analysis_result['forecast_timestamps']}
        )
        logging.info("The risk analysis result of %s from %s to %s is (labels: '%s', abnormal_detail: '%s')",
                     risk_analysis_result['name'], start_datetime, end_datetime, risk_analysis_result['labels'],
                     abnormal_detail)
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
    parser = argparse.ArgumentParser(description='Workload Forecasting: Forecast the risk of metric.')
    parser.add_argument('action', choices=('early-warning', ), help='Choose a functionality to perform.')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=path_type,
                        help='Set the directory of configuration files.')
    parser.add_argument('--metric-name', metavar='METRIC_NAME',
                        help='Set a metric name you want to retrieve.')
    parser.add_argument('--instance', metavar='INSTANCE',
                        help="Set a instance you want to retrieve. IP only or IP with port.")
    parser.add_argument('--labels', metavar='LABELS', action=KVPairAction,
                        help='A list of label (format is label=name) separated by comma(,). '
                             'Using in warning.')
    parser.add_argument('--retroactive-period', metavar='RETROACTIVE_PERIOD',
                        type=int, help='Set the retroactive time length, unit is second.')
    parser.add_argument('--upper', metavar='UPPER', type=float,
                        help='The upper value of early-warning. Using in warning.')
    parser.add_argument('--lower', metavar='LOWER', type=float,
                        help='The lower value of early-warning. Using in warning.')
    parser.add_argument('--warning-hours', metavar='WARNING-HOURS', type=int, help='warning length, unit is hour.')

    args = parser.parse_args(argv)

    if not os.path.exists(args.conf):
        parser.exit(1, 'Not found the directory %s.\n' % args.conf)

    if args.action == 'early-warning':
        if args.upper is None and args.lower is None:
            parser.exit(1, 'You did not specify the upper or lower.\n')
        if args.warning_hours is None:
            parser.exit(1, 'You did not specify warning hours.\n')

    log_file_path = os.path.join(os.getcwd(), 'dbmind_forecast.log')
    set_logger(log_file_path, "info")
    # Set the global_vars so that DAO can login the meta-database.
    os.chdir(args.conf)
    init_global_configs(args.conf)
    try:
        if args.action == 'early-warning':
            if not initialize_tsdb_param():
                write_to_terminal("TSDB initialization failed.", color='red')
                return 0

            warnings = early_warning(args.metric_name, args.instance, args.retroactive_period,
                                     args.warning_hours, args.upper, args.lower,
                                     labels=args.labels)
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
