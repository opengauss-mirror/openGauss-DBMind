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
import traceback
from datetime import datetime
from getpass import getpass

from prettytable import PrettyTable
from psycopg2.extensions import parse_dsn

from dbmind import constants
from dbmind import global_vars
from dbmind.app.diagnosis.query.slow_sql.query_info_source import is_sequence_valid, is_driver_result_valid
from dbmind.app.monitoring.generic_detection import AnomalyDetections
from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.opengauss_driver import Driver
from dbmind.common.types import Sequence
from dbmind.common.types.sequence import EMPTY_SEQUENCE
from dbmind.common.utils.checking import path_type, date_type
from dbmind.common.utils.cli import write_to_terminal
from dbmind.common.utils.exporter import set_logger
from dbmind.components.slow_query_diagnosis import initialize_rpc_service, initialize_tsdb_param
from dbmind.service import dai

continuous_increasing_detector = AnomalyDetections.do_increase_detect


def try_to_initialize_rpc_and_tsdb():
    if not initialize_rpc_service():
        return False, 'RPC service does not exist, exiting...'
    if not initialize_tsdb_param():
        return False, 'TSDB service does not exist, exiting...'
    return True, None


def try_to_get_driver(url):
    driver = Driver()
    try:
        driver.initialize(url)
    except ConnectionError:
        return None, 'Error occurred when initialized the URL, exiting...'
    return driver, None


class GetMemoryDetailFromTSDB:
    """RPC service exists when TSDB service exists."""

    def __init__(self, instance, start_time, end_time):
        self.start_time = datetime.fromtimestamp(start_time / 1000)
        self.end_time = datetime.fromtimestamp(end_time / 1000)
        self.instance = instance
        self.database = 'postgres'

    @property
    def total_memory_detail(self):
        # Consider further filtering based on specific type
        total_memory_detail = {}
        sequences = dai.get_metric_sequence('pg_total_memory_detail_mbytes', self.start_time, self.end_time). \
            from_server(self.instance).fetchall()
        if is_sequence_valid(sequences):
            for sequence in sequences:
                memory_type = sequence.labels.get('type')
                total_memory_detail[memory_type] = sequence
        return total_memory_detail

    @property
    def shared_context_memory_detail(self):
        shared_context_memory_detail = {}
        sequences = dai.get_metric_sequence('pg_shared_memory_detail_totalsize', self.start_time, self.end_time). \
            from_server(self.instance).fetchall()
        if is_sequence_valid(sequences):
            for sequence in sequences:
                context = sequence.labels.get('contextname')
                shared_context_memory_detail[context] = sequence
        return shared_context_memory_detail

    @property
    def session_context_memory_detail(self):
        session_memory_detail = {}
        sequences = dai.get_metric_sequence('pg_session_memory_detail_totalsize', self.start_time, self.end_time). \
            from_server(self.instance).fetchall()
        if is_sequence_valid(sequences):
            for sequence in sequences:
                context = sequence.labels.get('contextname')
                session_memory_detail[context] = sequence
        return session_memory_detail

    def get_shared_memctx_detail(self, context):
        stmt = "select * from gs_get_shared_memctx_detail('%s')" % context
        return global_vars.agent_proxy.call('query_in_database', stmt, self.database, return_tuples=False)

    def get_session_memctx_detail(self, context):
        stmt = "select * from gs_get_session_memctx_detail('%s')" % context
        return global_vars.agent_proxy.call('query_in_database', stmt, self.database, return_tuples=False)

    def get_thread_memctx_detail(self, tid, context):
        stmt = "select * from gs_get_thread_memctx_detail(%s, '%s')" % (tid, context)
        return global_vars.agent_proxy.call('query_in_database', stmt, self.database, return_tuples=False)


class GetMemoryDetailFromDriver:
    def __init__(self, driver=None):
        self.driver = driver
        self.database = 'postgres'

    @property
    def total_memory_detail(self):
        total_memory_detail = {}
        stmt = "select memorytype, memorymbytes from pg_catalog.gs_total_memory_detail;"
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('memorytype')
                totalsize = row.get('memorymbytes')
                sequence = Sequence(values=(totalsize,), timestamps=(int(time.time()) * 1000,))
                total_memory_detail[context] = sequence
        return total_memory_detail

    @property
    def shared_context_memory_detail(self):
        shared_context_memory_detail = {}
        stmt = "select contextname, sum(totalsize) / 1024 / 1024 as totalsize from gs_shared_memory_detail " \
               "group by contextname order by totalsize desc limit 10;"
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('contextname')
                totalsize = row.get('totalsize')
                sequence = Sequence(values=(totalsize,), timestamps=(int(time.time()) * 1000,))
                shared_context_memory_detail[context] = sequence
        return shared_context_memory_detail

    @property
    def session_context_memory_detail(self):
        session_context_memory_detail = {}
        stmt = "select contextname, sum(totalsize) / 1024 / 1024 as totalsize from gs_session_memory_detail " \
               "group by contextname order by totalsize desc limit 10;"
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('contextname')
                totalsize = row.get('totalsize')
                sequence = Sequence(values=(totalsize,), timestamps=(int(time.time()) * 1000,))
                session_context_memory_detail[context] = sequence
        return session_context_memory_detail

    def get_shared_memctx_detail(self, context):
        stmt = "select * from gs_get_shared_memctx_detail('%s')" % context
        return self.driver.query('query_in_database', stmt, 'postgres', return_tuples=False)

    def get_session_memctx_detail(self, context):
        stmt = "select * from gs_get_session_memctx_detail('%s')" % context
        return self.driver.query('query_in_database', stmt, 'postgres', return_tuples=False)

    def get_thread_memctx_detail(self, tid, context):
        stmt = "select * from gs_get_thread_memctx_detail(%s, '%s')" % (tid, context)
        return self.driver.query('query_in_database', stmt, 'postgres', return_tuples=False)


class MemoryChecker:
    def __init__(self, memory_detail):
        self.memory_detail = memory_detail
        self.minimal_elem_of_series_analysis = 5
        self.latest_elem_index = -1
        self.abnormal_memory_occupy_rate = 0.1
        self.output_context_num = 5
        self.output_file_num = 3

    def large_process_used_memory(self):
        large_process_used_memory_detail = {'status': 'normal', 'detail': ''}
        max_process_memory_sequence = self.memory_detail.total_memory_detail.get('max_process_memory', EMPTY_SEQUENCE)
        process_used_memory_sequence = self.memory_detail.total_memory_detail.get('process_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(max_process_memory_sequence) or not is_sequence_valid(process_used_memory_sequence):
            large_process_used_memory_detail['status'] = 'unknown'
            large_process_used_memory_detail['detail'] = "unable to get valid data"
            return large_process_used_memory_detail
        if process_used_memory_sequence.values[self.latest_elem_index] / \
                max_process_memory_sequence.values[self.latest_elem_index] >= self.abnormal_memory_occupy_rate:
            large_process_used_memory_detail['status'] = 'abnormal'
            rate = round(process_used_memory_sequence.values[self.latest_elem_index] /
                         max_process_memory_sequence.values[self.latest_elem_index], 4)
            large_process_used_memory_detail['detail'] = "the proportion of process_used_memory to " \
                                                         "max_process_memory: %s" % rate
        return large_process_used_memory_detail

    def large_dynamic_used_shrctx(self):
        large_dynamic_used_shrctx_detail = {'status': 'normal', 'detail': ''}
        dynamic_used_shrctx_sequence = self.memory_detail.total_memory_detail.get('dynamic_used_shrctx', EMPTY_SEQUENCE)
        max_dynamic_memory_sequence = self.memory_detail.total_memory_detail.get('max_dynamic_memory',
                                                                                 EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_shrctx_sequence) or not is_sequence_valid(max_dynamic_memory_sequence):
            large_dynamic_used_shrctx_detail['status'] = 'unknown'
            large_dynamic_used_shrctx_detail['detail'] = "unable to get valid data"
            return large_dynamic_used_shrctx_detail
        latest_dynamic_used_shrctx = dynamic_used_shrctx_sequence.values[self.latest_elem_index]
        latest_max_dynamic_memory = max_dynamic_memory_sequence.values[self.latest_elem_index]
        if latest_dynamic_used_shrctx / latest_max_dynamic_memory >= self.abnormal_memory_occupy_rate:
            large_dynamic_used_shrctx_detail['status'] = 'abnormal'
            rate = round(latest_dynamic_used_shrctx / latest_max_dynamic_memory, 4)
            detail = {}
            shared_context_memory_detail = list(self.memory_detail.shared_context_memory_detail.items())
            shared_context_memory_detail = [(item[0], max(item[1].values)) for item in shared_context_memory_detail]
            shared_context_memory_detail.sort(key=lambda item: item[1], reverse=True)
            for context, totalsize in shared_context_memory_detail[:self.output_context_num]:
                memory_detail = self.memory_detail.get_shared_memctx_detail(context)
                if not memory_detail:
                    detail[context] = 'NULL'
                    continue
                detail[context] = []
                memory_detail = list(sorted(memory_detail, key=lambda item: item['size'], reverse=True))
                for item in memory_detail[:self.output_file_num]:
                    detail[context].append("file:%s line:%s, size:%s" % (item['file'], item['line'], item['size']))
            large_dynamic_used_shrctx_detail['detail'] = "the proportion of dynamic_used_shrctx to " \
                                                         "max_dynamic_memory: %s, the memory detail: %s" % \
                                                         (rate, detail)
        return large_dynamic_used_shrctx_detail

    def large_dynamic_used_memory(self):
        large_dynamic_used_memory_detail = {'status': 'normal', 'detail': ''}
        dynamic_used_memory_sequence = self.memory_detail.total_memory_detail.get('dynamic_used_memory', EMPTY_SEQUENCE)
        max_dynamic_memory_sequence = self.memory_detail.total_memory_detail.get('max_dynamic_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_memory_sequence) or not is_sequence_valid(max_dynamic_memory_sequence):
            large_dynamic_used_memory_detail['status'] = 'unknown'
            large_dynamic_used_memory_detail['detail'] = "unable to get valid data"
            return large_dynamic_used_memory_detail
        if dynamic_used_memory_sequence.values[self.latest_elem_index] / \
                max_dynamic_memory_sequence.values[self.latest_elem_index] >= self.abnormal_memory_occupy_rate:
            large_dynamic_used_memory_detail['status'] = 'abnormal'
            rate = round(
                dynamic_used_memory_sequence.values[self.latest_elem_index] /
                max_dynamic_memory_sequence.values[self.latest_elem_index], 4)
            detail = {}
            session_context_memory_detail = list(self.memory_detail.session_context_memory_detail.items())
            session_context_memory_detail = [(item[0], max(item[1].values)) for item in session_context_memory_detail]
            session_context_memory_detail.sort(key=lambda item: item[1], reverse=True)
            for context, totalsize in session_context_memory_detail[:self.output_context_num]:
                memory_detail = self.memory_detail.get_session_memctx_detail(context)
                if not memory_detail:
                    detail[context] = 'NULL'
                    continue
                detail[context] = []
                memory_detail = list(sorted(memory_detail, key=lambda item: item['size'], reverse=True))
                for item in memory_detail[:self.output_file_num]:
                    detail[context].append("file:%s line:%s, size:%s" % (item['file'], item['line'], item['size']))
            large_dynamic_used_memory_detail['detail'] = "the proportion of dynamic_used_memory to " \
                                                         "max_dynamic_memory: %s, the memory detail : %s" % \
                                                         (rate, detail)
        return large_dynamic_used_memory_detail

    def other_used_memory_continuous_increase(self):
        # monitoring whether memory increase consistently and exceed thresholds
        other_used_memory_detail = {'status': 'normal', 'detail': ''}
        other_used_memory_sequence = self.memory_detail.total_memory_detail.get('other_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(other_used_memory_sequence):
            other_used_memory_detail['status'] = 'unknown'
            other_used_memory_detail['detail'] = "unable to get valid data"
            return other_used_memory_detail
        if len(other_used_memory_sequence) >= self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector(other_used_memory_sequence, side="positive")
            if True in increase_anomalies.values:
                other_used_memory_detail['status'] = 'abnormal'
                other_used_memory_detail['detail'] = "other_used_memory continues to increase over time"
                other_used_memory_detail['timestamps'] = other_used_memory_sequence.timestamps
                other_used_memory_detail['values'] = other_used_memory_sequence.values
        else:
            other_used_memory_detail['detail'] = "too little data for calculations to judge trend"
        return other_used_memory_detail

    def process_used_memory_continuous_increase(self):
        process_used_memory_detail = {'status': 'normal', 'detail': ''}
        process_used_memory_sequence = self.memory_detail.total_memory_detail.get('process_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(process_used_memory_sequence):
            process_used_memory_detail['status'] = 'unknown'
            process_used_memory_detail['detail'] = "unable to get valid data"
        if len(process_used_memory_sequence) > self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector(process_used_memory_sequence, side="positive")
            if True in increase_anomalies.values:
                process_used_memory_detail['status'] = 'abnormal'
                process_used_memory_detail['detail'] = "process_used_memory continues to increase over time"
                process_used_memory_detail['timestamps'] = process_used_memory_sequence.timestamps
                process_used_memory_detail['values'] = process_used_memory_sequence.values
        else:
            process_used_memory_detail['detail'] = "too little data for calculations to judge trend"
        return process_used_memory_detail

    def dynamic_used_memory_continuous_increase(self):
        dynamic_used_memory_detail = {'status': 'normal', 'detail': ''}
        dynamic_used_memory_sequence = self.memory_detail.total_memory_detail.get('dynamic_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_memory_sequence):
            dynamic_used_memory_detail['status'] = 'unknown'
            dynamic_used_memory_detail['detail'] = "unable to get valid data"
        if len(dynamic_used_memory_sequence) > self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector(dynamic_used_memory_sequence, side="positive")
            if True in increase_anomalies.values:
                dynamic_used_memory_detail['status'] = 'abnormal'
                dynamic_used_memory_detail['detail'] = "dynamic_used_memory continues to increase over time"
                dynamic_used_memory_detail['timestamps'] = dynamic_used_memory_sequence.timestamps
                dynamic_used_memory_detail['values'] = dynamic_used_memory_sequence.values
        else:
            dynamic_used_memory_detail['detail'] = "too little data for calculations to judge trend"
        return dynamic_used_memory_detail

    def dynamic_used_shrctx_continuous_increase(self):
        dynamic_used_shrctx_detail = {'status': 'normal', 'detail': ''}
        dynamic_used_shrctx_sequence = self.memory_detail.total_memory_detail.get('dynamic_used_shrctx', EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_shrctx_sequence):
            dynamic_used_shrctx_detail['status'] = 'unknown'
            dynamic_used_shrctx_detail['detail'] = "unable to get valid data"
        if len(dynamic_used_shrctx_sequence) > self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector(dynamic_used_shrctx_sequence, side="positive")
            if True in increase_anomalies.values:
                dynamic_used_shrctx_detail['status'] = 'abnormal'
                dynamic_used_shrctx_detail['detail'] = "dynamic_used_shrctx continues to increase over time"
                dynamic_used_shrctx_detail['timestamps'] = dynamic_used_shrctx_sequence.timestamps
                dynamic_used_shrctx_detail['values'] = dynamic_used_shrctx_sequence.values
        else:
            dynamic_used_shrctx_detail['detail'] = "too little data for calculations to judge trend"
        return dynamic_used_shrctx_detail

    def topk_context_from_session_memory_continuous_increase(self):
        # monitoring topk memory context
        topk_session_memory_detail = {'status': 'normal', 'detail': '', 'data': {}}
        topk_session_memory = self.memory_detail.session_context_memory_detail
        for context, sequence in topk_session_memory.items():
            if not is_sequence_valid(sequence):
                continue
            if len(sequence) >= self.minimal_elem_of_series_analysis:
                increase_anomalies = continuous_increasing_detector(sequence, side='positive')
                if True in increase_anomalies.values:
                    topk_session_memory_detail['data'][context] = {}
                    topk_session_memory_detail['data'][context]['timestamps'] = sequence.timestamps
                    topk_session_memory_detail['data'][context]['values'] = sequence.values
        if topk_session_memory_detail.get('data'):
            topk_session_memory_detail['detail'] = "%s continues to increase over time" % \
                                                   (list(topk_session_memory_detail.get('data').keys()))
            topk_session_memory_detail['status'] = 'abnormal'
        return topk_session_memory_detail

    def topk_context_from_shared_memory_continuous_increase(self):
        # monitoring topk memory context
        topk_shared_memory_detail = {'status': 'normal', 'detail': '', 'data': {}}
        topk_shared_memory = self.memory_detail.shared_context_memory_detail
        for context, sequence in topk_shared_memory.items():
            if not is_sequence_valid(sequence):
                continue
            if len(sequence) >= self.minimal_elem_of_series_analysis:
                increase_anomalies = continuous_increasing_detector(sequence, side='positive')
                if True in increase_anomalies.values:
                    topk_shared_memory_detail['data'][context] = {}
                    topk_shared_memory_detail['data'][context]['timestamps'] = sequence.timestamps
                    topk_shared_memory_detail['data'][context]['values'] = sequence.values
        if topk_shared_memory_detail.get('data'):
            topk_shared_memory_detail['detail'] = "%s continues to increase over time" % \
                                                   (list(topk_shared_memory_detail.get('data').keys()))
            topk_shared_memory_detail['status'] = 'abnormal'
        return topk_shared_memory_detail

    def __call__(self):
        return {'large_process_used_memory': self.large_process_used_memory(),
                'large_dynamic_used_memory': self.large_dynamic_used_memory(),
                'large_dynamic_used_shrctx': self.large_dynamic_used_shrctx(),
                'other_used_memory_continuous_increase': self.other_used_memory_continuous_increase(),
                'process_used_memory_continuous_increase': self.process_used_memory_continuous_increase(),
                'dynamic_used_memory_continuous_increase': self.dynamic_used_memory_continuous_increase(),
                'topk_context_from_shared_memory_continuous_increase':
                    self.topk_context_from_shared_memory_continuous_increase(),
                'topk_context_from_session_memory_continuous_increase':
                    self.topk_context_from_session_memory_continuous_increase()
                }


def memory_check(start_time, end_time, driver=None, data_source='TSDB'):
    if data_source == 'TSDB':
        instance = global_vars.agent_proxy.current_agent_addr()
        memory_detail = GetMemoryDetailFromTSDB(instance, start_time, end_time)
    else:
        memory_detail = GetMemoryDetailFromDriver(driver)
    memory_checker = MemoryChecker(memory_detail)
    return memory_checker()


def format_pretty_table(start_time, end_time, result):
    prompt = PrettyTable()
    prompt.align = "l"
    prompt.field_names = ('check_item', 'status', 'detail')
    prompt.get_string(title="Memory check result from %s to %s" % (start_time, end_time))
    for check_item, detail in result.items():
        status = detail.get('status', 'unknown')
        check_detail = detail.get('detail', '')
        prompt.add_row((check_item, status, check_detail))
    print(prompt)


def main(argv):
    parser = argparse.ArgumentParser(description='Memory Checker: Discover potential risks in memory.')
    parser.add_argument('action', choices=('check',),
                        help='choose a functionality to perform')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=path_type,
                        help='Set the directory of configuration files')
    parser.add_argument('--start-time', metavar='TIMESTAMP_IN_MICROSECONDS', type=date_type,
                        help='Set the start time of a slow SQL diagnosis result to be retrieved')
    parser.add_argument('--end-time', metavar='TIMESTAMP_IN_MICROSECONDS', type=date_type,
                        help='Set the end time of a slow SQL diagnosis result to be retrieved')
    parser.add_argument('--url', metavar='DSN of database',
                        help="set database dsn('postgres://user@host:port/dbname' or "
                             "'user=user dbname=dbname host=host port=port') "
                             "when tsdb is not available. Note: don't contain password in DSN for this diagnosis.")
    parser.add_argument('--data-source', choices=('TSDB', 'DRIVER'), metavar='data source of SLOW-SQL-RCA',
                        default='TSDB',
                        help='set database dsn when tsdb is not available. Using in diagnosis.')
    args = parser.parse_args(argv)
    # add dummy fields
    args.driver = None

    if not os.path.exists(args.conf):
        parser.exit(1, 'Not found the directory %s.\n' % args.conf)

    # Set the global_vars so that DAO can login the meta-database.
    os.chdir(args.conf)
    init_global_configs(args.conf)
    set_logger(os.path.join('logs', constants.MEMORY_CHECKER_LOG_NAME), "info")
    if args.action == 'check':
        if args.data_source == 'driver':
            if args.url is None:
                parser.exit(1, "Quitting due to lack of URL.\n")
            try:
                parsed_dsn = parse_dsn(args.url)
                if 'password' in parsed_dsn:
                    parser.exit(1, "Quitting due to security considerations.\n")
                password = getpass('Please input the password for URL:')
                parsed_dsn['password'] = password
                args.url = ' '.join(['{}={}'.format(k, v) for (k, v) in parsed_dsn.items()])
            except Exception:
                parser.exit(1, "Quitting due to wrong URL format.\n")
            args.driver, message = try_to_get_driver(args.url)
            if not args.driver:
                parser.exit(1, message)
        elif args.data_source == 'TSDB':
            success, message = try_to_initialize_rpc_and_tsdb()
            if not success:
                parser.exit(1, message)
    try:
        if args.action == 'check':
            result = memory_check(args.start_time, args.end_time, driver=args.driver, data_source=args.data_source)
            format_pretty_table(start_time=args.start_time, end_time=args.end_time, result=result)
    except Exception as e:
        write_to_terminal('An error occurred probably due to database operations, '
                          'please check database configurations. For details:\n' +
                          str(e), color='red', level='error')
        traceback.print_tb(e.__traceback__)
        return 2


if __name__ == '__main__':
    main(sys.argv[1:])
