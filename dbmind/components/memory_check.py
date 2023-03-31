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
from dbmind.app import monitoring
from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.algorithm import anomaly_detection
from dbmind.common.opengauss_driver import Driver
from dbmind.common.types import Sequence
from dbmind.common.types import EMPTY_SEQUENCE
from dbmind.common.utils.checking import path_type, date_type
from dbmind.common.utils.cli import write_to_terminal
from dbmind.common.utils import cached_property
from dbmind.common.utils.exporter import set_logger
from dbmind.common.utils.component import initialize_rpc_service, initialize_tsdb_param
from dbmind.service import dai
from dbmind.service.dai import is_sequence_valid, is_driver_result_valid

ONE_DAY = 24 * 60 * 60

continuous_increasing_detector = anomaly_detection.IncreaseDetector(
    side=monitoring.get_detection_param("increasing_side")
)


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
        self.instance_with_no_port = instance.split(':')[0]

    @cached_property
    def history_total_memory_detail(self):
        # consider further filtering based on specific type
        total_memory_detail = {}
        sequences = dai.get_metric_sequence('pg_total_memory_detail_mbytes', self.start_time, self.end_time). \
            from_server(self.instance).fetchall()
        if is_sequence_valid(sequences):
            for sequence in sequences:
                memory_type = sequence.labels.get('type')
                total_memory_detail[memory_type] = sequence
        return total_memory_detail

    @cached_property
    def real_time_total_memory_detail(self):
        total_memory_detail = {}
        stmt = "select memorytype, memorymbytes from pg_catalog.gs_total_memory_detail;"
        rows = global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('memorytype')
                totalsize = row.get('memorymbytes')
                sequence = Sequence(values=(totalsize,),
                                    timestamps=(int(time.time()) * 1000,), labels={'name': 'pg_total_memory'})
                total_memory_detail[context] = sequence
        return total_memory_detail

    @cached_property
    def history_shared_context_memory_detail(self):
        shared_context_memory_detail = {}
        sequences = dai.get_metric_sequence('pg_shared_memory_detail_totalsize', self.start_time, self.end_time). \
            from_server(self.instance).fetchall()
        if is_sequence_valid(sequences):
            for sequence in sequences:
                context = sequence.labels.get('contextname')
                shared_context_memory_detail[context] = sequence
        return shared_context_memory_detail

    @cached_property
    def real_time_shared_context_memory_detail(self):
        shared_context_memory_detail = {}
        stmt = "select contextname, sum(totalsize) / 1024 / 1024 as totalsize from gs_shared_memory_detail " \
               "group by contextname order by totalsize desc limit 10;"
        rows = global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('contextname')
                totalsize = row.get('totalsize')
                sequence = Sequence(values=(totalsize,),
                                    timestamps=(int(time.time()) * 1000,), labels={'name': 'shared_context_memor'})
                shared_context_memory_detail[context] = sequence
        return shared_context_memory_detail

    @cached_property
    def history_session_context_memory_detail(self):
        session_memory_detail = {}
        sequences = dai.get_metric_sequence('pg_session_memory_detail_totalsize', self.start_time, self.end_time). \
            from_server(self.instance).fetchall()
        if is_sequence_valid(sequences):
            for sequence in sequences:
                context = sequence.labels.get('contextname')
                session_memory_detail[context] = sequence
        return session_memory_detail

    @cached_property
    def real_time_session_context_memory_detail(self):
        session_context_memory_detail = {}
        stmt = "select contextname, sum(totalsize) / 1024 / 1024 as totalsize from gs_session_memory_detail " \
               "group by contextname order by totalsize desc limit 10;"
        rows = global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('contextname')
                totalsize = row.get('totalsize')
                sequence = Sequence(values=(totalsize,),
                                    timestamps=(int(time.time()) * 1000,), labels={'name': 'session_context_memory'})
                session_context_memory_detail[context] = sequence
        return session_context_memory_detail

    @cached_property
    def history_mem_usage_detail(self):
        sequence = dai.get_metric_sequence('os_mem_usage', self.start_time, self.end_time).from_server(
            self.instance_with_no_port).fetchone()
        if is_sequence_valid(sequence):
            return sequence
        return EMPTY_SEQUENCE

    @cached_property
    def topk_session_memory_sql(self):
        stmt = "select  psa.pid, psa.query_start, current_timestamp - psa.query_start as running_time, " \
               "psa.application_name, psm.contextname, psm.totalsize / 1024 / 1024 as totalsize,  " \
               "psm.usedsize / 1024 / 1024 as usedsize, psm.freesize / 1024 / 1024 as freesize, psa.query from " \
               "pg_catalog.gs_session_memory_detail psm join pg_stat_activity psa on " \
               "pid=substring(sessid, 12) where state='active' order by totalsize, running_time desc limit 20;"
        return global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)

    @cached_property
    def topk_running_time_sql(self):
        stmt = "select query, query_start, current_timestamp - query_start as running_time, " \
               "application_name, waiting, state from pg_stat_activity order by running_time desc limit 10;"
        return global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)

    def get_shared_memctx_detail(self, context):
        stmt = "select * from gs_get_shared_memctx_detail('%s')" % context
        return global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)

    def get_session_memctx_detail(self, context):
        stmt = "select * from gs_get_session_memctx_detail('%s')" % context
        return global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)

    def get_thread_memctx_detail(self, tid, context):
        stmt = "select * from gs_get_thread_memctx_detail(%s, '%s')" % (tid, context)
        return global_vars.agent_proxy.call('query_in_database', stmt, None, return_tuples=False)


class GetMemoryDetailFromDriver:
    def __init__(self, driver=None):
        self.driver = driver

    @cached_property
    def real_time_total_memory_detail(self):
        total_memory_detail = {}
        stmt = "select memorytype, memorymbytes from pg_catalog.gs_total_memory_detail;"
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('memorytype')
                totalsize = row.get('memorymbytes')
                sequence = Sequence(values=(totalsize,),
                                    timestamps=(int(time.time()) * 1000,), labels={'name': 'pg_total_memory'})
                total_memory_detail[context] = sequence
        return total_memory_detail

    @cached_property
    def history_total_memory_detail(self):
        return {}

    @cached_property
    def history_shared_context_memory_detail(self):
        return {}

    @cached_property
    def real_time_shared_context_memory_detail(self):
        shared_context_memory_detail = {}
        stmt = "select contextname, sum(totalsize) / 1024 / 1024 as totalsize from gs_shared_memory_detail " \
               "group by contextname order by totalsize desc limit 10;"
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('contextname')
                totalsize = row.get('totalsize')
                sequence = Sequence(values=(totalsize,),
                                    timestamps=(int(time.time()) * 1000,), labels={'name': 'shared_context_memory'})
                shared_context_memory_detail[context] = sequence
        return shared_context_memory_detail

    @cached_property
    def history_session_context_memory_detail(self):
        return {}

    @cached_property
    def real_time_session_context_memory_detail(self):
        session_context_memory_detail = {}
        stmt = "select contextname, sum(totalsize) / 1024 / 1024 as totalsize from gs_session_memory_detail " \
               "group by contextname order by totalsize desc limit 10;"
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            for row in rows:
                context = row.get('contextname')
                totalsize = row.get('totalsize')
                sequence = Sequence(values=(totalsize,),
                                    timestamps=(int(time.time()) * 1000,), labels={'name': 'session_context_memory'})
                session_context_memory_detail[context] = sequence
        return session_context_memory_detail

    @cached_property
    def history_mem_usage_detail(self):
        return EMPTY_SEQUENCE

    @cached_property
    def topk_session_memory_sql(self):
        stmt = "select  psa.pid, psa.query_start, current_timestamp - psa.query_start as running_time, " \
               "psa.application_name, psm.contextname, psm.totalsize / 1024 / 1024 as totalsize,  " \
               "psm.usedsize / 1024 / 1024 as usedsize, psm.freesize / 1024 / 1024 as freesize, psa.query from " \
               "pg_catalog.gs_session_memory_detail psm join pg_stat_activity psa on " \
               "pid=substring(sessid, 12) where state='active' order by totalsize, running_time desc limit 20;"
        return self.driver.query(stmt, return_tuples=False)

    @cached_property
    def topk_running_time_sql(self):
        stmt = "select query, query_start, current_timestamp - query_start as running_time, " \
               "application_name, waiting, state from pg_stat_activity order by running_time desc limit 10;"
        return self.driver.query(stmt, return_tuples=False)

    def get_shared_memctx_detail(self, context):
        stmt = "select * from gs_get_shared_memctx_detail('%s')" % context
        return self.driver.query(stmt, return_tuples=False)

    def get_session_memctx_detail(self, context):
        stmt = "select * from gs_get_session_memctx_detail('%s')" % context
        return self.driver.query(stmt, return_tuples=False)

    def get_thread_memctx_detail(self, tid, context):
        stmt = "select * from gs_get_thread_memctx_detail(%s, '%s')" % (tid, context)
        return self.driver.query(stmt, return_tuples=False)


class MemoryChecker:
    def __init__(self, memory_detail):
        self.memory_detail = memory_detail
        self.minimal_elem_of_series_analysis = 5
        self.latest_elem_index = -1
        self.abnormal_memory_occupy_rate = 0.1
        self.output_context_num = 10
        self.output_file_num = 10

    def large_os_memory_usage(self):
        # system memory usage is too large
        os_mem_usage_detail = {'status': 'normal', 'rate': 0, 'remark': ''}
        os_mem_usage_sequence = self.memory_detail.history_mem_usage_detail
        if not is_sequence_valid(os_mem_usage_sequence):
            os_mem_usage_detail['remark'] = 'No data found'
            os_mem_usage_detail['status'] = 'unknown'
            return os_mem_usage_detail
        if os_mem_usage_sequence.values[self.latest_elem_index] > self.abnormal_memory_occupy_rate:
            os_mem_usage_detail['status'] = 'abnormal'
            os_mem_usage_detail['rate'] = os_mem_usage_sequence.values[self.latest_elem_index]
        return os_mem_usage_detail

    def large_process_used_memory(self):
        # determine whether the proportion of process_used_memory to max_process_memory is too large
        large_process_used_memory_detail = {'status': 'normal', 'rate': 0, 'remark': ''}
        max_process_memory_sequence = self.memory_detail.real_time_total_memory_detail.\
            get('max_process_memory', EMPTY_SEQUENCE)
        process_used_memory_sequence = self.memory_detail.real_time_total_memory_detail.\
            get('process_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(max_process_memory_sequence) or not is_sequence_valid(process_used_memory_sequence):
            large_process_used_memory_detail['status'] = 'unknown'
            large_process_used_memory_detail['remark'] = "unable to get valid data"
            return large_process_used_memory_detail
        latest_process_used_memory = process_used_memory_sequence.values[self.latest_elem_index]
        latest_max_process_memory = max_process_memory_sequence.values[self.latest_elem_index]
        if latest_process_used_memory / latest_max_process_memory >= self.abnormal_memory_occupy_rate:
            large_process_used_memory_detail['status'] = 'abnormal'
            large_process_used_memory_detail['rate'] = round(latest_process_used_memory / latest_max_process_memory, 4)
        return large_process_used_memory_detail

    def large_dynamic_used_shrctx(self):
        # determine whether the proportion of dynamic_used_shrctx to max_dynamic_memory is too large
        large_dynamic_used_shrctx_detail = {'status': 'normal', 'rate': 0, 'remark': '', 'data': {}}
        dynamic_used_shrctx_sequence = self.memory_detail.real_time_total_memory_detail.\
            get('dynamic_used_shrctx', EMPTY_SEQUENCE)
        max_dynamic_memory_sequence = self.memory_detail.real_time_total_memory_detail.\
            get('max_dynamic_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_shrctx_sequence) or not is_sequence_valid(max_dynamic_memory_sequence):
            large_dynamic_used_shrctx_detail['status'] = 'unknown'
            large_dynamic_used_shrctx_detail['remark'] = "unable to get valid data"
            return large_dynamic_used_shrctx_detail
        latest_dynamic_used_shrctx = dynamic_used_shrctx_sequence.values[self.latest_elem_index]
        latest_max_dynamic_memory = max_dynamic_memory_sequence.values[self.latest_elem_index]
        if latest_dynamic_used_shrctx / latest_max_dynamic_memory >= self.abnormal_memory_occupy_rate:
            large_dynamic_used_shrctx_detail['status'] = 'abnormal'
            large_dynamic_used_shrctx_detail['rate'] = round(latest_dynamic_used_shrctx / latest_max_dynamic_memory, 4)
            shared_context_memory_detail = list(self.memory_detail.real_time_shared_context_memory_detail.items())
            shared_context_memory_detail = [(item[0], max(item[1].values)) for item in shared_context_memory_detail]
            shared_context_memory_detail.sort(key=lambda item: item[1], reverse=True)
            for context, totalsize in shared_context_memory_detail[:self.output_context_num]:
                large_dynamic_used_shrctx_detail['data'][context] = {'totalsize': totalsize}
                memory_detail = self.memory_detail.get_shared_memctx_detail(context)
                if not memory_detail:
                    continue
                memory_detail = list(sorted(memory_detail, key=lambda item: item['size'], reverse=True))
                large_dynamic_used_shrctx_detail['data'][context]['detail'] = memory_detail[:self.output_file_num]
        return large_dynamic_used_shrctx_detail

    def large_dynamic_used_memory(self):
        # determine whether the proportion of dynamic_used_memory to max_dynamic_memory is too large
        large_dynamic_used_memory_detail = {'status': 'normal', 'rate': 0, 'remark': '', 'data': {}}
        dynamic_used_memory_sequence = self.memory_detail.real_time_total_memory_detail.get('dynamic_used_memory', EMPTY_SEQUENCE)
        max_dynamic_memory_sequence = self.memory_detail.real_time_total_memory_detail.get('max_dynamic_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_memory_sequence) or not is_sequence_valid(max_dynamic_memory_sequence):
            large_dynamic_used_memory_detail['status'] = 'unknown'
            large_dynamic_used_memory_detail['remark'] = "unable to get valid data"
            return large_dynamic_used_memory_detail
        latest_dynamic_user_memory = dynamic_used_memory_sequence.values[self.latest_elem_index]
        latest_max_dynamic_memory = max_dynamic_memory_sequence.values[self.latest_elem_index]
        if latest_dynamic_user_memory / latest_max_dynamic_memory >= self.abnormal_memory_occupy_rate:
            large_dynamic_used_memory_detail['status'] = 'abnormal'
            large_dynamic_used_memory_detail['rate'] = round(
                latest_dynamic_user_memory / latest_max_dynamic_memory, 4)
            session_context_memory_detail = list(self.memory_detail.real_time_session_context_memory_detail.items())
            session_context_memory_detail = [(item[0], max(item[1].values)) for item in session_context_memory_detail]
            session_context_memory_detail.sort(key=lambda item: item[1], reverse=True)
            for context, totalsize in session_context_memory_detail[:self.output_context_num]:
                large_dynamic_used_memory_detail['data'][context] = {'totalsize': totalsize}
                memory_detail = self.memory_detail.get_session_memctx_detail(context)
                if not memory_detail:
                    continue
                # large_dynamic_used_memory_detail['data']
                memory_detail = list(sorted(memory_detail, key=lambda item: item['size'], reverse=True))
                large_dynamic_used_memory_detail['data'][context]['detail'] = memory_detail[:self.output_file_num]
        return large_dynamic_used_memory_detail

    def large_other_used_memory(self):
        # determine whether the proportion of other_used_memory to max_process_memory is too large
        latest_other_used_memory_detail = {'status': 'normal', 'size': 0, 'remark': ''}
        other_used_memory_sequence = self.memory_detail.real_time_total_memory_detail.get('other_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(other_used_memory_sequence):
            latest_other_used_memory_detail['status'] = 'unknown'
            latest_other_used_memory_detail['remark'] = "unable to get valid data"
            return latest_other_used_memory_detail
        latest_other_used_memory = other_used_memory_sequence.values[self.latest_elem_index]
        # unit is MB
        if latest_other_used_memory >= 5 * 1024:
            latest_other_used_memory_detail['status'] = 'abnormal'
            latest_other_used_memory_detail['size'] = latest_other_used_memory
        return latest_other_used_memory_detail

    def other_used_memory_continuous_increase(self):
        # monitoring whether memory increase consistently and exceed thresholds
        other_used_memory_detail = {'status': 'normal', 'remark': '', 'data': {}}
        other_used_memory_sequence = self.memory_detail.history_total_memory_detail.\
            get('other_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(other_used_memory_sequence):
            other_used_memory_detail['status'] = 'unknown'
            other_used_memory_detail['remark'] = "unable to get valid data"
            return other_used_memory_detail
        other_used_memory_detail['data']['timestamps'] = other_used_memory_sequence.timestamps
        other_used_memory_detail['data']['values'] = other_used_memory_sequence.values
        if len(other_used_memory_sequence) >= self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector.fit_predict(other_used_memory_sequence)
            if True in increase_anomalies.values:
                other_used_memory_detail['status'] = 'abnormal'
        else:
            other_used_memory_detail['status'] = 'unknown'
            other_used_memory_detail['remark'] = "too little data for calculations to judge trend"
        return other_used_memory_detail

    def process_used_memory_continuous_increase(self):
        process_used_memory_detail = {'status': 'normal', 'remark': '', 'data': {}}
        process_used_memory_sequence = self.memory_detail.history_total_memory_detail.\
            get('process_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(process_used_memory_sequence):
            process_used_memory_detail['status'] = 'unknown'
            process_used_memory_detail['remark'] = "unable to get valid data"
            return process_used_memory_detail
        process_used_memory_detail['data']['timestamps'] = process_used_memory_sequence.timestamps
        process_used_memory_detail['data']['values'] = process_used_memory_sequence.values
        if len(process_used_memory_sequence) >= self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector.fit_predict(process_used_memory_sequence)
            if True in increase_anomalies.values:
                process_used_memory_detail['status'] = 'abnormal'
        else:
            process_used_memory_detail['status'] = 'unknown'
            process_used_memory_detail['remark'] = "too little data for calculations to judge trend"
        return process_used_memory_detail

    def dynamic_used_memory_continuous_increase(self):
        dynamic_used_memory_detail = {'status': 'normal', 'remark': '', 'data': {}}
        dynamic_used_memory_sequence = self.memory_detail.history_total_memory_detail.\
            get('dynamic_used_memory', EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_memory_sequence):
            dynamic_used_memory_detail['status'] = 'unknown'
            dynamic_used_memory_detail['remark'] = "unable to get valid data"
            return dynamic_used_memory_detail
        dynamic_used_memory_detail['data']['timestamps'] = dynamic_used_memory_sequence.timestamps
        dynamic_used_memory_detail['data']['values'] = dynamic_used_memory_sequence.values
        if len(dynamic_used_memory_sequence) >= self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector.fit_predict(dynamic_used_memory_sequence)
            if True in increase_anomalies.values:
                dynamic_used_memory_detail['status'] = 'abnormal'
        else:
            dynamic_used_memory_detail['status'] = 'unknown'
            dynamic_used_memory_detail['remark'] = "too little data for calculations to judge trend"
        return dynamic_used_memory_detail

    def dynamic_used_shrctx_continuous_increase(self):
        dynamic_used_shrctx_detail = {'status': 'normal', 'remark': '', 'data': {}}
        dynamic_used_shrctx_sequence = self.memory_detail.history_total_memory_detail.\
            get('dynamic_used_shrctx', EMPTY_SEQUENCE)
        if not is_sequence_valid(dynamic_used_shrctx_sequence):
            dynamic_used_shrctx_detail['status'] = 'unknown'
            dynamic_used_shrctx_detail['remark'] = 'unable to get valid data'
            return dynamic_used_shrctx_detail
        dynamic_used_shrctx_detail['data']['timestamps'] = dynamic_used_shrctx_sequence.timestamps
        dynamic_used_shrctx_detail['data']['values'] = dynamic_used_shrctx_sequence.values
        if len(dynamic_used_shrctx_sequence) >= self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector.fit_predict(dynamic_used_shrctx_sequence)
            if True in increase_anomalies.values:
                dynamic_used_shrctx_detail['status'] = 'abnormal'

        else:
            dynamic_used_shrctx_detail['status'] = 'unknown'
            dynamic_used_shrctx_detail['remark'] = "too little data for calculations to judge trend"
        return dynamic_used_shrctx_detail

    def topk_context_from_session_memory_continuous_increase(self):
        # monitoring topk memory context
        topk_session_memory_detail = {}
        topk_session_memory = self.memory_detail.history_session_context_memory_detail
        for context, sequence in topk_session_memory.items():
            if not is_sequence_valid(sequence):
                continue
            topk_session_memory_detail[context] = {}
            topk_session_memory_detail[context]['timestamps'] = sequence.timestamps
            topk_session_memory_detail[context]['values'] = sequence.values
            if len(sequence) < self.minimal_elem_of_series_analysis:
                topk_session_memory_detail[context]['status'] = 'unknown'
                topk_session_memory_detail[context]['remark'] = \
                    'too little data for calculations to judge trend'
            increase_anomalies = continuous_increasing_detector.fit_predict(sequence)
            if True in increase_anomalies.values:
                topk_session_memory_detail[context]['status'] = 'abnormal'
            else:
                topk_session_memory_detail[context]['status'] = 'normal'
            topk_session_memory_detail[context]['range'] = [min(sequence.values), max(sequence.values)]
            topk_session_memory_detail[context]['remark'] = ''
        return topk_session_memory_detail

    def topk_context_from_shared_memory_continuous_increase(self):
        # monitoring topk memory context
        topk_shared_memory_detail = {}
        topk_shared_memory = self.memory_detail.history_shared_context_memory_detail
        for context, sequence in topk_shared_memory.items():
            if not is_sequence_valid(sequence):
                continue
            topk_shared_memory_detail[context] = {}
            topk_shared_memory_detail[context]['timestamps'] = sequence.timestamps
            topk_shared_memory_detail[context]['values'] = sequence.values
            if len(sequence) < self.minimal_elem_of_series_analysis:
                topk_shared_memory_detail[context]['status'] = 'unknown'
                topk_shared_memory_detail[context]['remark'] = \
                    'too little data for calculations to judge trend'
            increase_anomalies = continuous_increasing_detector.fit_predict(sequence)
            if True in increase_anomalies.values:
                topk_shared_memory_detail[context]['status'] = 'abnormal'
            else:
                topk_shared_memory_detail[context]['status'] = 'normal'
            topk_shared_memory_detail[context]['range'] = [min(sequence.values), max(sequence.values)]
            topk_shared_memory_detail[context]['remark'] = ''
        return topk_shared_memory_detail

    def os_mem_usage_continuous_increase(self):
        os_mem_usage_detail = {'status': 'normal', 'remark': '', 'data': {}}
        os_mem_usage_sequence = self.memory_detail.history_mem_usage_detail
        if not is_sequence_valid(os_mem_usage_sequence):
            os_mem_usage_detail['status'] = 'unknown'
            os_mem_usage_detail['remark'] = 'unable to get valid data'
        os_mem_usage_detail['data']['timestamps'] = os_mem_usage_sequence.timestamps
        os_mem_usage_detail['data']['values'] = os_mem_usage_sequence.values
        if len(os_mem_usage_sequence) >= self.minimal_elem_of_series_analysis:
            increase_anomalies = continuous_increasing_detector.fit_predict(os_mem_usage_sequence)
            if True in increase_anomalies.values:
                os_mem_usage_detail['status'] = 'abnormal'
        else:
            os_mem_usage_detail['status'] = 'unknown'
            os_mem_usage_detail['remark'] = 'too little data for calculations to judge trend'
        return os_mem_usage_detail

    def topk_running_time_sql(self):
        return self.memory_detail.topk_running_time_sql

    def topk_session_memory_sql(self):
        return self.memory_detail.topk_session_memory_sql

    def __call__(self):
        return {'large_process_used_memory': self.large_process_used_memory(),
                'large_dynamic_used_memory': self.large_dynamic_used_memory(),
                'large_dynamic_used_shrctx': self.large_dynamic_used_shrctx(),
                'large_other_used_memory': self.large_other_used_memory(),
                'large_os_memory_usage': self.large_os_memory_usage(),
                'other_used_memory_continuous_increase': self.other_used_memory_continuous_increase(),
                'process_used_memory_continuous_increase': self.process_used_memory_continuous_increase(),
                'dynamic_used_memory_continuous_increase': self.dynamic_used_memory_continuous_increase(),
                'topk_context_from_shared_memory_continuous_increase':
                    self.topk_context_from_shared_memory_continuous_increase(),
                'dynamic_used_shrctx_continuous_increase': self.dynamic_used_shrctx_continuous_increase(),
                'os_mem_usage_continuous_increase': self.os_mem_usage_continuous_increase(),
                'topk_context_from_session_memory_continuous_increase':
                    self.topk_context_from_session_memory_continuous_increase(),
                'topk_session_memory_sql': self.topk_session_memory_sql(),
                'data_source': 'TSDB' if isinstance(self.memory_detail, GetMemoryDetailFromTSDB) else 'DRIVER'
                }


def memory_check(latest_hours, driver=None, data_source='TSDB'):
    if data_source == 'TSDB':
        end_time = int(time.time()) * 1000
        if latest_hours is None:
            start_time = end_time - ONE_DAY
        else:
            start_time = end_time - latest_hours * 60 * 60 * 1000
        instance = global_vars.agent_proxy.current_agent_addr()
        memory_detail = GetMemoryDetailFromTSDB(instance, start_time, end_time)
    else:
        memory_detail = GetMemoryDetailFromDriver(driver)
    memory_checker = MemoryChecker(memory_detail)
    return memory_checker()


def format_pretty_table(title, field_names, align='l'):
    return PrettyTable(field_names=field_names, title=title, align=align)


def format_check_output(check_item, status, rate, remark):
    if status == 'abnormal':
        color = 'red'
    elif status == 'unknown':
        color = 'yellow'
    else:
        color = 'green'
    output = f"[{check_item.upper()}]: status: {status}"
    if check_item == 'large_other_used_memory' and rate:
        output += f", size: {rate}"
    else:
        if rate != 'NULL':
            output += f", rate: {rate}"
    if remark:
        output += f", remark: {remark}"
    write_to_terminal(output, color=color)


def output_check_result(check_result):
    title = f"{'=' * 60} MEMORY CHECKING {'=' * 60}"
    write_to_terminal('\n' + title, color='green')
    # system
    large_os_memory_usage = check_result['large_os_memory_usage']
    os_mem_usage_continuous_increase = check_result['os_mem_usage_continuous_increase']
    format_check_output('large_memory_usage',
                        large_os_memory_usage['status'],
                        large_os_memory_usage['rate'],
                        large_os_memory_usage['remark'])
    format_check_output('os_mem_usage_continuous_increase',
                        os_mem_usage_continuous_increase['status'],
                        'NULL',
                        os_mem_usage_continuous_increase['remark'])
    # database

    # 1) process_used_memory
    large_process_used_memory = check_result['large_process_used_memory']
    process_used_memory_continuous_increase = check_result['process_used_memory_continuous_increase']
    format_check_output('large_process_used_memory',
                        large_process_used_memory['status'],
                        large_process_used_memory['rate'],
                        large_process_used_memory['remark'])
    format_check_output('process_used_memory_continuous_increase',
                        process_used_memory_continuous_increase['status'],
                        'NULL',
                        process_used_memory_continuous_increase['remark'])

    # 2) other_used_memory
    large_other_used_memory = check_result['large_other_used_memory']
    other_used_memory_continuous_increase = check_result['other_used_memory_continuous_increase']
    format_check_output('large_other_used_memory',
                        large_other_used_memory['status'],
                        large_other_used_memory['size'],
                        large_other_used_memory['remark'])
    format_check_output('other_used_memory_continuous_increase',
                        other_used_memory_continuous_increase['status'],
                        'NULL',
                        other_used_memory_continuous_increase['remark'])

    # 3) dynamic_used_memory
    large_dynamic_used_memory = check_result['large_dynamic_used_memory']
    dynamic_used_memory_continuous_increase = check_result['dynamic_used_memory_continuous_increase']
    format_check_output('large_dynamic_used_memory',
                        large_dynamic_used_memory['status'],
                        large_dynamic_used_memory['rate'],
                        large_dynamic_used_memory['remark'])
    for context, detail in large_dynamic_used_memory['data'].items():
        totalsize = float(detail['totalsize'])
        mctx_detail = detail.get('detail', {})
        print(f"\t{context}(totalsize: {totalsize:.4f})")
        for item in mctx_detail:
            print(f"\t\tfile: {item['file']}, line: {item['line']}, size: {float(item['size']):.4f}(KB)")
    format_check_output('dynamic_used_memory_continuous_increase',
                        dynamic_used_memory_continuous_increase['status'],
                        'NULL',
                        dynamic_used_memory_continuous_increase['remark'])

    # 4） dynamic_used_shrctx
    large_dynamic_used_shrctx = check_result['large_dynamic_used_shrctx']
    dynamic_used_shrctx_continuous_increase = check_result['dynamic_used_shrctx_continuous_increase']
    format_check_output('large_dynamic_used_shrctx',
                        large_dynamic_used_shrctx['status'],
                        large_dynamic_used_shrctx['rate'],
                        large_dynamic_used_shrctx['remark'])
    for context, detail in large_dynamic_used_shrctx['data'].items():
        totalsize = float(detail['totalsize'])
        mctx_detail = detail.get('detail', {})
        print(f"\t{context}(totalsize: {totalsize:.4f})")
        for item in mctx_detail:
            print(f"\t\tfile: {item['file']}, line: {item['line']}, size: {float(item['size']):.4f}(KB)")
    format_check_output('dynamic_used_shrctx_continuous_increase',
                        dynamic_used_shrctx_continuous_increase['status'],
                        'NULL',
                        dynamic_used_shrctx_continuous_increase['remark'])

    # 5) topk context from gs_session_memory
    topk_context_from_session_memory_continuous_increase = \
        check_result['topk_context_from_session_memory_continuous_increase']
    if len(topk_context_from_session_memory_continuous_increase) == 0:
        status = 'unknown'
    elif 'abnormal' in [item['status'] for _, item in topk_context_from_session_memory_continuous_increase.items()]:
        status = 'abnormal'
    elif 'unknown' in [item['status'] for _, item in topk_context_from_session_memory_continuous_increase.items()]:
        status = 'existing unknown'
    else:
        status = 'normal'
    format_check_output('topk_context_from_session_memory_continuous_increase', status, 'NULL', '')
    for context, detail in topk_context_from_session_memory_continuous_increase.items():
        if detail['status'] == 'unknown':
            color = 'yellow'
        elif detail['status'] == 'abnormal':
            color = 'red'
        else:
            color = None
        if detail['remark']:
            lines = f"\tcontext: {context}, status: {detail['status']}, min: {detail['range'][0]:.4f}, " \
                    f"max: {detail['range'][1]:.4f}, remark: {detail['remark']}"
        else:
            lines = f"\tcontext: {context}, status: {detail['status']}, min: {detail['range'][0]:.4f}, " \
                    f"max: {detail['range'][1]:.4f}"
        write_to_terminal(lines, color=color)

    # 6) topk context from gs_shared_memory
    topk_context_from_shared_memory_continuous_increase = \
        check_result['topk_context_from_shared_memory_continuous_increase']
    if len(topk_context_from_shared_memory_continuous_increase) == 0:
        status = 'unknown'
    elif 'abnormal' in [item['status'] for _, item in topk_context_from_shared_memory_continuous_increase.items()]:
        status = 'abnormal'
    elif 'unknown' in [item['status'] for _, item in topk_context_from_shared_memory_continuous_increase.items()]:
        status = 'existing unknown'
    else:
        status = 'normal'
    format_check_output('topk_context_from_shared_memory_continuous_increase', status, 'NULL', '')
    for context, detail in topk_context_from_shared_memory_continuous_increase.items():
        if detail['status'] == 'unknown':
            color = 'yellow'
        elif detail['status'] == 'abnormal':
            color = 'red'
        else:
            color = None
        if detail['remark']:
            lines = f"\tcontext: {context}, status: {detail['status']}, min: {detail['range'][0]:.4f}, " \
                    f"max: {detail['range'][1]:.4f}, remark: {detail['remark']}"
        else:
            lines = f"\tcontext: {context}, status: {detail['status']}, min: {detail['range'][0]:.4f}, " \
                    f"max: {detail['range'][1]:.4f}"
        write_to_terminal(lines, color=color)

    # 7) topk SQL order by context totalsize
    title = f"{'=' * 60} TOPK SQL ORDER BY MEMORY SIZE {'=' * 60}"
    write_to_terminal(title, color='green')

    topk_session_memory_sql = check_result['topk_session_memory_sql']
    session_memory_sql = format_pretty_table(None,
                                            ('pid', 'application_name', 'query_start', 'running_time', 'context',
                                             'totalsize', 'usedsize', 'freesize', 'query'))
    for item in topk_session_memory_sql:
        session_memory_sql.add_row((item['pid'], item['application_name'], item['query_start'], item['running_time'],
                                    item['contextname'], item['totalsize'], item['usedsize'], item['freesize'],
                                    item['query']))
    print(session_memory_sql)


def main(argv):
    parser = argparse.ArgumentParser(description='Memory Checker: Discover potential risks in memory.')
    parser.add_argument('action', choices=('check',),
                        help='choose a functionality to perform')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=path_type,
                        help='Set the directory of configuration files')
    parser.add_argument('--hours', metavar='HOURS', type=date_type,
                        help='Set the latest time for memory checking')
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
        if args.data_source == 'DRIVER':
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
            result = memory_check(args.hours, driver=args.driver, data_source=args.data_source)
            output_check_result(check_result=result)
    except Exception as e:
        write_to_terminal('An error occurred probably due to database operations, '
                          'please check database configurations. For details:\n' +
                          str(e), color='red', level='error')
        traceback.print_tb(e.__traceback__)
        return 2


if __name__ == '__main__':
    main(sys.argv[1:])
