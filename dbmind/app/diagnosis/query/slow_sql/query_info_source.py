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
import logging
import re
from datetime import datetime
from functools import wraps
from typing import List

from dbmind import global_vars
from dbmind.app.optimization.index_recommendation import rpc_index_advise
from dbmind.app.optimization.index_recommendation_rpc_executor import RpcExecutor
from dbmind.common.parser import plan_parsing
from dbmind.common.parser.sql_parsing import fill_value, standardize_sql
from dbmind.common.parser.sql_parsing import get_generate_prepare_sqls_function
from dbmind.common.parser.sql_parsing import remove_parameter_part, is_query_normalized
from dbmind.common.parser.sql_parsing import replace_question_mark_with_value, replace_question_mark_with_dollar
from dbmind.common.types import Sequence
from dbmind.common.utils import ExceptionCatcher
from dbmind.components.sql_rewriter.sql_rewriter import rewrite_sql_api
from dbmind.service import dai
from dbmind.service.dai import is_sequence_valid, is_driver_result_valid

exception_catcher = ExceptionCatcher(strategy='raise', name='SLOW QUERY')
DEFAULT_FETCH_INTERVAL = 15


def exception_follower(output=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.exception(
                    "Function %s execution error: %s", func.__name__, e
                )
                if callable(output):
                    return output()
                return output

        return wrapper

    return decorator


REQUIRED_PARAMETERS = ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'synchronous_commit',
                       'max_process_memory', 'enable_nestloop', 'enable_hashjoin', 'random_page_cost',
                       'enable_mergejoin', 'enable_indexscan', 'enable_hashagg', 'enable_sort',
                       'skew_option', 'block_size', 'recovery_min_apply_delay', 'max_connections',
                       'job_queue_processes')


class TableStructure:
    """Data structure to save table structure, contains the main information of the table structure such as
    database address, database name, schema name, table name, dead tuples, etc
    """

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.db_name = None
        self.schema_name = None
        self.table_name = None
        self.dead_tuples = 0
        self.live_tuples = 0
        self.data_changed_delay = -1
        self.dead_rate = 0.0
        self.tuples_diff = 0
        # How to get field num for table
        self.column_number = 0
        # This field can be used as a flag for whether the statistics are updated.
        self.vacuum_delay = -1
        self.analyze_delay = -1
        self.table_size = 0
        self.index_size = 0
        self.index = {}
        # Note: for the distributed database version, the following two indicators are meaningful
        self.skew_ratio = 0.0
        self.skew_stddev = 0.0


class LockInfo:
    """Data structure to save lock information such as database information, locker_query and locked_query, etc"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.locked_query = None
        self.locked_query_start = None
        self.locker_query = None
        self.locker_query_start = None


class DatabaseInfo:
    """Data structure to save database information such as database address and TPS, connection"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.history_tps = 1
        self.current_tps = 1
        self.current_connection = 1
        self.thread_pool = {}


class SystemInfo:
    """Data structure to save system information such as database address, IOWAIT, IOCAPACITY, CPU_USAGE, etc"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.iops = 0
        self.db_iops = 0
        self.ioutils = {}
        self.iocapacity = 0
        self.db_iocapacity = 0
        self.iowait = 0.0
        self.total_memory = 0.0
        self.gaussdb_number = 0
        self.cpu_core_number = 1
        self.system_cpu_usage = 0
        self.system_mem_usage = 0
        self.db_cpu_usage = 0
        self.db_mem_usage = 0
        self.db_data_occupy_rate = 0.0
        self.disk_usage = {}
        self.load_average1 = 0
        self.db_process_fds_rate = 0
        self.process_fds_rate = 0
        self.io_read_delay = {}
        self.io_write_delay = {}
        self.io_queue_number = {}


class PgSetting:
    """Data structure to save GUC Parameter"""

    def __init__(self):
        self.name = None
        self.setting = None
        self.unit = None
        self.min_val = None
        self.max_val = None
        self.vartype = None
        self.boot_val = None


class NetWorkInfo:
    """Data structure to save server network metrics"""

    def __init__(self):
        self.name = None
        self.receive_packets = 1.0
        self.transmit_packets = 1.0
        self.receive_drop = 0.0
        self.transmit_drop = 0.0
        self.transmit_error = 0.0
        self.receive_error = 0.0
        self.receive_bytes = 1.0
        self.transmit_bytes = 1.0


class Index:
    def __init__(self):
        self.db_name = None
        self.schema_name = None
        self.table_name = None
        self.column_name = None
        self.index_name = None
        self.index_type = None

    def __repr__(self):
        return "(schema: %s, index: %s(%s), index_type: %s)" % (
            self.schema_name, self.table_name, self.column_name, self.index_type
        )


class TimedTask:
    def __init__(self):
        self.job_id = None
        self.priv_user = None
        self.dbname = None
        self.job_status = None
        self.last_start_date = 0
        self.last_end_date = 0
        self.failure_count = 0


class WaitEvent:
    def __init__(self):
        self.type = None
        self.event = None
        self.wait = 0
        self.failed_wait = 0
        self.total_wait_time = 0
        self.last_updated = 0


class ThreadInfo:
    def __init__(self):
        self.type = None
        self.event = None
        self.wait_status = None
        self.last_updated = 0
        self.sessionid = None
        self.thread_id = None
        self.block_sessionid = None
        self.lockmode = None


class Process:
    def __init__(self):
        self.name = ''
        self.cpu_usage = ''
        self.mem_usage = ''
        self.fds = 0


class QueryContext:
    def __init__(self, slow_sql_instance):
        self.slow_sql_instance = slow_sql_instance
        self.is_sql_valid = True

    @exception_catcher
    def acquire_plan_parse(self):
        if self.slow_sql_instance.query_plan is not None:
            plan_parse = plan_parsing.Plan()
            plan_parse.parse(self.slow_sql_instance.query_plan)
            return plan_parse


def parse_field_from_indexdef(indexdef):
    if indexdef is None or not len(indexdef):
        return []
    pattern = re.compile(r'CREATE INDEX \w+ ON (?:\w+\.)?\w+ USING (?:btree|hash) \((.+)?\) TABLESPACE \w+')
    fields = pattern.match(indexdef)
    if fields:
        fields = [item.strip() for item in fields.groups()[0].split(',')]
        return fields
    return []


def _get_sequence_max_value(s: Sequence, precision=0):
    """
    Get the max value of sequence.
    precision: 0 means casting to integer,
               positive value means float with the precision,
               negative value means nothing to do.
    """
    if precision == 0:
        return int(max(s.values))
    elif precision > 0:
        return round(float(max(s.values)), precision)
    else:
        return max(s.values)


def _get_sequence_first_value(s: Sequence, precision=0):
    """
    Get the first value of sequence.
    precision: 0 means casting to integer,
               positive value means float with the precision,
               negative value means nothing to do.
    """
    if precision == 0:
        return int(s.values[0])
    elif precision > 0:
        return round(float(s.values[0]), precision)
    else:
        return s.values[0]


def _get_sequences_sum_value(seqs: List[Sequence], precision=0):
    """
    Get the sum value of sequence.
    precision: 0 means casting to integer,
               positive value means float with the precision,
               negative value means nothing to do.
    """
    value = 0
    for s in seqs:
        if is_sequence_valid(s):
            value += _get_sequence_first_value(s, precision=precision)
    return value


def _get_driver_value(rows, key, default=0):
    """
    Get the execution result of the driver.
    Note: the result format: '[RealDictRow([('relname', 't1'), ('n_live_tup', 3000076)]), RealDictRow(...), ...]'
    """
    if len(rows) == 1:
        return rows[0].get(key, default)
    else:
        return [item.get(key, default) for item in rows]


class QueryContextFromTSDBAndRPC(QueryContext):
    """The object of slow query data processing factory"""

    def __init__(self, slow_sql_instance, **kwargs):
        """
        :param slow_sql_instance: The instance of slow query
        :param default_fetch_interval: fetch interval of data source
        :param expansion_factor: Ensure that the time expansion rate of the data can be collected
        """
        super().__init__(slow_sql_instance)
        self.query_type = 'raw'
        self.fetch_interval = self.acquire_fetch_interval()
        self.expansion_factor = kwargs.get('expansion_factor', 3)
        self.query_start_time = datetime.fromtimestamp(self.slow_sql_instance.start_at / 1000)
        if self.slow_sql_instance.duration_time / 1000 >= self.fetch_interval:
            self.query_end_time = datetime.fromtimestamp(
                self.slow_sql_instance.start_at / 1000 + self.slow_sql_instance.duration_time / 1000
            )
        else:
            self.query_end_time = datetime.fromtimestamp(
                self.slow_sql_instance.start_at / 1000 + int(self.expansion_factor * self.acquire_fetch_interval())
            )
        logging.debug('[SLOW QUERY] fetch start time: %s, fetch end time: %s', self.query_start_time,
                      self.query_end_time)
        logging.debug('[SLOW QUERY] fetch interval: %s', self.fetch_interval)
        self.standard_query = self.slow_sql_instance.query
        if self.slow_sql_instance.track_parameter:
            self.standard_query = fill_value(self.slow_sql_instance.query)
            self.slow_sql_instance.query = remove_parameter_part(slow_sql_instance.query)
        if is_query_normalized(self.standard_query):
            self.query_type = 'normalized'
        self.standard_query = standardize_sql(self.standard_query)
        if self.slow_sql_instance.query_plan is None:
            self.slow_sql_instance.query_plan = self.acquire_plan(self.standard_query)
            if self.slow_sql_instance.query_plan is None:
                self.is_sql_valid = False

    @exception_follower(output=None)
    @exception_catcher
    def acquire_plan(self, query):
        query_plan = ''
        if self.query_type == 'normalized':
            query = replace_question_mark_with_value(query)
            query = replace_question_mark_with_dollar(query)
            stmts = "set current_schema='%s';" % self.slow_sql_instance.schema_name + ';'.join(
                get_generate_prepare_sqls_function()(query))
            rows = global_vars.agent_proxy.call('query_in_database',
                                                stmts,
                                                self.slow_sql_instance.db_name,
                                                return_tuples=False,
                                                fetch_all=True)
            if len(rows) == 4 and rows[-2]:
                rows = rows[-2]
        else:
            stmts = "set current_schema='%s';explain %s" % (self.slow_sql_instance.schema_name,
                                                            query)
            rows = global_vars.agent_proxy.call('query_in_database',
                                                stmts,
                                                self.slow_sql_instance.db_name,
                                                return_tuples=False,
                                                fetch_all=False)
        for row in rows:
            if not row:
                continue
            query_plan += row.get('QUERY PLAN') + '\n'
        if not query_plan:
            logging.warning("The plan is not fetched for query: %s", query)
            return
        return query_plan

    @exception_follower(output=15)
    @exception_catcher
    def acquire_fetch_interval(self) -> int:
        """Get data source collection frequency"""
        sequence = dai.get_latest_metric_value("prometheus_target_interval_length_seconds").filter(
            quantile="0.99").fetchone()
        if is_sequence_valid(sequence):
            self.fetch_interval = int(sequence.values[0])
        else:
            return DEFAULT_FETCH_INTERVAL
        return self.fetch_interval

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_sort_condition(self):
        """
        Determine whether SQL has been spilled to disk during execution.
        Judging by gaussdb_statement_sort_spill and gaussdb_statement_hash_spill in reprocessing_exporter,
        which coming from dbe_perf.statement.
        This method is not friendly for interactive.
        """
        query = self.slow_sql_instance.query.replace("\'", "\'\'").lower()
        stmts = """
            select sort_spill_count / n_calls as sort_spill_count, hash_spill_count / n_calls as 
            hash_spill_count from dbe_perf.statement where lower(query) = '%s';
        """ % query
        sort_condition = {'sort_spill': False, 'hash_spill': False}
        sort_spill_sequence = dai.get_metric_sequence("gaussdb_statement_sort_spill", self.query_start_time,
                                                      self.query_end_time).from_server(
            self.slow_sql_instance.instance).filter(
            query=f"{self.slow_sql_instance.query}").fetchone()
        hash_spill_sequence = dai.get_metric_sequence("gaussdb_statement_hash_spill", self.query_start_time,
                                                      self.query_end_time).from_server(
            self.slow_sql_instance.instance).filter(
            query=f"{self.slow_sql_instance.query}").fetchone()
        if is_sequence_valid(sort_spill_sequence) and max(int(item) for item in sort_spill_sequence.values) > 0:
            sort_condition['sort_spill'] = True
        if is_sequence_valid(hash_spill_sequence) and max(int(item) for item in hash_spill_sequence.values) > 0:
            sort_condition['hash_spill'] = True
        rows = global_vars.agent_proxy.call('query_in_database',
                                            stmts,
                                            'postgres',
                                            return_tuples=False)
        if is_driver_result_valid(rows):
            self.slow_sql_instance.sort_spill_count = _get_driver_value(rows, 'sort_spill_count')
            self.slow_sql_instance.hash_spill_count = _get_driver_value(rows, 'hash_spill_count')
        return sort_condition

    @exception_follower(output=LockInfo)
    @exception_catcher
    def acquire_lock_info(self) -> LockInfo:
        """Get lock information during slow SQL execution."""
        blocks_info = LockInfo()
        lock_sequence = dai.get_metric_sequence("pg_lock_sql_locked_times", self.query_start_time,
                                                self.query_end_time).from_server(
            self.slow_sql_instance.instance).filter(
            locked_query=f"{self.slow_sql_instance.query}").fetchone()
        if is_sequence_valid(lock_sequence):
            blocks_info.locker_query = lock_sequence.labels.get('locker_query', 'Unknown')
        return blocks_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_tables_structure_info(self) -> list:
        """Acquire table structure information related to slow query"""
        table_structure = []
        if not self.slow_sql_instance.tables_name:
            return table_structure
        tuples_statistics_stmt = """
            select abs(r1.n_live_tup - r2.reltuples)::int diff from pg_stat_user_tables r1, pg_class r2 
            where r1.schemaname = '{schemaname}' and r2.relname = '{relname}' and r1.relname = r2.relname;
        """
        user_table_stmt = """
            SELECT n_live_tup, n_dead_tup,
                   round(n_dead_tup / (n_live_tup + 1), 2) as dead_rate,
                   case when (last_vacuum is null) then -1 else 
                   extract(epoch from pg_catalog.now() - last_vacuum)::bigint end as vacuum_delay,
                   case when (last_analyze is null) then -1 else 
                   extract(epoch from pg_catalog.now() - last_analyze)::bigint end as analyze_delay, 
                   case when (last_data_changed is null) then -1 else 
                   extract(epoch from pg_catalog.now() - last_data_changed)::bigint end as data_changed_delay 
            FROM pg_stat_user_tables where schemaname = '{schemaname}' and relname = '{relname}'
        """
        pg_index_stmt = """
            select indexrelname, pg_get_indexdef(indexrelid) as indexdef from 
            pg_stat_user_indexes  where schemaname = '{schemaname}' and relname ='{relname}';
        """
        table_size_stmt = """
        select pg_catalog.pg_total_relation_size(rel.oid) / 1024 / 1024 AS mbytes 
                          FROM pg_namespace nsp JOIN pg_class rel ON nsp.oid = rel.relnamespace  
                          WHERE nsp.nspname = '{schemaname}' AND rel.relname = '{relname}'; 
        """
        for schema_name, tables_name in self.slow_sql_instance.tables_name.items():
            for table_name in tables_name:
                table_info = TableStructure()
                table_info.db_host = self.slow_sql_instance.db_host
                table_info.db_port = self.slow_sql_instance.db_port
                table_info.db_name = self.slow_sql_instance.db_name
                table_info.schema_name = schema_name
                table_info.table_name = table_name
                dead_rate_info = dai.get_metric_sequence("pg_tables_structure_dead_rate",
                                                         self.query_start_time,
                                                         self.query_end_time).from_server(
                    self.slow_sql_instance.instance).filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                if is_sequence_valid(dead_rate_info):
                    table_info.dead_rate = _get_sequence_first_value(dead_rate_info, precision=4)
                    live_tup_info = dai.get_metric_sequence("pg_tables_structure_n_live_tup",
                                                            self.query_start_time,
                                                            self.query_end_time).from_server(
                        self.slow_sql_instance.instance).filter(
                        datname=f"{self.slow_sql_instance.db_name}").filter(
                        schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                    dead_tup_info = dai.get_metric_sequence("pg_tables_structure_n_dead_tup",
                                                            self.query_start_time,
                                                            self.query_end_time).from_server(
                        self.slow_sql_instance.instance).filter(
                        datname=f"{self.slow_sql_instance.db_name}").filter(
                        schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                    vacuum_delay_info = dai.get_metric_sequence("pg_tables_structure_vacuum_delay",
                                                                self.query_start_time,
                                                                self.query_end_time).from_server(
                        self.slow_sql_instance.instance).filter(
                        datname=f"{self.slow_sql_instance.db_name}").filter(
                        schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                    last_data_changed_delay_info = dai.get_metric_sequence(
                        "pg_tables_structure_last_data_changed_delay",
                        self.query_start_time,
                        self.query_end_time).from_server(
                        self.slow_sql_instance.instance).filter(
                        datname=f"{self.slow_sql_instance.db_name}").filter(
                        schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                    analyze_delay_info = dai.get_metric_sequence("pg_tables_structure_analyze_delay",
                                                                 self.query_start_time,
                                                                 self.query_end_time).from_server(
                        self.slow_sql_instance.instance).filter(
                        datname=f"{self.slow_sql_instance.db_name}").filter(
                        schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                    if is_sequence_valid(live_tup_info):
                        table_info.live_tuples = _get_sequence_first_value(live_tup_info)
                    if is_sequence_valid(dead_tup_info):
                        table_info.dead_tuples = _get_sequence_first_value(dead_tup_info)
                    if is_sequence_valid(dead_rate_info):
                        table_info.dead_rate = _get_sequence_first_value(dead_rate_info, precision=4)
                    if is_sequence_valid(analyze_delay_info):
                        table_info.analyze_delay = _get_sequence_first_value(analyze_delay_info)
                    if is_sequence_valid(last_data_changed_delay_info):
                        table_info.data_changed_delay = _get_sequence_first_value(last_data_changed_delay_info)
                    if is_sequence_valid(vacuum_delay_info):
                        table_info.vacuum_delay = _get_sequence_first_value(vacuum_delay_info)
                else:
                    tuples_statistics_rows = global_vars.agent_proxy.call('query_in_database',
                                                                          tuples_statistics_stmt.
                                                                          format(schemaname=schema_name,
                                                                                 relname=table_name),
                                                                          self.slow_sql_instance.db_name,
                                                                          return_tuples=False)
                    user_table_rows = global_vars.agent_proxy.call('query_in_database',
                                                                   user_table_stmt.format(schemaname=schema_name,
                                                                                          relname=table_name),
                                                                   self.slow_sql_instance.db_name,
                                                                   return_tuples=False)
                    if is_driver_result_valid(tuples_statistics_rows):
                        table_info.tuples_diff = tuples_statistics_rows.get('diff', 0)
                    if is_driver_result_valid(user_table_rows):
                        table_info.live_tuples = _get_driver_value(user_table_rows, 'n_live_tup')
                        table_info.dead_tuples = _get_driver_value(user_table_rows, 'n_dead_tup')
                        table_info.dead_rate = _get_driver_value(user_table_rows, 'dead_rate')
                        table_info.analyze_delay = _get_driver_value(user_table_rows, 'analyze_delay', default=-1)
                        table_info.vacuum_delay = _get_driver_value(user_table_rows, 'vacuum_delay', default=-1)
                        table_info.data_changed_delay = _get_driver_value(user_table_rows,
                                                                          'data_changed_delay', default=-1)
                pg_table_size_info = dai.get_metric_sequence("pg_tables_size_totalsize", self.query_start_time,
                                                             self.query_end_time).from_server(
                    self.slow_sql_instance.instance).filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                if is_sequence_valid(pg_table_size_info):
                    table_info.table_size = _get_sequence_max_value(pg_table_size_info, precision=4)
                else:
                    table_size_rows = global_vars.agent_proxy.call('query_in_database',
                                                                   table_size_stmt.format(schemaname=schema_name,
                                                                                          relname=table_name),
                                                                   self.slow_sql_instance.db_name,
                                                                   return_tuples=False)
                    if is_driver_result_valid(table_size_rows):
                        table_info.table_size = _get_driver_value(table_size_rows, 'mbytes')
                index_number_info = dai.get_metric_sequence("pg_index_idx_scan", self.query_start_time,
                                                            self.query_end_time).from_server(
                    self.slow_sql_instance.instance).filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(tablename=f"{table_name}").fetchall()
                if is_sequence_valid(index_number_info):
                    table_info.index = {item.labels['relname']: parse_field_from_indexdef(item.labels['indexdef'])
                                        for item in index_number_info if item.labels}
                else:
                    pg_index_rows = global_vars.agent_proxy.call('query_in_database',
                                                                 pg_index_stmt.format(schemaname=schema_name,
                                                                                      relname=table_name),
                                                                 self.slow_sql_instance.db_name,
                                                                 return_tuples=False)
                    for row in pg_index_rows:
                        table_info.index[row.get('indexrelname')] = parse_field_from_indexdef(row.get('indexdef'))
                table_structure.append(table_info)
        return table_structure

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_pg_settings(self) -> dict:
        pg_settings = {}
        for parameter in REQUIRED_PARAMETERS:
            pg_setting = PgSetting()
            sequence = dai.get_metric_sequence("pg_settings_setting", self.query_start_time,
                                               self.query_end_time).from_server(
                self.slow_sql_instance.instance).filter(
                name=f"{parameter}").fetchone()
            if is_sequence_valid(sequence):
                pg_setting.name = parameter
                pg_setting.vartype = sequence.labels['vartype']
                if pg_setting.vartype in ('integer', 'int64'):
                    pg_setting.setting = _get_sequence_first_value(sequence)
                elif pg_setting.vartype == 'bool':
                    pg_setting.setting = 1 if pg_setting.setting == 'on' else 0
                elif pg_setting.vartype == 'real':
                    pg_setting.setting = _get_sequence_first_value(sequence, precision=3)
                else:
                    pg_setting.setting = _get_sequence_first_value(sequence, precision=-1)
            pg_settings[parameter] = pg_setting
        return pg_settings

    @exception_follower(output=DatabaseInfo)
    @exception_catcher
    def acquire_database_info(self) -> DatabaseInfo:
        """Acquire table database information related to slow query"""
        database_info = DatabaseInfo()
        used_connection_sequences = dai.get_metric_sequence("pg_stat_activity_count", self.query_start_time,
                                                            self.query_end_time).from_server(
            self.slow_sql_instance.instance).fetchall()
        cur_tps_sequences = dai.get_metric_sequence("gaussdb_qps_by_instance", self.query_start_time,
                                                    self.query_end_time).from_server(
            self.slow_sql_instance.instance).fetchone()
        if is_sequence_valid(used_connection_sequences):
            database_info.current_connection = _get_sequences_sum_value(used_connection_sequences)
        else:
            used_connections_stmt = "select count(1) as used_conn from pg_stat_activity;"
            used_connections_rows = global_vars.agent_proxy.call('query_in_database',
                                                                 used_connections_stmt,
                                                                 self.slow_sql_instance.db_name,
                                                                 return_tuples=False)
            if is_driver_result_valid(used_connections_rows):
                database_info.current_connection = _get_driver_value(used_connections_rows, 'used_conn')
        if is_sequence_valid(cur_tps_sequences):
            database_info.current_tps = cur_tps_sequences.values
        return database_info

    @exception_follower(output=ThreadInfo)
    @exception_catcher
    def acquire_thread_info(self) -> ThreadInfo:
        """Acquire database thread info"""
        stmt = """
        select event, wait_status, block_sessionid, lockmode, locktag from gs_asp where 
        gs_asp.query_id={debug_query_id};
        """.format(debug_query_id=self.slow_sql_instance.query_id)
        rows = global_vars.agent_proxy.call('query_in_database',
                                            stmt,
                                            'postgres',
                                            return_tuples=False)
        thread_info = ThreadInfo()
        if is_driver_result_valid(rows):
            thread_info.event = _get_driver_value(rows, 'event')
            thread_info.wait_status = _get_driver_value(rows, 'wait_status')
            thread_info.block_sessionid = _get_driver_value(rows, 'block_sessionid')
            thread_info.lockmode = _get_driver_value(rows, 'lock_mode')
            thread_info.locktag = _get_driver_value(rows, 'locktag')
        return thread_info

    @exception_follower(output=SystemInfo)
    @exception_catcher
    def acquire_system_info(self) -> SystemInfo:
        """Acquire system information on the database server """
        system_info = SystemInfo()
        iops_info = dai.get_metric_sequence("os_disk_iops", self.query_start_time, self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        ioutils_info = dai.get_metric_sequence("os_disk_ioutils", self.query_start_time,
                                               self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        disk_usage_info = dai.get_metric_sequence("os_disk_usage", self.query_start_time,
                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        cpu_usage_info = dai.get_metric_sequence("os_cpu_usage", self.query_start_time,
                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        mem_usage_info = dai.get_metric_sequence("os_mem_usage", self.query_start_time,
                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        process_fds_rate_info = dai.get_metric_sequence("os_process_fds_rate", self.query_start_time,
                                                        self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        cpu_process_number_info = dai.get_metric_sequence("os_cpu_processor_number", self.query_start_time,
                                                          self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        db_cpu_usage_info = dai.get_metric_sequence("gaussdb_progress_cpu_usage", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        db_mem_usage_info = dai.get_metric_sequence("gaussdb_progress_mem_usage", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        if is_sequence_valid(iops_info):
            system_info.iops = _get_sequence_max_value(iops_info)
        if is_sequence_valid(process_fds_rate_info):
            system_info.process_fds_rate = _get_sequence_max_value(process_fds_rate_info, precision=4)
        if is_sequence_valid(cpu_process_number_info):
            system_info.cpu_core_number = _get_sequence_max_value(cpu_process_number_info)
        if is_sequence_valid(ioutils_info):
            ioutils_dict = {item.labels['device']: _get_sequence_max_value(item, precision=4)
                            for item in ioutils_info if item.labels}
            system_info.ioutils = ioutils_dict
        if is_sequence_valid(disk_usage_info):
            disk_usage_dict = {item.labels['device']: _get_sequence_max_value(item, precision=4)
                               for item in disk_usage_info if item.labels}
            system_info.disk_usage = disk_usage_dict
        # Add the consumption of all gaussdb related processes as the total consumption,
        # it is suitable for situations where only one instance exists on a machine, db_mem_usage is the same as him.
        if is_sequence_valid(db_cpu_usage_info):
            system_info.db_cpu_usage = _get_sequences_sum_value(db_cpu_usage_info, precision=4) / \
                                       (system_info.cpu_core_number * 100)
        if is_sequence_valid(db_mem_usage_info):
            system_info.db_mem_usage = _get_sequences_sum_value(db_mem_usage_info, precision=4)
        if is_sequence_valid(cpu_usage_info):
            system_info.system_cpu_usage = _get_sequence_first_value(cpu_usage_info, precision=4) - \
                                           system_info.db_cpu_usage
        if is_sequence_valid(mem_usage_info):
            system_info.system_mem_usage = _get_sequence_first_value(mem_usage_info, precision=4) - \
                                           system_info.db_mem_usage
        return system_info

    @exception_follower(output=NetWorkInfo)
    @exception_catcher
    def acquire_network_info(self) -> NetWorkInfo:
        network_info = NetWorkInfo()
        node_network_receive_drop_info = dai.get_metric_sequence('os_network_receive_drop',
                                                                 self.query_start_time,
                                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_transmit_drop_info = dai.get_metric_sequence('os_network_transmit_drop',
                                                                  self.query_start_time,
                                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        if is_sequence_valid(node_network_receive_drop_info):
            network_info.receive_drop = _get_sequence_first_value(node_network_receive_drop_info, precision=4)
        if is_sequence_valid(node_network_transmit_drop_info):
            network_info.transmit_drop = _get_sequence_first_value(node_network_transmit_drop_info, precision=4)

        return network_info

    @exception_follower(output=str)
    @exception_catcher
    def acquire_rewritten_sql(self) -> str:
        if not self.standard_query.upper().startswith('SELECT'):
            return ''
        rewritten_flags = []
        query = self.standard_query
        if self.query_type == 'normalized':
            query = replace_question_mark_with_value(query)
            query = replace_question_mark_with_dollar(query)
        rewritten_sql = rewrite_sql_api(self.slow_sql_instance.db_name,
                                            query,
                                            rewritten_flags=rewritten_flags,
                                            if_format=False)
        flag = rewritten_flags[0]
        if not flag:
            return ''
        rewritten_sql = rewritten_sql.replace('\n', ' ')
        rewritten_sql_plan = self.acquire_plan(rewritten_sql)
        if rewritten_sql_plan is None:
            return ''
        old_sql_plan_parse = plan_parsing.Plan()
        rewritten_sql_plan_parse = plan_parsing.Plan()
        # Abandon the rewrite if the rewritten statement does not perform as well as the original statement.
        old_sql_plan_parse.parse(self.slow_sql_instance.query_plan)
        rewritten_sql_plan_parse.parse(rewritten_sql_plan)
        if old_sql_plan_parse.root_node.total_cost > rewritten_sql_plan_parse.root_node.total_cost:
            return rewritten_sql
        return ''

    @exception_follower(output=tuple)
    @exception_catcher
    def acquire_index_analysis_info(self) -> tuple:
        recommend_indexes, redundant_indexes = [], []
        query = self.standard_query
        if self.query_type == 'normalized':
            query = replace_question_mark_with_value(query)
            query = replace_question_mark_with_dollar(query)
        executor = RpcExecutor(self.slow_sql_instance.db_name, None, None, None, None,
                               self.slow_sql_instance.schema_name)
        template = {query: {'cnt': 1, 'samples': [query]}}
        result = rpc_index_advise(executor, template)
        if result.get('recommendIndexes'):
            for recommend_index_detail in result.get('recommendIndexes'):
                recommend_index = Index()
                recommend_index.schema_name = recommend_index_detail.get('schemaName')
                recommend_index.table_name = recommend_index_detail.get('tbName')
                recommend_index.column_name = recommend_index_detail.get('columns')
                recommend_index.index_type = recommend_index_detail.get('index_type')
                recommend_indexes.append(str(recommend_index))
        if result.get('uselessIndexes'):
            for unless_index in result.get('uselessIndexes'):
                if unless_index.get('schemaName') in self.slow_sql_instance.tables_name and \
                        unless_index.get('tbName') in \
                        self.slow_sql_instance.tables_name.get(unless_index.get('schemaName')):
                    redundant_index = Index()
                    redundant_index.schema_name = unless_index.get('schemaName')
                    redundant_index.table_name = unless_index.get('tbName')
                    redundant_index.column_name = unless_index.get('columns')
                    redundant_index.index_type = unless_index.get('index_type')
                    redundant_indexes.append(str(redundant_index))
        return ';'.join(recommend_indexes), ';'.join(redundant_indexes)

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_unused_index(self):
        """Real-time access to unused indexes based on RPC"""
        unused_index = {}
        stmt = """
            select pi.indexrelname from pg_indexes pis
            join pg_stat_user_indexes pi
            on pis.schemaname = pi.schemaname and pis.tablename = pi.relname and pis.indexname = pi.indexrelname
            left join pg_constraint pco
            on pco.conname = pi.indexrelname and pco.conrelid = pi.relid
            where pco.contype is distinct from 'p' and pco.contype is distinct from 'u'
            and (idx_scan,idx_tup_read,idx_tup_fetch) = (0,0,0)
            and pis.indexdef !~ ' UNIQUE INDEX '
            and pis.schemaname = '{schemaname}' and relname = '{relname}'
        """
        if not self.slow_sql_instance.tables_name:
            return unused_index
        for schema_name, tables_name in self.slow_sql_instance.tables_name.items():
            for table_name in tables_name:
                sql = stmt.format(schemaname=schema_name, relname=table_name)
                rows = global_vars.agent_proxy.call('query_in_database',
                                                    sql,
                                                    self.slow_sql_instance.db_name,
                                                    return_tuples=False)
                for row in rows:
                    if not row.get('indexrelname'):
                        continue
                    key = "%s:%s" % (schema_name, table_name)
                    if key not in unused_index:
                        unused_index[key] = []
                    unused_index[key].append(row.get('indexrelname'))
        return unused_index


class QueryContextFromDriver(QueryContext):
    def __init__(self, slow_sql_instance, **kwargs):
        super().__init__(slow_sql_instance)
        self.query_type = 'raw'
        self.standard_query = self.slow_sql_instance.query
        self.driver = kwargs.get('driver')
        self.standard_query = self.slow_sql_instance.query
        if self.slow_sql_instance.track_parameter:
            self.standard_query = fill_value(self.slow_sql_instance.query)
            self.slow_sql_instance.query = remove_parameter_part(slow_sql_instance.query)
        if is_query_normalized(self.standard_query):
            self.query_type = 'normalized'
        self.standard_query = standardize_sql(self.standard_query)
        if self.slow_sql_instance.query_plan is None:
            self.slow_sql_instance.query_plan = self.acquire_plan(self.standard_query)
            if self.slow_sql_instance.query_plan is None:
                self.is_sql_valid = False

    @exception_follower(output=None)
    @exception_catcher
    def acquire_plan(self, query):
        query_plan = ''
        if self.query_type == 'normalized':
            query = replace_question_mark_with_value(query)
            query = replace_question_mark_with_dollar(query)
            stmts = "set current_schema='%s';" % self.slow_sql_instance.schema_name + ';'.join(
                get_generate_prepare_sqls_function()(query))
            rows = self.driver.query(stmts, return_tuples=True, fetch_all=True)
            if len(rows) == 4 and rows[-2]:
                rows = rows[-2]
        else:
            stmts = "set current_schema='%s';explain %s" % (self.slow_sql_instance.schema_name, query)
            rows = self.driver.query(stmts, return_tuples=True)
        for row in rows:
            if not row:
                continue
            query_plan += row[0] + '\n'
        if not query_plan:
            logging.warning("The plan is not fetched for query: %s", query)
            return
        return query_plan

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_sort_condition(self):
        """
        Detect whether there is a possible disk spill behavior.
        """
        query = self.slow_sql_instance.query.replace('\'', '\'\'').lower()
        sort_condition = {'sort_spill': False, 'hash_spill': False}
        stmt = """
            select sort_spill_count / n_calls as sort_spill_count, hash_spill_count / n_calls as 
            hash_spill_count from dbe_perf.statement where lower(query) = '%s';
        """ % query
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            self.slow_sql_instance.sort_spill_count = _get_driver_value(rows, 'sort_spill_count')
            self.slow_sql_instance.hash_spill_count = _get_driver_value(rows, 'hash_spill_count')
        return sort_condition

    @exception_follower(output=LockInfo)
    @exception_catcher
    def acquire_lock_info(self):
        blocks_info = LockInfo()
        query = self.slow_sql_instance.query.replace('\'', '\'\'')
        stmt = """
            select distinct locker.pid as locker_pid, 
                    locked.pid as locked_pid, 
                    locker_act.query as locker_query,  
                    locked_act.query as locked_query  
                    from pg_locks locked,  
                    pg_locks locker,  
                    pg_stat_activity locked_act,  
                    pg_stat_activity locker_act  
                    where locker.granted=true  
                    and locked.granted=false  
                    and locked.pid=locked_act.pid  
                    and locker.pid=locker_act.pid  
                    and locked_act.query = '%s'  
                    and locker.pid <> locked.pid  
                    and locker.mode not like 'AccessShareLock' and locker.mode not like 'ExclusiveLock';
        """ % query
        rows = self.driver.query(stmt, return_tuples=False)
        if is_driver_result_valid(rows):
            blocks_info.locker_query = _get_driver_value(rows, 'locker_query')
        return blocks_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_tables_structure_info(self) -> list:
        tables_info = []
        tuples_statistics_stmt = """
            select abs(r1.n_live_tup - r2.reltuples)::int diff from pg_stat_user_tables r1, pg_class r2 
            where r1.schemaname = '{schemaname}' and r2.relname = '{relname}' and r1.relname = r2.relname;
        """
        user_table_stmt = """
            SELECT n_live_tup, n_dead_tup,
                   round(n_dead_tup / (n_live_tup + 1), 2) as dead_rate,
                   case when (last_vacuum is null) then -1 else 
                   extract(epoch from pg_catalog.now() - last_vacuum)::bigint end as vacuum_delay,
                   case when (last_analyze is null) then -1 else 
                   extract(epoch from pg_catalog.now() - last_analyze)::bigint end as analyze_delay, 
                   case when (last_data_changed is null) then -1 else 
                   extract(epoch from pg_catalog.now() - last_data_changed)::bigint end as data_changed_delay 
            FROM pg_stat_user_tables where schemaname = '{schemaname}' and relname = '{relname}';
        """
        pg_index_stmt = """
            select indexrelname, pg_get_indexdef(indexrelid) as indexdef from 
            pg_stat_user_indexes  where schemaname = '{schemaname}' and relname ='{relname}';
        """
        table_size_stmt = """
        select pg_catalog.pg_total_relation_size(rel.oid) / 1024 / 1024 AS mbytes 
                          FROM pg_namespace nsp JOIN pg_class rel ON nsp.oid = rel.relnamespace  
                          WHERE nsp.nspname = '{schemaname}' AND rel.relname = '{relname}'; 
        """
        for schema_name, tables_name in self.slow_sql_instance.tables_name.items():
            for table_name in tables_name:
                table_info = TableStructure()
                table_info.db_host = self.slow_sql_instance.db_host
                table_info.db_port = self.slow_sql_instance.db_port
                table_info.db_name = self.slow_sql_instance.db_name
                table_info.schema_name = schema_name
                table_info.table_name = table_name
                tuples_statistics_rows = self.driver.query(
                    tuples_statistics_stmt.format(schemaname=schema_name, relname=table_name), return_tuples=False)
                user_table_rows = self.driver.query(
                    user_table_stmt.format(schemaname=schema_name, relname=table_name), return_tuples=False)
                pg_index_rows = self.driver.query(
                    pg_index_stmt.format(schemaname=schema_name, relname=table_name), return_tuples=False)
                table_size_rows = self.driver.query(
                    table_size_stmt.format(schemaname=schema_name, relname=table_name), return_tuples=False)
                if is_driver_result_valid(tuples_statistics_rows):
                    table_info.tuples_diff = _get_driver_value(tuples_statistics_rows, 'diff')
                if is_driver_result_valid(user_table_rows):
                    table_info.live_tuples = _get_driver_value(user_table_rows, 'n_live_tup')
                    table_info.dead_tuples = _get_driver_value(user_table_rows, 'n_dead_tup')
                    table_info.dead_rate = _get_driver_value(user_table_rows, 'dead_rate')
                    table_info.analyze_delay = _get_driver_value(user_table_rows, 'analyze_delay', default=-1)
                    table_info.vacuum_delay = _get_driver_value(user_table_rows, 'vacuum_delay', default=-1)
                    table_info.data_changed_delay = _get_driver_value(user_table_rows, 'data_changed_delay', default=-1)
                if is_driver_result_valid(pg_index_rows):
                    for row in pg_index_rows:
                        table_info.index[row.get('indexrelname')] = parse_field_from_indexdef(row.get('indexdef'))
                if is_driver_result_valid(table_size_rows):
                    table_info.table_size = _get_driver_value(table_size_rows, 'mbytes')
                tables_info.append(table_info)
        return tables_info

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_pg_settings(self) -> dict:
        pg_settings = {}
        stmts = "select name, setting, vartype from pg_settings where name='{metric_name}';"
        for parameter in REQUIRED_PARAMETERS:
            row = self.driver.query(stmts.format(metric_name=parameter), return_tuples=False)
            if is_driver_result_valid(row):
                pg_setting = PgSetting()
                pg_setting.name = _get_driver_value(row, 'name')
                pg_setting.vartype = _get_driver_value(row, 'vartype')
                if pg_setting.vartype in ('integer', 'int64'):
                    pg_setting.setting = int(_get_driver_value(row, 'setting'))
                elif pg_setting.vartype == 'bool':
                    pg_setting.setting = 1 if _get_driver_value(row, 'setting') == 'on' else 0
                elif pg_setting.vartype == 'real':
                    pg_setting.setting = float(_get_driver_value(row, 'setting'))
                else:
                    pg_setting.setting = _get_driver_value(row, 'setting')
                pg_settings[parameter] = pg_setting
        return pg_settings

    @exception_follower(output=DatabaseInfo)
    @exception_catcher
    def acquire_database_info(self) -> DatabaseInfo:
        database_info = DatabaseInfo()
        used_connections_stmt = "select count(1) as used_conn from pg_stat_activity;"
        tps_stmt = """
            with 
               traction_number_1 as (select sum(xact_commit+xact_rollback) from pg_stat_database), 
               traction_number_2 as (select pg_sleep(0.2), sum(xact_commit+xact_rollback) from pg_stat_database) 
               select (traction_number_2.sum - traction_number_1.sum) / 0.2 as tps
               from traction_number_1, traction_number_2;
                   """
        used_connections_rows = self.driver.query(used_connections_stmt, return_tuples=False)
        tps_rows = self.driver.query(tps_stmt, return_tuples=False)
        if is_driver_result_valid(used_connections_rows):
            database_info.current_connection = _get_driver_value(used_connections_rows, 'used_conn')
        if is_driver_result_valid(tps_rows):
            database_info.current_tps = _get_driver_value(tps_rows, 'tps')
        return database_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_thread_info(self) -> ThreadInfo:
        stmt = """
        select event, wait_status, block_sessionid, lockmode, locktag from gs_asp where 
        query_id={debug_query_id};
        """.format(debug_query_id=self.slow_sql_instance.query_id)
        thread_info = ThreadInfo()
        rows = self.driver.query(stmt, force_connection_db='postgres', return_tuples=False)
        if is_driver_result_valid(rows):
            thread_info.event = _get_driver_value(rows, 'event')
            thread_info.wait_status = _get_driver_value(rows, 'wait_status')
            thread_info.block_sessionid = _get_driver_value(rows, 'block_sessionid')
            thread_info.lockmode = _get_driver_value(rows, 'lock_mode')
            thread_info.locktag = _get_driver_value(rows, 'locktag')
        return thread_info

    @exception_follower(output=SystemInfo)
    @exception_catcher
    def acquire_system_info(self) -> SystemInfo:
        system_info = SystemInfo()
        return system_info

    @exception_follower(output=NetWorkInfo)
    @exception_catcher
    def acquire_network_info(self):
        """
        Unable to get network metrics in driver-based data context
        """
        network_info = NetWorkInfo()
        return network_info

    @exception_follower(output=str)
    @exception_catcher
    def acquire_rewritten_sql(self) -> str:
        if not self.slow_sql_instance.query.strip().upper().startswith('SELECT'):
            return ''
        rewritten_flags = []
        query = self.standard_query
        if self.query_type == 'normalized':
            query = replace_question_mark_with_value(query)
            query = replace_question_mark_with_dollar(query)
        rewritten_sql = rewrite_sql_api(self.slow_sql_instance.db_name,
                                            query,
                                            rewritten_flags=rewritten_flags,
                                            if_format=False,
                                            driver=self.driver)
        flag = rewritten_flags[0] if len(rewritten_flags) else False
        if not flag:
            return ''
        rewritten_sql = rewritten_sql.replace('\n', ' ')
        rewritten_sql_plan = self.acquire_plan(rewritten_sql)
        if rewritten_sql_plan is None:
            return ''
        old_sql_plan_parse = plan_parsing.Plan()
        rewritten_sql_plan_parse = plan_parsing.Plan()
        # Abandon the rewrite if the rewritten statement does not perform as well as the original statement.
        old_sql_plan_parse.parse(self.slow_sql_instance.query_plan)
        rewritten_sql_plan_parse.parse(rewritten_sql_plan)
        if old_sql_plan_parse.root_node.total_cost > rewritten_sql_plan_parse.root_node.total_cost:
            return rewritten_sql
        return ''

    @exception_follower(output=tuple)
    @exception_catcher
    def acquire_index_analysis_info(self) -> tuple:
        recommend_indexes, redundant_indexes = [], []
        query = self.standard_query
        if self.query_type == 'normalized':
            query = replace_question_mark_with_value(query)
            query = replace_question_mark_with_dollar(query)
        executor = RpcExecutor(self.slow_sql_instance.db_name, None, None, None, None,
                               self.slow_sql_instance.schema_name, driver=self.driver)
        template = {query: {'cnt': 1, 'samples': [query]}}
        result = rpc_index_advise(executor, template)
        if result.get('recommendIndexes'):
            for recommend_index_detail in result.get('recommendIndexes'):
                recommend_index = Index()
                recommend_index.schema_name = recommend_index_detail.get('schemaName')
                recommend_index.table_name = recommend_index_detail.get('tbName')
                recommend_index.column_name = recommend_index_detail.get('columns')
                recommend_index.index_type = recommend_index_detail.get('index_type')
                recommend_indexes.append(str(recommend_index))
        if result.get('uselessIndexes'):
            for unless_index in result.get('uselessIndexes'):
                if unless_index.get('schemaName') in self.slow_sql_instance.tables_name and \
                        unless_index.get('tbName') in \
                        self.slow_sql_instance.tables_name.get(unless_index.get('schemaName')):
                    redundant_index = Index()
                    redundant_index.schema_name = unless_index.get('schemaName')
                    redundant_index.table_name = unless_index.get('tbName')
                    redundant_index.column_name = unless_index.get('columns')
                    redundant_index.index_type = unless_index.get('index_type')
                    redundant_indexes.append(str(redundant_index))
        return ';'.join(recommend_indexes), ';'.join(redundant_indexes)

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_unused_index(self):
        unused_index = []
        unused_index_stmt = """
            select pi.indexrelname from pg_indexes pis
            join pg_stat_user_indexes pi
            on pis.schemaname = pi.schemaname and pis.tablename = pi.relname and pis.indexname = pi.indexrelname
            left join pg_constraint pco
            on pco.conname = pi.indexrelname and pco.conrelid = pi.relid
            where pco.contype is distinct from 'p' and pco.contype is distinct from 'u'
            and (idx_scan,idx_tup_read,idx_tup_fetch) = (0,0,0)
            and pis.indexdef !~ ' UNIQUE INDEX '
            and pis.schemaname = '{schemaname}' and relname = '{relname}'
        """
        for schema_name, tables_name in self.slow_sql_instance.tables_name.items():
            for table_name in tables_name:
                stmt = unused_index_stmt.format(schemaname=schema_name, relname=table_name)
                rows = self.driver.query(stmt, return_tuples=False)
                for row in rows:
                    unused_index.append(row.get('indexrelname'))
        return unused_index
