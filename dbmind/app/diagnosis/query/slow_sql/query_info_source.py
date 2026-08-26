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

PORT_SUFFIX = "(:[0-9]{4,5}|)"
white_list_of_sql_type = ('SELECT', 'UPDATE', 'DELETE', 'INSERT', 'WITH')
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


REQUIRED_PARAMETERS = ('enable_nestloop', 'enable_hashjoin', 'enable_mergejoin', 'enable_thread_pool',
                       'enable_indexscan', 'enable_hashagg', 'enable_sort', 'max_connections')


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


class DatabaseInfo:
    """Data structure to save database information such as database address and TPS, connection"""

    def __init__(self):
        self.tps = 0
        self.connection = 1
        self.thread_pool_rate = 0.0


class SystemInfo:
    """Data structure to save system information such as database address, IOWAIT, CPU_USAGE, etc"""

    def __init__(self):
        self.ioutils = {}
        self.iowait_cpu_usage = []
        self.cpu_core_number = 1
        self.user_cpu_usage = []
        self.system_mem_usage = []
        self.db_cpu_usage = []
        self.db_mem_usage = []
        # we only record the usage of disk which data directory is located
        self.disk_usage = []
        self.process_fds_rate = []
        self.io_read_delay = {}
        self.io_write_delay = {}


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
        self.receive_drop = []
        self.transmit_drop = []
        self.bandwidth_usage = {}


class Index:
    def __init__(self):
        self.db_name = None
        self.schema_name = None
        self.table_name = None
        self.column_name = None
        self.index_name = None
        self.index_type = None

    def __repr__(self):
        return "(schema: %s, index: %s(%s))" % (
            self.schema_name, self.table_name, self.column_name
        )


class TotalMemoryDetail:
    def __init__(self):
        # unit is 'MB'
        self.max_process_memory = 1
        self.process_used_memory = 0
        self.max_dynamic_memory = 1
        self.dynamic_used_memory = 0
        self.other_used_memory = 0


class ThreadInfo:
    def __init__(self):
        self.wait_event = None
        self.wait_status = None
        self.sessionid = None
        self.thread_id = None
        self.block_sessionid = None
        self.lockmode = None
        self.locktag = None


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

    def acquire_database_info(self):
        raise NotImplementedError

    def acquire_plan(self):
        raise NotImplementedError

    def acquire_tables_structure_info(self):
        raise NotImplementedError

    def acquire_system_info(self):
        raise NotImplementedError

    def acquire_network_info(self):
        raise NotImplementedError

    def acquire_wait_event_info(self):
        raise NotImplementedError

    def acquire_pg_settings(self):
        raise NotImplementedError

    def acquire_unused_index(self):
        raise NotImplementedError

    def acquire_index_analysis_info(self):
        raise NotImplementedError

    def acquire_rewritten_sql(self):
        raise NotImplementedError

    def acquire_total_memory_detail(self):
        raise NotImplementedError


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


def _get_sequence_values(s: Sequence, precision=0):
    """
    Get all values of sequence.
    precision: 0 means casting to integer,
               positive value means float with the precision,
               negative value means nothing to do.
    """
    if precision == 0:
        return [int(item) for item in s.values]
    elif precision > 0:
        return [round(float(item), precision) for item in s.values]
    else:
        return s.values


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


def _get_sequences_sum_value(seqs: List[Sequence], precision=0, method='first'):
    """
    Get the sum value of sequence.
    precision: 0 means casting to integer,
               positive value means float with the precision,
               negative value means nothing to do.
    """
    if method == 'first':
        value = 0
        for s in seqs:
            if is_sequence_valid(s):
                value += _get_sequence_first_value(s, precision=precision)
    elif method == 'all':
        value = []
        # to prevent inconsistent lengths, first obtain the minimum length
        minimum_length = min(len(seq) for seq in seqs)
        for i in range(minimum_length):
            value.append(round(sum(seq.values[i] for seq in seqs), precision))
    else:
        raise ValueError
    return value


def _get_specified_sequence(seqs, **labels):
    # get sequence which has specified labels
    flag = True
    for seq in seqs:
        for key, value in labels.items():
            if seq.labels[key] != value:
                flag = False
                break
        if flag:
            return seq


def _get_driver_value(rows, key, precision=0, default=0):
    """
    Get the execution result of the driver.
    Note: the result format: '[RealDictRow([('relname', 't1'), ('n_live_tup', 3000076)]), RealDictRow(...), ...]'
    """
    if precision == 0:
        return int(rows[0].get(key, default)) if len(rows) == 1 else [int(item.get(key, default)) for item in rows]
    elif precision > 1:
        return round(float(rows[0].get(key, default)), precision) if len(rows) == 1 \
            else [round(float(item.get(key, default)), precision) for item in rows]
    else:
        return rows[0].get(key, default) if len(rows) == 1 else [item.get(key, default) for item in rows]


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
        self.expansion_factor = kwargs.get('expansion_factor', 2)
        self.query_start_time = datetime.fromtimestamp(self.slow_sql_instance.start_time / 1000)
        if self.slow_sql_instance.duration_time / 1000 >= self.fetch_interval:
            self.query_end_time = datetime.fromtimestamp(
                self.slow_sql_instance.start_time / 1000 + self.slow_sql_instance.duration_time / 1000
            )
        else:
            self.query_end_time = datetime.fromtimestamp(
                self.slow_sql_instance.start_time / 1000 + int(self.expansion_factor * self.acquire_fetch_interval())
            )
        logging.debug('[SLOW QUERY] fetch start time: %s, fetch end time: %s', self.query_start_time,
                      self.query_end_time)
        logging.debug('[SLOW QUERY] fetch interval: %s', self.fetch_interval)
        self.is_rpc_valid = True
        # determine whether the current context(RPC) is available
        try:
            if global_vars.agent_proxy.current_rpc() is None:
                global_vars.agent_proxy.switch_context(slow_sql_instance.instance)
            else:
                agent_addr = global_vars.agent_proxy.current_agent_addr()
                clusters = global_vars.agent_proxy.current_cluster_instances()
                if self.slow_sql_instance.db_host is None:
                    self.slow_sql_instance.db_host, self.slow_sql_instance.db_port = agent_addr.split(':')
                # in a centralized or stand-alone environment, the slow SQL instance is the agent address,
                # but not in a distributed environment
                elif self.slow_sql_instance.instance not in clusters:
                    global_vars.agent_proxy.switch_context(slow_sql_instance.instance)
        except Exception as e:
            logging.warning("RPC is not available in instance '%s'.", slow_sql_instance.instance)
            self.is_rpc_valid = False
        self.standard_query = self.slow_sql_instance.query
        if self.slow_sql_instance.track_parameter:
            self.standard_query = fill_value(self.slow_sql_instance.query)
            self.slow_sql_instance.query = remove_parameter_part(slow_sql_instance.query)
        if is_query_normalized(self.standard_query):
            self.query_type = 'normalized'
        self.standard_query = standardize_sql(self.standard_query)
        # make sure that only the SQL in the whitelist is used to obtain the execution plan
        if sum(self.slow_sql_instance.query.upper().startswith(item) for item in white_list_of_sql_type) and \
                self.slow_sql_instance.query_plan is None:
            # in order to speed up diagnosis, first use the default schema,
            # if the default schema does not work, then traverse all schemas.
            self.slow_sql_instance.query_plan = self.acquire_plan(self.standard_query)
            if self.slow_sql_instance.query_plan is None:
                logging.warning("Execution plan cannot be obtained in schema "
                                "'%s', start automatically matching the "
                                "appropriate schema...", self.slow_sql_instance.schema_name)
                self.is_sql_valid = False
                schemas = self.adjust_schema()
                for schema in schemas:
                    logging.info("Try to get execution plan under schema '%s'", schema)
                    self.slow_sql_instance.schema_name = schema
                    self.slow_sql_instance.query_plan = self.acquire_plan(self.standard_query)
                    if self.slow_sql_instance.query_plan is None:
                        self.is_sql_valid = False
                        logging.info("Failed to get execution plan under schema '%s'", schema)
                        continue
                    else:
                        self.is_sql_valid = True
                        logging.info("Get execution plan under schema '%s'", schema)
                        break
        else:
            self.is_sql_valid = False

    @exception_follower(output=list)
    @exception_catcher
    def adjust_schema(self):
        schemas = []
        if not self.is_rpc_valid:
             return schemas
        stmt = (
            "select nspname "
            "from pg_namespace "
            "where nspname not like 'dbe_%' and nspname not in "
            "('pg_toast', 'cstore', 'snapshot', 'blockchain', 'sys', 'pg_catalog', "
            "'db4ai', 'sqladvisor', 'information_schema', 'pkg_service', 'pkg_util');"
        )
        rows = global_vars.agent_proxy.call('query_in_database',
                                            stmt,
                                            self.slow_sql_instance.db_name,
                                            return_tuples=False,
                                            fetch_all=False)
        for row in rows:
            if not row:
                continue
            schemas.append(row.get('nspname'))
        return schemas

    @exception_follower(output=None)
    @exception_catcher
    def acquire_plan(self, query):
        """
        scenes to be used:
          1) query_plan in statement_history is empty
          2) used in component scene
        """
        query_plan = ''
        if self.query_type == 'normalized':
            query = replace_question_mark_with_value(query)
            query = replace_question_mark_with_dollar(query)
            stmts = "set explain_perf_mode=normal;set current_schema='%s';" % self.slow_sql_instance.schema_name + ';'.join(
                get_generate_prepare_sqls_function()(query))
            rows = global_vars.agent_proxy.call('query_in_database',
                                                stmts,
                                                self.slow_sql_instance.db_name,
                                                return_tuples=False,
                                                fetch_all=True)
            if len(rows) == 5 and rows[-2]:
                rows = rows[-2]
        else:
            stmts = "set explain_perf_mode=normal;set current_schema='%s';explain %s" % (self.slow_sql_instance.schema_name,
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
        """Get data source collection frequency, unit 'second'"""
        sequence = dai.get_latest_metric_value("prometheus_target_interval_length_seconds").filter(
            quantile="0.99").fetchone()
        if is_sequence_valid(sequence):
            self.fetch_interval = int(sequence.values[0])
        else:
            return DEFAULT_FETCH_INTERVAL
        return self.fetch_interval

    @exception_follower(output=list)
    @exception_catcher
    def acquire_tables_structure_info(self) -> list:
        """
        it is used to acquire table structure and index information related to slow query.
        note: this method first obtain data from TSDB, and uses RPC to obtain data when tsdb returns no data,
              the data obtained based on RPC cannot represent the historical situation.
        """
        table_structure = []
        if not self.slow_sql_instance.tables_name:
            return table_structure
        # this SQL estimate the number of rows in table statistics and it can be a bit time-consuming
        tuples_statistics_stmt = """
            select abs(r1.n_live_tup - r2.reltuples)::int diff from pg_stat_user_tables r1, pg_class r2, 
            pg_namespace r3 where r1.relname = '{relname}' and r1.schemaname = '{schemaname}' 
            and r1.relname = r2.relname and r1.schemaname = r3.nspname and r2.relnamespace = r3.oid;
        """
        user_table_stmt = """
            SELECT n_live_tup::int, n_dead_tup::int,
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
                        table_info.tuples_diff = _get_driver_value(tuples_statistics_rows, 'diff')
                    if is_driver_result_valid(user_table_rows):
                        table_info.live_tuples = _get_driver_value(user_table_rows, 'n_live_tup')
                        table_info.dead_tuples = _get_driver_value(user_table_rows, 'n_dead_tup')
                        table_info.dead_rate = _get_driver_value(user_table_rows, 'dead_rate', precision=2)
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
                    table_info.table_size = _get_sequence_max_value(pg_table_size_info, precision=2)
                else:
                    table_size_rows = global_vars.agent_proxy.call('query_in_database',
                                                                   table_size_stmt.format(schemaname=schema_name,
                                                                                          relname=table_name),
                                                                   self.slow_sql_instance.db_name,
                                                                   return_tuples=False)
                    if is_driver_result_valid(table_size_rows):
                        table_info.table_size = _get_driver_value(table_size_rows, 'mbytes', precision=2)
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
                pg_setting.name = sequence.labels['name']
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
        """Acquire table database related information related to slow query, such as thread pool, connection, TPS"""
        database_info = DatabaseInfo()
        used_connection_sequences = dai.get_metric_sequence("gaussdb_total_connection", self.query_start_time,
                                                            self.query_end_time).from_server(
            self.slow_sql_instance.instance).fetchall()
        cur_tps_sequence = dai.get_metric_sequence("gaussdb_qps_by_instance", self.query_start_time,
                                                    self.query_end_time).from_server(
            self.slow_sql_instance.instance).fetchone()
        thread_pool_occupy_rate = dai.get_metric_sequence("pg_thread_pool_rate", self.query_start_time,
                                                          self.query_end_time).from_server(
            self.slow_sql_instance.instance).fetchone()
        if is_sequence_valid(used_connection_sequences):
            database_info.connection = _get_sequences_sum_value(used_connection_sequences)
        else:
            used_connections_stmt = "select count(1) as used_conn from pg_stat_activity;"
            used_connections_rows = global_vars.agent_proxy.call('query_in_database',
                                                                 used_connections_stmt,
                                                                 self.slow_sql_instance.db_name,
                                                                 return_tuples=False)
            if is_driver_result_valid(used_connections_rows):
                database_info.connection = _get_driver_value(used_connections_rows, 'used_conn')
        if is_sequence_valid(cur_tps_sequence):
            database_info.tps = _get_sequence_max_value(cur_tps_sequence, precision=2)
        if is_sequence_valid(thread_pool_occupy_rate):
            database_info.thread_pool_rate = _get_sequence_max_value(thread_pool_occupy_rate, precision=4)
        else:
            thread_pool_rate_stmts = "select s1.count / s2.count as rate from " \
                                     "(select count(*)  as count from pg_thread_wait_status " \
                                     "where wait_status != 'wait cmd') s1, " \
                                     "(select count(*)  as count from pg_thread_wait_status " \
                                     "where wait_status != 'wait cmd') s2;"
            thread_pool_rate_rows = global_vars.agent_proxy.call('query_in_database',
                                                                 thread_pool_rate_stmts,
                                                                 self.slow_sql_instance.db_name,
                                                                 return_tuples=False)
            if is_driver_result_valid(thread_pool_rate_rows):
                database_info.thread_pool_rate = _get_driver_value(thread_pool_rate_rows, 'rate', precision=4)
        return database_info

    @exception_follower(output=ThreadInfo)
    @exception_catcher
    def acquire_wait_event_info(self) -> ThreadInfo:
        """
        acquire wait event info of thread.
        notes: it is expensive to record all waiting events in TSDB,
               so currently only support for obtaining thread waiting event information at runtime.
        """
        thread_info = ThreadInfo()
        if not self.slow_sql_instance.debug_query_id:
            return thread_info
        if not self.is_rpc_valid:
            return thread_info
        stmt = """
        select tid, wait_status, wait_event, block_sessionid, lockmode from pg_catalog.pg_thread_wait_status where 
        query_id={debug_query_id};""".format(debug_query_id=self.slow_sql_instance.debug_query_id)
        rows = global_vars.agent_proxy.call('query_in_database',
                                            stmt,
                                            self.slow_sql_instance.db_name,
                                            return_tuples=False)
        if is_driver_result_valid(rows):
            thread_info.thread_id = _get_driver_value(rows, 'tid', precision=-1)
            thread_info.wait_event = _get_driver_value(rows, 'wait_event', precision=-1)
            thread_info.wait_status = _get_driver_value(rows, 'wait_status', precision=-1)
            thread_info.block_sessionid = _get_driver_value(rows, 'block_sessionid', precision=-1)
            thread_info.lockmode = _get_driver_value(rows, 'lock_mode', precision=-1)
            thread_info.locktag = _get_driver_value(rows, 'locktag', precision=-1)
        return thread_info

    @exception_follower(output=SystemInfo)
    @exception_catcher
    def acquire_system_info(self) -> SystemInfo:
        """Acquire system information on the database server """
        system_info = SystemInfo()
        ioutils_info = dai.get_metric_sequence("os_disk_ioutils", self.query_start_time,
                                               self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        # we only acquire usage of the disk which instance data directory is located
        mountpoint, _, _ = dai.get_data_directory_mountpoint_info(self.slow_sql_instance.instance)
        disk_usage_info = dai.get_metric_sequence("os_disk_usage", self.query_start_time,
                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").filter(mountpoint=mountpoint).fetchone()
        user_cpu_usage_info = dai.get_metric_sequence("os_cpu_usage", self.query_start_time,
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
        db_cpu_usage_info = \
            dai.get_metric_sequence("gaussdb_progress_cpu_usage",
                                    self.query_start_time,
                                    self.query_end_time)\
            .filter_like(instance=self.slow_sql_instance.db_host + PORT_SUFFIX)\
            .fetchall()
        db_mem_usage_info = \
            dai.get_metric_sequence("gaussdb_progress_mem_usage",
                                    self.query_start_time,
                                    self.query_end_time)\
            .filter_like(instance=self.slow_sql_instance.db_host + PORT_SUFFIX)\
            .fetchall()
        if is_sequence_valid(process_fds_rate_info):
            system_info.process_fds_rate = _get_sequence_values(process_fds_rate_info, precision=4)
        if is_sequence_valid(cpu_process_number_info):
            system_info.cpu_core_number = _get_sequence_max_value(cpu_process_number_info)
        if is_sequence_valid(ioutils_info):
            ioutils_dict = {item.labels['device']: _get_sequence_values(item, precision=4)
                            for item in ioutils_info if item.labels}
            system_info.ioutils = ioutils_dict
        if is_sequence_valid(disk_usage_info):
            system_info.disk_usage = _get_sequence_values(disk_usage_info, precision=4)
        # add the consumption of all gaussdb related processes as the total consumption,
        # it is suitable for situations where only one instance exists on a machine, db_mem_usage is the same as him.
        if is_sequence_valid(db_cpu_usage_info) and is_sequence_valid(cpu_process_number_info):
            system_info.db_cpu_usage = [item / (system_info.cpu_core_number * 100) for item
                                        in _get_sequences_sum_value(db_cpu_usage_info, precision=4, method='all')]
        if is_sequence_valid(db_mem_usage_info):
            system_info.db_mem_usage = _get_sequences_sum_value(db_mem_usage_info, precision=4, method='all')
            system_info.db_mem_usage = [item / 100 for item in system_info.db_mem_usage]
        if is_sequence_valid(user_cpu_usage_info):
            system_info.user_cpu_usage = _get_sequence_values(user_cpu_usage_info, precision=4)
        if is_sequence_valid(mem_usage_info):
            system_info.system_mem_usage = _get_sequence_values(mem_usage_info, precision=4)
        return system_info

    @exception_follower(output=NetWorkInfo)
    @exception_catcher
    def acquire_network_info(self) -> NetWorkInfo:
        network_info = NetWorkInfo()
        node_network_receive_drop_info = dai.get_metric_sequence('os_network_receive_drop',
                                                                 self.query_start_time,
                                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        node_network_transmit_drop_info = dai.get_metric_sequence('os_network_transmit_drop',
                                                                  self.query_start_time,
                                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        node_network_transmit_bytes_info = dai.get_metric_sequence('os_network_transmit_bytes',
                                                                   self.query_start_time,
                                                                   self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        node_network_receive_bytes_info = dai.get_metric_sequence('os_network_receive_bytes',
                                                                  self.query_start_time,
                                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        node_network_duplex_info = dai.get_latest_metric_value('node_network_info').from_server_like(
            f"{self.slow_sql_instance.db_host}.*").fetchall()
        node_network_speed_bytes_info = dai.get_latest_metric_value('node_network_speed_bytes').from_server_like(
            f"{self.slow_sql_instance.db_host}.*").fetchall()
        for sequence in node_network_transmit_drop_info:
            if is_sequence_valid(sequence):
                network_info.transmit_drop.append({'device': sequence.labels['device'],
                                                   'drop': _get_sequence_max_value(sequence, precision=2)})
        for sequence in node_network_receive_drop_info:
            if is_sequence_valid(sequence):
                network_info.receive_drop.append({'device': sequence.labels['device'],
                                                  'drop': _get_sequence_max_value(sequence, precision=2)})
        network_device_info = {}
        for sequence in node_network_speed_bytes_info:
            if is_sequence_valid(sequence):
                # transmit to MB/s
                network_device_info[sequence.labels['device']] = \
                    {'speed': _get_sequence_first_value(sequence, precision=2) / 1024 / 1024}
        for sequence in node_network_duplex_info:
            if is_sequence_valid(sequence):
                if sequence.labels['device'] in network_device_info:
                    network_device_info[sequence.labels['device']]['duplex'] = sequence.labels['duplex']
        for sequence in node_network_transmit_bytes_info:
            if is_sequence_valid(sequence):
                device = sequence.labels['device']
                receive_bytes_sequence = _get_specified_sequence(node_network_receive_bytes_info, device=device)
                device_info = network_device_info.get(device)
                if device_info is None:
                    continue
                if device_info['duplex'] == 'full':
                    network_info.bandwidth_usage[device] = \
                        {'transmit': [round(item / device_info['speed'], 2) for item in sequence.values],
                         'receive': [round(item / device_info['speed'], 2) for item in sequence.values]}
                elif device_info['duplex'] == 'half':
                    network_info.bandwidth_usage[device] = \
                        [round((item1 + item2) / device_info['speed'], 2) for
                         item1, item2 in zip(sequence.values, receive_bytes_sequence.values)]
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
        rewritten_sql = rewrite_sql_api(self.slow_sql_instance.db_name, query,
                                        rewritten_flags=rewritten_flags, if_format=False)
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
        if not self.is_rpc_valid:
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

    @exception_follower(output=TotalMemoryDetail)
    @exception_catcher
    def acquire_total_memory_detail(self) -> TotalMemoryDetail:
        memory_detail = TotalMemoryDetail()
        if not self.is_rpc_valid:
            return memory_detail
        stmt = """
        select memorytype, memorybytes from pg_catalog.gs_total_memory_detail 
        where memorytype in ('max_process_memory', 'process_used_memory', 'max_dynamic_memory', 
        'dynamic_used_memory', 'other_used_memory')"""
        rows = global_vars.agent_proxy.call('query_in_database',
                                            stmt,
                                            self.slow_sql_instance.db_name,
                                            return_tuples=False)
        for row in rows:
            setattr(memory_detail, row.get('memorytype'), round(float(row.get('memorymbytes')), 2))
        return memory_detail


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
        # make sure that only the SQL in the whitelist is used to obtain the execution plan
        if sum(self.slow_sql_instance.query.upper().startswith(item) for item in white_list_of_sql_type) and \
                self.slow_sql_instance.query_plan is None:
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

    @exception_follower(output=list)
    @exception_catcher
    def acquire_tables_structure_info(self) -> list:
        tables_info = []
        tuples_statistics_stmt = """
            select abs(r1.n_live_tup - r2.reltuples)::int diff from pg_stat_user_tables r1, pg_class r2, 
            pg_namespace r3 where r1.relname='{relname}' and r1.schemaname='{schemaname}' 
            and r1.relname=r2.relname and r1.schemaname=r3.nspname and r2.relnamespace=r3.oid;
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
                    table_info.dead_rate = _get_driver_value(user_table_rows, 'dead_rate', precision=2)
                    table_info.analyze_delay = _get_driver_value(user_table_rows, 'analyze_delay', default=-1)
                    table_info.vacuum_delay = _get_driver_value(user_table_rows, 'vacuum_delay', default=-1)
                    table_info.data_changed_delay = _get_driver_value(user_table_rows, 'data_changed_delay', default=-1)
                if is_driver_result_valid(pg_index_rows):
                    for row in pg_index_rows:
                        table_info.index[row.get('indexrelname')] = parse_field_from_indexdef(row.get('indexdef'))
                if is_driver_result_valid(table_size_rows):
                    table_info.table_size = _get_driver_value(table_size_rows, 'mbytes', precision=2)
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
                pg_setting.name = _get_driver_value(row, 'name', precision=-1)
                pg_setting.vartype = _get_driver_value(row, 'vartype', precision=-1)
                if pg_setting.vartype in ('integer', 'int64'):
                    pg_setting.setting = int(_get_driver_value(row, 'setting'))
                elif pg_setting.vartype == 'bool':
                    pg_setting.setting = 1 if _get_driver_value(row, 'setting', precision=-1) == 'on' else 0
                elif pg_setting.vartype == 'real':
                    pg_setting.setting = float(_get_driver_value(row, 'setting', precision=4))
                else:
                    pg_setting.setting = _get_driver_value(row, 'setting', precision=-1)
                pg_settings[parameter] = pg_setting
        return pg_settings

    @exception_follower(output=DatabaseInfo)
    @exception_catcher
    def acquire_database_info(self) -> DatabaseInfo:
        database_info = DatabaseInfo()
        used_connections_stmt = "select count(1) as used_conn from pg_stat_activity;"
        used_connections_rows = self.driver.query(used_connections_stmt,
                                                  return_tuples=False)
        thread_pool_rate_stmts = "select s1.count / s2.count as rate from " \
                                 "(select count(*)  as count from pg_thread_wait_status " \
                                 "where wait_status != 'wait cmd') s1, " \
                                 "(select count(*)  as count from pg_thread_wait_status " \
                                 "where wait_status != 'wait cmd') s2;"
        thread_pool_rate_rows = self.driver.query(thread_pool_rate_stmts,
                                                  return_tuples=False)
        if is_driver_result_valid(thread_pool_rate_rows):
            database_info.thread_pool_rate = _get_driver_value(thread_pool_rate_rows, 'rate', precision=4)
        if is_driver_result_valid(used_connections_rows):
            database_info.connection = _get_driver_value(used_connections_rows, 'used_conn')
        return database_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_wait_event_info(self) -> ThreadInfo:
        thread_info = ThreadInfo()
        if not self.slow_sql_instance.debug_query_id:
            return thread_info
        stmt = """
        select tid, wait_status, wait_event, block_sessionid, lockmode from pg_catalog.pg_thread_wait_status where 
        query_id={debug_query_id};""".format(debug_query_id=self.slow_sql_instance.debug_query_id)
        rows = self.driver.query(stmt, force_connection_db=self.slow_sql_instance.db_name, return_tuples=False)
        if is_driver_result_valid(rows):
            thread_info.event = _get_driver_value(rows, 'event', precision=-1)
            thread_info.wait_status = _get_driver_value(rows, 'wait_status', precision=-1)
            thread_info.block_sessionid = _get_driver_value(rows, 'block_sessionid', precision=-1)
            thread_info.lockmode = _get_driver_value(rows, 'lock_mode', precision=-1)
            thread_info.locktag = _get_driver_value(rows, 'locktag', precision=-1)
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
        rewritten_sql = rewrite_sql_api(self.slow_sql_instance.db_name, query, rewritten_flags=rewritten_flags,
                                        if_format=False, driver=self.driver)
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

    @exception_follower(output=TotalMemoryDetail)
    @exception_catcher
    def acquire_total_memory_detail(self) -> TotalMemoryDetail:
        memory_detail = TotalMemoryDetail()
        stmt = """
        select memorytype, memorymbytes from pg_catalog.gs_total_memory_detail 
        where memorytype in ('max_process_memory', 'process_used_memory', 'max_dynamic_memory', 
        'dynamic_used_memory', 'other_used_memory')"""
        rows = self.driver.query(stmt, return_tuples=False)
        for row in rows:
            setattr(memory_detail, row.get('memorytype'), round(float(row.get('memorymbytes')), 2))
        return memory_detail
