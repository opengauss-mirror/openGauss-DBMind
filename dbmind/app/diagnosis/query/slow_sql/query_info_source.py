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
import math
from datetime import datetime, timedelta
from functools import wraps

from dbmind.common.parser import plan_parsing
from dbmind.common.parser.sql_parsing import to_ts, get_generate_prepare_sqls_function
from dbmind.common.utils import ExceptionCatcher
from dbmind.global_vars import agent_rpc_client
from dbmind.service import dai
from dbmind.common.parser.sql_parsing import fill_value, standardize_sql, replace_comma_with_dollar
from dbmind.common.parser.sql_parsing import remove_parameter_part
from dbmind.service.web import toolkit_rewrite_sql
from dbmind.components.index_advisor.utils import WorkLoad
from dbmind.components.index_advisor.index_advisor_workload import generate_candidate_indexes
from dbmind.app.optimization._index_recommend_client_driver import RpcExecutor


exception_catcher = ExceptionCatcher(strategy='raise', name='SLOW QUERY')
DEFAULT_FETCH_INTERVAL = 15


def exception_follower(output=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.exception("Function execution error: %s" % func.__name__)
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
        self.dead_rate = 0.0
        # How to get field num for table
        self.column_number = 0
        # This field can be used as a flag for whether the statistics
        # are updated.
        self.last_autovacuum = 0
        self.last_autoanalyze = 0
        self.vacuum = 0
        self.analyze = 0
        self.table_size = 0
        self.index_size = 0
        self.index = {}
        self.redundant_index = []
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
        self.locker_query_end = None


class DatabaseInfo:
    """Data structure to save database information such as database address and TPS, connection"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.history_tps = []
        self.current_tps = []
        self.max_conn = 1
        self.used_conn = 0
        self.thread_pool = {}


class GsSQLCountInfo:
    def __init__(self):
        self.node_name = None
        self.select_count = 0
        self.update_count = 0
        self.insert_count = 0
        self.delete_count = 0
        self.mergeinto_count = 0
        self.ddl_count = 0
        self.dcl_count = 0
        self.dml_count = 0


class SystemInfo:
    """Data structure to save system information such as database address, IOWAIT, IOCAPACITY, CPU_USAGE, etc"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.iops = 0
        self.db_iops = []
        self.ioutils = {}
        self.iocapacity = []
        self.db_iocapacity = []
        self.iowait = []
        self.total_memory = 0.0
        self.gaussdb_number = 0
        self.cpu_core_number = 1
        self.db_cpu_usage = []
        self.db_mem_usage = []
        self.db_data_occupy_rate = 0.0
        self.disk_usage = {}
        self.cpu_usage = []
        self.mem_usage = []
        self.load_average1 = []
        self.load_average5 = []
        self.load_average15 = []
        self.db_process_fds_rate = []
        self.process_fds_rate = []
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


class PgReplicationInfo:
    def __init__(self):
        self.application_name = None
        self.pg_downstream_state_count = 0
        self.pg_replication_lsn = 0
        self.pg_replication_sent_diff = 0
        self.pg_replication_write_diff = 0
        self.pg_replication_flush_diff = 0
        self.pg_replication_replay_diff = 0


class BgWriter:
    def __init__(self):
        self.checkpoint_avg_sync_time = 0.0
        self.checkpoint_proactive_triggering_ratio = 0.0
        self.buffers_checkpoint = 0.0
        self.buffers_clean = 0.0
        self.buffers_backend = 0.0
        self.buffers_alloc = 0.0


class Index:
    def __init__(self):
        self.db_name = None
        self.schema_name = None
        self.table_name = None
        self.column_name = None
        self.index_name = None
        self.index_type = None

    def __repr__(self):
        return "database: %s, schema: %s, index: %s(%s), index_type: %s" % (
            self.db_name, self.schema_name, self.table_name, self.column_name, 'b-tree'
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
        self.node_name = None
        self.type = None
        self.event = None
        self.wait = 0
        self.failed_wait = 0
        self.total_wait_time = 0
        self.last_updated = 0


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


class QueryContextFromTSDB(QueryContext):
    """The object of slow query data processing factory"""

    def __init__(self, slow_sql_instance, **kwargs):
        """
        :param slow_sql_instance: The instance of slow query
        :param default_fetch_interval: fetch interval of data source
        :param expansion_factor: Ensure that the time expansion rate of the data can be collected
        """
        super().__init__(slow_sql_instance)
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
        self.standard_query = standardize_sql(self.standard_query)
        if self.slow_sql_instance.query_plan is None:
            self.slow_sql_instance.query_plan = self.acquire_plan(self.standard_query)
            if self.slow_sql_instance.query_plan is None:
                self.is_sql_valid = False

    @exception_follower(output=None)
    @exception_catcher
    def acquire_plan(self, query):
        query_plan = ''
        if self.slow_sql_instance.track_parameter:
            stmts = "set current_schema='%s';explain %s" % (self.slow_sql_instance.schema_name,
                                                            query)
        else:
            # Get execution plan based on PBE
            no_comma_query = replace_comma_with_dollar(query)
            stmts = "set current_schema='%s';" + ';'.join(get_generate_prepare_sqls_function()(no_comma_query))
        rows = agent_rpc_client.call('query_in_database',
                                     stmts,
                                     self.slow_sql_instance.db_name,
                                     return_tuples=True)
        for row in rows:
            query_plan += row[0] + '\n'
        if query_plan:
            return query_plan

    @exception_follower(output=15)
    @exception_catcher
    def acquire_fetch_interval(self) -> int:
        """Get data source collection frequency"""

        sequence = dai.get_latest_metric_value("prometheus_target_interval_length_seconds").filter(
            quantile="0.99").fetchone()
        if sequence.values:
            self.fetch_interval = int(sequence.values[0])
        else:
            return DEFAULT_FETCH_INTERVAL
        return self.fetch_interval

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_sort_condition(self):
        """
        Record the SQL for which the total number of template queuing changes during execution
        """
        sort_condition = {'sort_spill': False, 'hash_spill': False}
        sort_spill_sequence = dai.get_metric_sequence("gaussdb_statement_sort_spill", self.query_start_time,
                                                      self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
            query={self.slow_sql_instance.query}).fetchone()
        hash_spill_sequence = dai.get_metric_sequence("gaussdb_statement_hash_spill", self.query_start_time,
                                                      self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
            query={self.slow_sql_instance.query}).fetchone()
        if sort_spill_sequence.values and max(int(item) for item in sort_spill_sequence.values) > 0:
            sort_condition['sort_spill'] = True
        if hash_spill_sequence.values and max(int(item) for item in hash_spill_sequence.values) > 0:
            sort_condition['hash_spill'] = True
        return sort_condition

    @exception_follower(output=LockInfo)
    @exception_catcher
    def acquire_lock_info(self) -> LockInfo:
        """Get lock information during slow SQL execution
        """
        blocks_info = LockInfo()
        locked_query = self.slow_sql_instance.query.replace('\n', ' ')
        lock_sequence = dai.get_metric_sequence("pg_lock_sql_locked_times", self.query_start_time,
                                                self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
            locked_query=f"{locked_query}").fetchone()
        if lock_sequence.values:
            blocks_info.locker_query = lock_sequence.labels.get('locker_query', 'Unknown')
            blocks_info.locker_query_start = lock_sequence.labels.get('locker_query_start', 'Unknown')
        return blocks_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_tables_structure_info(self) -> list:
        """Acquire table structure information related to slow query"""
        table_structure = []
        if not self.slow_sql_instance.tables_name:
            return table_structure
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
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                live_tup_info = dai.get_metric_sequence("pg_tables_structure_n_live_tup",
                                                        self.query_start_time,
                                                        self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                dead_tup_info = dai.get_metric_sequence("pg_tables_structure_n_dead_tup",
                                                        self.query_start_time,
                                                        self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_vacuum_info = dai.get_metric_sequence("pg_tables_structure_last_vacuum",
                                                           self.query_start_time,
                                                           self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_autovacuum_info = dai.get_metric_sequence("pg_tables_structure_last_autovacuum",
                                                               self.query_start_time,
                                                               self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_analyze_info = dai.get_metric_sequence("pg_tables_structure_last_analyze",
                                                            self.query_start_time,
                                                            self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_autoanalyze_info = dai.get_metric_sequence("pg_tables_structure_last_autoanalyze",
                                                                self.query_start_time,
                                                                self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                pg_table_size_info = dai.get_metric_sequence("pg_tables_size_bytes", self.query_start_time,
                                                             self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                index_number_info = dai.get_metric_sequence("pg_index_idx_scan", self.query_start_time,
                                                            self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(tablename=f"{table_name}").fetchall()
                redundant_index_info = dai.get_metric_sequence("pg_never_used_indexes_index_size",
                                                               self.query_start_time,
                                                               self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchall()
                if dead_rate_info.values:
                    table_info.dead_rate = round(float(dead_rate_info.values[0]), 4)
                if live_tup_info.values:
                    table_info.live_tuples = int(live_tup_info.values[0])
                if dead_tup_info.values:
                    table_info.dead_tuples = int(dead_tup_info.values[0])
                if dead_rate_info.values:
                    table_info.dead_rate = float(dead_rate_info.values[0])
                if last_analyze_info.values:
                    filtered_values = [float(item) for item in last_analyze_info.values if not math.isnan(float(item))]
                    table_info.analyze = int(max(filtered_values)) if filtered_values else 0
                if last_autoanalyze_info.values:
                    filtered_values = [float(item) for item in last_autoanalyze_info.values if not math.isnan(float(item))]
                    table_info.last_autoanalyze = int(max(filtered_values)) if filtered_values else 0
                if last_vacuum_info.values:
                    filtered_values = [float(item) for item in last_vacuum_info.values if not math.isnan(float(item))]
                    table_info.vacuum = int(max(filtered_values)) if filtered_values else 0
                if last_autovacuum_info.values:
                    filtered_values = [float(item) for item in last_autovacuum_info.values if not math.isnan(float(item))]
                    table_info.last_autovacuum = int(max(filtered_values)) if filtered_values else 0
                if pg_table_size_info.values:
                    table_info.table_size = round(float(max(pg_table_size_info.values)) / 1024 / 1024, 4)
                if index_number_info:
                    table_info.index = {item.labels['relname']: parse_field_from_indexdef(item.labels['indexdef'])
                                        for item in index_number_info if item.labels}
                if redundant_index_info:
                    table_info.redundant_index = [item.labels['indexrelname'] for item in redundant_index_info if
                                                  item.labels]
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
                f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                name=f"{parameter}").fetchone()
            if sequence.labels:
                pg_setting.name = parameter
                pg_setting.vartype = sequence.labels['vartype']
                if pg_setting.vartype in ('integer', 'int64', 'bool'):
                    pg_setting.setting = int(sequence.values[-1])
                if pg_setting.vartype == 'real':
                    pg_setting.setting = float(sequence.values[-1])
            pg_settings[parameter] = pg_setting
        return pg_settings

    @exception_follower(output=DatabaseInfo)
    @exception_catcher
    def acquire_database_info(self) -> DatabaseInfo:
        """Acquire table database information related to slow query"""
        database_info = DatabaseInfo()
        days_time_interval = 24 * 60 * 60
        cur_tps_sequences = dai.get_metric_sequence("gaussdb_qps_by_instance", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        his_tps_sequences = dai.get_metric_sequence("gaussdb_qps_by_instance",
                                                    self.query_start_time - timedelta(seconds=days_time_interval),
                                                    self.query_end_time - timedelta(
                                                        seconds=days_time_interval)).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        max_conn_sequence = dai.get_metric_sequence("pg_connections_max_conn", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        used_conn_sequence = dai.get_metric_sequence("pg_connections_used_conn", self.query_start_time,
                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        if his_tps_sequences.values:
            database_info.history_tps = [float(item) for item in his_tps_sequences.values]
        if cur_tps_sequences.values:
            database_info.current_tps = [float(item) for item in cur_tps_sequences.values]
        if max_conn_sequence.values:
            database_info.max_conn = int(max(max_conn_sequence.values))
        if used_conn_sequence.values:
            database_info.used_conn = int(max(used_conn_sequence.values))
        return database_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_wait_event(self) -> list:
        """Acquire database wait events"""
        wait_event = []
        pg_wait_event_spike_info = dai.get_metric_sequence("pg_wait_event_spike",
                                                           self.query_start_time,
                                                           self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        for sequence in pg_wait_event_spike_info:
            wait_event_info = WaitEvent()
            # Check whether the wait event occurs during this event
            if not sequence.values or sum(sequence.values) == 0:
                continue
            wait_event_info.node_name = sequence.labels['nodename']
            wait_event_info.type = sequence.labels['type']
            wait_event_info.event = sequence.labels['event']
            wait_event_info.last_updated = int(max(sequence.values))
            wait_event.append(wait_event_info)
        return wait_event

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
        load_average_info1 = dai.get_metric_sequence("load_average1", self.query_start_time,
                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        io_queue_number_info = dai.get_metric_sequence("os_io_queue_number", self.query_start_time,
                                                       self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        process_fds_rate_info = dai.get_metric_sequence("os_process_fds_rate", self.query_start_time,
                                                        self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        cpu_process_number_info = dai.get_metric_sequence("os_cpu_processor_number", self.query_start_time,
                                                          self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        db_cpu_usage_info = dai.get_metric_sequence("db_cpu_usage", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        db_mem_usage_info = dai.get_metric_sequence("db_mem_usage", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        if iops_info.values:
            system_info.iops = [int(item) for item in iops_info.values]
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_disk_iops' data.")
        if process_fds_rate_info.values:
            system_info.process_fds_rate = [round(float(item), 4) for item in process_fds_rate_info.values]
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_process_fds_rate' data.")
        if cpu_process_number_info:
            system_info.cpu_core_number = int(cpu_process_number_info.values[-1])
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_cpu_processor_number' data.")
        if ioutils_info:
            ioutils_dict = {item.labels['device']: round(float(max(item.values)), 4) for item in ioutils_info if
                            item.labels}
            system_info.ioutils = ioutils_dict
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_disk_ioutils' data.")
        if disk_usage_info:
            disk_usage_dict = {item.labels['device']: round(float(max(item.values)), 4) for item in disk_usage_info if
                               item.labels}
            system_info.disk_usage = disk_usage_dict
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_disk_usage' data.")
        if db_cpu_usage_info.values:
            system_info.db_cpu_usage = [round(float(item), 4) for item in db_cpu_usage_info.values]
        else:
            logging.debug("[SLOW SQL][DATA SOURCE]: Not get 'db_cpu_usage' data.")
        if db_mem_usage_info.values:
            system_info.db_mem_usage = [round(float(item), 4) for item in db_mem_usage_info.values]
        else:
            logging.debug("[SLOW SQL][DATA SOURCE]: Not get 'db_mem_usage' data.")
        if cpu_usage_info.values:
            system_info.cpu_usage = [round(float(item), 4) for item in cpu_usage_info.values]
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_cpu_usage' data.")
        if mem_usage_info.values:
            system_info.mem_usage = [round(float(item), 4) for item in mem_usage_info.values]
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_mem_usage' data.")
        if load_average_info1.values:
            system_info.load_average1 = [round(float(item), 4) for item in load_average_info1.values]
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'load_average1' data.")
        if io_queue_number_info:
            system_info.io_queue_number = {item.labels['device']: round(float(max(item.values)), 4) for item in
                                           io_queue_number_info if
                                           item.labels}
        else:
            logging.warning("[SLOW SQL][DATA SOURCE]: Not get 'os_io_queue_number' data.")
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
        node_network_receive_packets_info = dai.get_metric_sequence('os_network_receive_packets',
                                                                    self.query_start_time,
                                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_transmit_packets_info = dai.get_metric_sequence('os_network_transmit_packets',
                                                                     self.query_start_time,
                                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        if node_network_receive_drop_info.values:
            network_info.receive_drop = round(float(max(node_network_receive_drop_info.values)), 4)
        if node_network_transmit_drop_info.values:
            network_info.transmit_drop = round(float(max(node_network_transmit_drop_info.values)), 4)
        if node_network_receive_packets_info.values:
            network_info.receive_packets = round(float(max(node_network_receive_packets_info.values)), 4)
        if node_network_transmit_packets_info.values:
            network_info.transmit_packets = round(float(max(node_network_transmit_packets_info.values)), 4)

        return network_info

    @exception_follower(output=str)
    @exception_catcher
    def acquire_rewritten_sql(self) -> str:
        #if not self.slow_sql_instance.track_parameter or \
        #        not self.slow_sql_instance.query.strip().upper().startswith('SELECT'):
        #    return ''
        #rewritten_flags = []
        #rewritten_sql = toolkit_rewrite_sql(self.slow_sql_instance.db_name,
        #                                    self.slow_sql_instance.query,
        #                                    rewritten_flags=rewritten_flags,
        #                                    if_format=False)
        #flag = rewritten_flags[0]
        #if not flag:
        #    return ''
        #rewritten_sql = rewritten_sql.replace('\n', ' ')
        #rewritten_sql_plan = self.acquire_plan(rewritten_sql)
        #old_sql_plan_parse = plan_parsing.Plan()
        #rewritten_sql_plan_parse = plan_parsing.Plan()
        ## Abandon the rewrite if the rewritten statement does not perform as well as the original statement.
        #old_sql_plan_parse.parse(self.slow_sql_instance.query_plan)
        #rewritten_sql_plan_parse.parse(rewritten_sql_plan)
        #if old_sql_plan_parse.root_node.total_cost > rewritten_sql_plan_parse.root_node.total_cost:
        #    return rewritten_sql
        return ''

    @exception_follower(output=str)
    @exception_catcher
    def acquire_recommend_index(self) -> str:
        if not self.slow_sql_instance.query.strip().upper().startswith('SELECT'):
            return ''
        recommend_indexes = []
        if self.slow_sql_instance.track_parameter:
            query = self.slow_sql_instance.query.replace('\'', '\'\'')
            stmt = "set current_schema=%s;select * from gs_index_advise('%s')" % \
                   (self.slow_sql_instance.schema_name, query)
            rows = agent_rpc_client.call('query_in_database',
                                         stmt,
                                         self.slow_sql_instance.db_name,
                                         return_tuples=True)
            for row in rows:
                if row[2]:
                    index = Index()
                    index.db_name = self.slow_sql_instance.db_name
                    index.schema_name = row[0]
                    index.table_name = row[1]
                    index.column_name = row[2]
                    recommend_indexes.append(str(index))
        else:
            workload = WorkLoad([self.slow_sql_instance.query])
            db_name = self.slow_sql_instance.db_name
            schema_name = ','.join(self.slow_sql_instance.tables_name.keys())
            executor = RpcExecutor(db_name, None, None, None, None, schema_name)
            candidate_indexes = generate_candidate_indexes(workload, executor, 1, 10, True)
            for candidate_index in candidate_indexes:
                index = Index()
                index.db_name = self.slow_sql_instance.db_name
                index.schema_name = ''
                index.schema_name = candidate_index.get_table()
                index.column_name = candidate_index.get_columns()
        return ';'.join(recommend_indexes)

    @exception_follower(output=list)
    @exception_catcher
    def acquire_timed_task(self) -> list:
        timed_task_list = []
        sequences = dai.get_metric_sequence('db_timed_task_failure_count', self.query_start_time,
                                            self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
            dbname=self.slow_sql_instance.db_name).fetchall()
        sequences = [sequence for sequence in sequences if sequence.labels]
        for sequence in sequences:
            timed_task = TimedTask()
            timed_task.job_id = sequence.labels['job_id']
            timed_task.priv_user = sequence.labels['priv_user']
            timed_task.dbname = sequence.labels['dbname']
            timed_task.job_status = sequence.labels['job_status']
            timed_task.last_start_date = to_ts(sequence.labels['last_start_date']) * 1000 \
                if not math.isnan(sequence.labels['last_start_date']) else 0
            timed_task.last_end_date = to_ts(sequence.labels['last_end_date']) * 1000 \
                if not math.isnan(sequence.labels['last_end_date']) else 0
            timed_task_list.append(timed_task)
        return timed_task_list

    @exception_follower(output=list)
    @exception_catcher
    def acquire_abnormal_process(self):
        # todo: need add metric
        process_list = dai.get_metric_sequence('abnormal_process', self.query_start_time,
                                               self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        return process_list
