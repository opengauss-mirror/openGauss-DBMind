# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
"""The Insight"""

import logging
import re
from datetime import datetime

from dbmind import global_vars
from dbmind.common.opengauss_driver import Driver
from dbmind.common.utils.checking import split_ip_port, prepare_ip, WITH_PORT
from dbmind.service import dai

from .utils import get_detector_params


def rpc_is_available(rpc):
    """ Test if rpc is available"""
    try:
        result = rpc.call("query_in_database", "select 1", "postgres", return_tuples=True)
        return result[0][0] == 1
    except Exception as e:
        logging.exception(e)
        return False


def get_rpc(main_instance, main_ip_list):
    """ get the very rpc within the agents."""
    agent_map = global_vars.agent_proxy.agent_get_all()
    if main_instance in agent_map:
        try:
            rpc = global_vars.agent_proxy.get(main_instance)
        except TypeError:
            rpc = global_vars.agent_proxy.get()

        if rpc_is_available(rpc):
            return rpc

    for agent_addr, agent_list in agent_map.items():
        for agent in agent_list:
            if split_ip_port(agent)[0] in main_ip_list:
                try:
                    rpc = global_vars.agent_proxy.get(agent_addr)
                except TypeError:
                    rpc = global_vars.agent_proxy.get()

                if rpc_is_available(rpc):
                    return rpc

    return None


def parse_deadlock_log(content):
    lock_pattern = re.compile(
        "Process (\d+?) waits for (\S+?) on transaction (\d+?); "
        "blocked by process (\d+?). "
    )
    sql_pattern = re.compile("Process (\d+?): ((?:(?!Process).)*)")

    lock_matches = lock_pattern.findall(content)
    sql_matches = sql_pattern.findall(content)

    locks = [
        {"tid": match[0], "lock": match[1], "transaction_id": match[2], "blocked_by": match[3]}
        for match in lock_matches
    ]
    sqls = [{"tid": match[0], "sql": match[1]} for match in sql_matches]

    return locks, sqls


class Executor:
    """ Try to get the available driver or RPC"""
    def __init__(self, driver, main_instance, main_ip_list):
        self.driver = driver
        self.main_instance = main_instance
        self.main_ip_list = main_ip_list
        self.rpc = get_rpc(main_instance, main_ip_list)

    def call(self, stmt, return_tuples=False):
        if isinstance(self.driver, Driver):
            return self.driver.query(stmt, return_tuples=return_tuples)
        elif self.rpc:
            return self.rpc.call("query_in_database", stmt, "postgres", return_tuples=return_tuples)

        return None


class DriverBased:
    """The Abstract Insight based on db driver or rpc"""
    def __init__(self, driver=None, main_instance=None, main_ip_list=None):
        self.driver = driver
        self.main_instance = main_instance
        self.main_ip_list = main_ip_list
        self.executor = Executor(self.driver, self.main_instance, self.main_ip_list)
        self.statement = None

    def init_driver(self):
        self.executor = Executor(self.driver, self.main_instance, self.main_ip_list)


class TableSpace(DriverBased):
    """Check the table space information"""
    def __init__(self, driver=None, main_instance=None, main_ip_list=None):
        super().__init__(
            driver=driver,
            main_instance=main_instance,
            main_ip_list=main_ip_list
        )
        self.statement = (
            """
            WITH table_size AS (
                SELECT 
                    nsp.nspname AS nspname, 
                    rel.relname AS relname, 
                    pg_catalog.pg_total_relation_size(rel.oid)  / 1024 / 1024 AS totalsize, 
                    pg_catalog.pg_relation_size(rel.oid) / 1024 / 1024 AS relsize, 
                    pg_catalog.pg_indexes_size(rel.oid) / 1024 / 1024 AS indexsize 
                FROM 
                    pg_catalog.pg_namespace nsp JOIN pg_catalog.pg_class rel ON nsp.oid = rel.relnamespace 
                WHERE 
                    nspname NOT IN ('pg_catalog', 'information_schema','snapshot', 'dbe_pldeveloper', 'db4ai', 'dbe_perf') 
                    AND 
                    rel.relkind = 'r' 
                ORDER BY 
                    totalsize DESC 
                LIMIT 10
            ) 
            SELECT 
                table_size.nspname, 
                table_size.relname, 
                table_size.totalsize, 
                table_size.relsize, 
                table_size.indexsize, 
                table_tuple.n_live_tup, 
                table_tuple.n_dead_tup 
            FROM 
                pg_catalog.pg_stat_user_tables table_tuple JOIN table_size ON table_size.relname = table_tuple.relname AND table_size.nspname = table_tuple.schemaname;
            """
        )

    def check(self):
        ans = self.executor.call(self.statement, return_tuples=True)
        if ans is None:
            self.init_driver()

        ans = self.executor.call(self.statement, return_tuples=True)
        if not ans:
            return None

        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        res = list()
        for row in ans:
            res.append(
                {"time": time,
                 "nspname": row[0],
                 "relname": row[1],
                 "totalsize_in_mb": row[2],
                 "relsize_in_mb": row[3],
                 "indexsize_in_mb": row[4],
                 "n_live_tup": row[5],
                 "n_dead_tup": row[6]}
            )

        return sorted(res, key=lambda x: -x["totalsize_in_mb"])


class WaitStatus(DriverBased):
    """Check the table space information"""
    def __init__(self, driver=None, main_instance=None, main_ip_list=None):
        super().__init__(
            driver=driver,
            main_instance=main_instance,
            main_ip_list=main_ip_list
        )
        self.long_transaction_threshold = get_detector_params("slow_sql_detector", "high")
        self.from_instance = main_instance
        self.statement = (
            f"""
            SELECT 
                pid, psa.sessionid,  unique_sql_id, datname,
                psa.query_id, query, state, wait_status,
                wait_event, lockmode, block_sessionid, 
                extract(epoch FROM pg_catalog.now() - xact_start) AS count
            FROM 
                pg_catalog.pg_stat_activity psa JOIN pg_catalog.pg_thread_wait_status ptws ON psa.sessionid = ptws.sessionid 
            WHERE 
                psa.sessionid != pg_catalog.pg_current_sessid() 
                AND 
                pg_catalog.length(query) > 0 
                AND 
                (
                    state in ('idle in transaction')
                    OR 
                    (
                        unique_sql_id != 0
                        AND 
                        state in ('active')
                    )
                )
                AND 
                count > {self.long_transaction_threshold};
            """
        )

    def check(self):
        ans = self.executor.call(self.statement, return_tuples=True)
        if ans is None:
            self.init_driver()
            ans = self.executor.call(self.statement, return_tuples=True)
            if not ans:
                return None

        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        res = list()
        for row in ans:
            res.append(
                {"time": time,
                 "from_instance": self.from_instance,
                 "pid": row[0],
                 "sessionid": row[1],
                 "unique_sql_id": row[2],
                 "datname": row[3],
                 "query_id": row[4],
                 "query": row[5],
                 "state": row[6],
                 "wait_status": row[7],
                 "wait_event": row[8],
                 "lockmode": row[9],
                 "block_sessionid": row[10],
                 "duration_in_seconds": row[11]}
            )

        return sorted(res, key=lambda x: x["time"])


class TSDBBased:
    """The Abstract Insight based on tsdb"""
    def __init__(self, recent_start=None, recent_end=None,
                 beginning_start=None, beginning_end=None,
                 original_start=None, step=None, main_instance=None):
        self.recent_start = recent_start
        self.recent_end = recent_end
        self.beginning_start = beginning_start
        self.beginning_end = beginning_end
        self.original_start = original_start
        self.step = step
        if isinstance(main_instance, str) and WITH_PORT.match(main_instance):
            host, port = split_ip_port(main_instance)
            self.from_instance_like = f"{prepare_ip(host)}(:{port})|{host}"
        else:
            self.from_instance_like = main_instance

    def query(self, metric_name, timestamp, step):
        fetcher = dai.get_metric_sequence(
            metric_name,
            datetime.fromtimestamp(timestamp),
            datetime.fromtimestamp(timestamp),
            step=step * 1000
        )
        if self.from_instance_like:
            fetcher = fetcher.from_server_like(self.from_instance_like)

        return fetcher.fetchall()

    def query_range(self, metric_name, start_time, end_time, step):
        fetcher = dai.get_metric_sequence(
            metric_name,
            datetime.fromtimestamp(start_time),
            datetime.fromtimestamp(end_time),
            step=step * 1000
        )
        if self.from_instance_like:
            fetcher = fetcher.from_server_like(self.from_instance_like)

        return fetcher.fetchall()


class TempFilesSnapshot(TSDBBased):
    """Check the temporary files"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = "pg_temp_files_count"

    def check(self):
        sequences = self.query(
            self.metric_name,
            self.original_start,
            self.step
        )

        time = datetime.fromtimestamp(self.original_start).strftime("%Y-%m-%d %H:%M:%S")
        res = list()
        for sequence in sequences:
            labels = sequence.labels
            tid = labels.get("tid")
            query = labels.get("query")
            count = sequence.values[0]
            res.append(
                {"time": time,
                 "tid": tid,
                 "query": query,
                 "count": count}
            )

        return sorted(res, key=lambda x: -x["count"])


class LongTransactionMemoryContextSnapshot(TSDBBased):
    """Check the memory context when long transaction happens."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = "pg_long_xact_mem_ctx_size"

    def check(self):
        sequences = self.query_range(
            self.metric_name,
            self.beginning_start,
            self.beginning_end,
            self.step
        )
        res = list()
        for sequence in sequences:
            labels = sequence.labels
            session_id = labels.get("sessionid")
            contextname = labels.get("contextname")
            start = datetime.fromtimestamp(sequence.timestamps[0] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            end = datetime.fromtimestamp(sequence.timestamps[-1] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            res.append(
                {"start": start,
                 "end": end,
                 "sessionid": session_id,
                 "contextname": contextname,
                 "size_in_mb": sum(sequence.values) / len(sequence.values)}
            )

        return sorted(res, key=lambda x: -x["size_in_mb"])


class SessionMemoryDetailSnapshot(TSDBBased):
    """Check the session memory context."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = "pg_session_memory_detail_size"

    def check(self):
        sequences = self.query(
            self.metric_name,
            self.original_start,
            self.step
        )

        time = datetime.fromtimestamp(self.original_start).strftime("%Y-%m-%d %H:%M:%S")
        res = []
        for sequence in sequences:
            labels = sequence.labels
            contextname = labels.get("contextname")
            size = sequence.values[0] / 1024 / 1024
            res.append(
                {"time": time,
                 "contextname": contextname,
                 "size_in_mb": size}
            )

        return sorted(res, key=lambda x: -x["size_in_mb"])


class SharedMemoryDetailSnapshot(TSDBBased):
    """Check the shared memory context."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = "pg_shared_memory_detail_size"

    def check(self):
        sequences = self.query(
            self.metric_name,
            self.original_start,
            self.step
        )

        time = datetime.fromtimestamp(self.original_start).strftime("%Y-%m-%d %H:%M:%S")
        res = []
        for sequence in sequences:
            labels = sequence.labels
            contextname = labels.get("contextname")
            size = sequence.values[0] / 1024 / 1024
            res.append(
                {"time": time,
                 "contextname": contextname,
                 "size_in_mb": size}
            )

        return sorted(res, key=lambda x: -x["size_in_mb"])


class DeadlockLoop(TSDBBased):
    """Check the linked locks from beginning to end to form a loop."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = "opengauss_log_deadlock_count"

    def check(self):
        sequences = self.query_range(
            self.metric_name,
            self.beginning_start,
            self.beginning_end,
            self.step
        )

        res = []
        for sequence in sequences:
            labels = sequence.labels
            content = labels.get("content")
            start = datetime.fromtimestamp(sequence.timestamps[0] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            end = datetime.fromtimestamp(sequence.timestamps[-1] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            locks, sqls = parse_deadlock_log(content)
            if locks and sqls:
                res.append(
                    {"start": start,
                     "end": end,
                     "locks": locks,
                     "sqls": sqls}
                )

        return res


class BlockingLock(TSDBBased):
    """Check the linked locks from beginning to end to form a loop."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = "opengauss_log_lock_wait_timeout"

    def check(self):
        sequences = self.query_range(
            self.metric_name,
            self.beginning_start,
            self.beginning_end,
            self.step
        )

        res = []
        for sequence in sequences:
            labels = sequence.labels
            thread = labels.get("thread")
            statement = labels.get("statement")
            lockmode = labels.get("lockmode")
            start = datetime.fromtimestamp(sequence.timestamps[0] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            end = datetime.fromtimestamp(sequence.timestamps[-1] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            res.append(
                {"start": start,
                 "end": end,
                 "thread": thread,
                 "statement": statement,
                 "lockmode": lockmode}
            )

        return res


class ActiveSqlTime(TSDBBased):
    """Check the Active SQL Time."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric_name = "pg_sql_active_time"

    def check(self):
        sequences = self.query(
            self.metric_name,
            self.original_start,
            self.step
        )

        time = datetime.fromtimestamp(self.original_start).strftime("%Y-%m-%d %H:%M:%S")
        res = []
        for sequence in sequences:
            labels = sequence.labels
            unique_sql_id = labels.get("unique_sql_id")
            sql_time_in_seconds = sequence.values[0]
            if sql_time_in_seconds > 0:
                res.append(
                    {"time": time,
                     "unique_sql_id": unique_sql_id,
                     "sql_time_in_seconds": sql_time_in_seconds}
                )

        return sorted(res, key=lambda x: -x["sql_time_in_seconds"])


class CoreDumpSqlId:
    """Check the unique_sql_id and debug_query_id from opengauss_log_ffic metric."""
    def __init__(self, time=None, metric_filter=None):
        self.metric_name = "opengauss_log_ffic"
        self.metric_filter = metric_filter
        self.time = time

    def check(self):
        res = []
        time = datetime.fromtimestamp(self.time).strftime("%Y-%m-%d %H:%M:%S")
        unique_sql_id = self.metric_filter.get("unique_sql_id")
        debug_query_id = self.metric_filter.get("debug_query_id")
        if unique_sql_id or debug_query_id:
            res.append({"time": time})
            if unique_sql_id:
                res[0]["unique_sql_id"] = unique_sql_id

            if debug_query_id:
                res[0]["debug_query_id"] = debug_query_id

        return res
