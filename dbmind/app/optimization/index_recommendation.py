# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import time

from dbmind import global_vars
from dbmind.common.parser.sql_parsing import fill_value, standardize_sql
from dbmind.components.extract_log import get_workload_template
from dbmind.components.index_advisor import index_advisor_workload, process_bar
from dbmind.components.fetch_statement import fetch_statement
from dbmind.components.fetch_statement.fetch_statement import is_valid_statement
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils

from .index_recommendation_rpc_executor import RpcExecutor

process_bar.print = lambda *args, **kwargs: None

FETCH_UNIQUE_QUERY_FROM_STATEMENT = "select query, n_calls, execution_time from dbe_perf.statement " \
                                    "order by execution_time desc;"
MIN_HISTORY_QUERY_NUM = 20


class TemplateArgs:
    def __init__(self, max_reserved_period, max_template_num):
        self.max_reserved_period = max_reserved_period
        self.max_template_num = max_template_num


class QUERY_SOURCE:
    TSDB = 'tsdb_metric'
    ASP = 'asp'
    HISTORY = 'history'
    STATEMENT = 'statement'


def need_recommend_index():
    return True


def get_database_schemas():
    """Get database name and schema name.
    :return: a dict, the key is instance address, the value is also a
    dict whose key is database name and value is schema names.
    e.g.,
    {'127.0.0.1:1234': {'db1': 'schema1,schema2,schema3', 'db2': 'schema1'},
     '127.0.0.1:2345': {'db1': 'schema1,schema2', 'db2': 'schema1'}
    }
    """
    sequences = dai.get_latest_metric_value('pg_database_size_bytes').fetchall()
    rv = defaultdict(dict)
    for sequence in sequences:
        labels = sequence.labels
        if (not labels) or ('datname' not in labels):
            continue
        db_name = sequence.labels['datname']
        address = SequenceUtils.from_server(sequence)

        if db_name in rv[address]:
            # This means we have recorded all schemas for this
            # database, so we can skip the following steps.
            continue

        try:
            with global_vars.agent_proxy.context(address):
                executor = RpcExecutor(db_name, None, None, None, None, 'public')
                schemas = executor.execute_sqls(
                    ["select distinct(nspname) FROM pg_namespace nsp JOIN pg_class rel ON "
                     "nsp.oid = rel.relnamespace WHERE nspname NOT IN "
                     "('pg_catalog', 'information_schema','snapshot', "
                     "'dbe_pldeveloper', 'db4ai', 'dbe_perf') AND rel.relkind = 'r';"]
                )

                rv[address][db_name] = ','.join(map(lambda l: l[0], schemas))
        except global_vars.agent_proxy.RPCAddressError as e:
            logging.warning(e)
            continue

    return rv


def is_rpc_available(db_name):
    try:
        global_vars.agent_proxy.call('query_in_database',
                                     'select 1',
                                     db_name,
                                     return_tuples=True)
        return True
    except Exception as e:
        logging.warning(e)
        global_vars.agent_proxy = None
        return False


def rpc_index_advise(executor,
                     templates,
                     max_index_num=None,
                     max_index_storage=None):
    if max_index_num is None:
        max_index_num = global_vars.dynamic_configs.get_int_or_float(
            'SELF-OPTIMIZATION', 'max_index_num', fallback=10)
    if max_index_storage is None:
        max_index_storage = global_vars.dynamic_configs.get_int_or_float(
            'SELF-OPTIMIZATION', 'max_index_storage', fallback=100)
    # only single threads can be used
    index_advisor_workload.MAX_INDEX_NUM = max_index_num
    index_advisor_workload.MAX_INDEX_STORAGE = max_index_storage

    index_advisor_workload.get_workload_costs = index_advisor_workload.get_plan_cost
    detail_info, _, _ = index_advisor_workload.index_advisor_workload({'historyIndexes': {}}, executor, templates,
                                                                      multi_iter_mode=True, show_detail=True,
                                                                      n_distinct=1, reltuples=10,
                                                                      use_all_columns=True, improved_rate=0.05,
                                                                      max_candidate_columns=40)
    return detail_info


def is_dml(query):
    if query.lower().startswith(('select', 'delete', 'update', 'insert', 'with')):
        return True
    return False


def do_index_recomm(templatization_args, instance, db_name, schemas, database_templates, optimization_interval):
    if (not global_vars.agent_proxy.switch_context(instance)
            or not is_rpc_available(db_name)):
        return
    executor = RpcExecutor(db_name, None, None, None, None, '"$user",public,' + schemas)
    database_templates, source = get_queries(database_templates, instance, db_name, schemas, optimization_interval,
                                             templatization_args)

    detail_info = rpc_index_advise(executor, database_templates)
    # Query source is used to record queries that cannot be distinguished from different databases.
    detail_info['stmt_source'] = source
    detail_info['db_name'] = db_name
    detail_info['instance'] = instance
    return detail_info, {db_name: database_templates}


def get_queries(database_templates, instance, db_name, schemas, optimization_interval, templatization_args):
    source = QUERY_SOURCE.TSDB
    start_time = datetime.now() - timedelta(seconds=optimization_interval)
    end_time = datetime.now()
    queries = []

    # firstly, try to fetch sequence from TSDB
    for pg_sql_statement_full_count in dai.get_metric_sequence(
            'pg_sql_statement_full_count',
            start_time,
            end_time).from_server(instance).fetchall():
        if (pg_sql_statement_full_count.labels
                and pg_sql_statement_full_count.labels['datname'] == db_name):
            query = standardize_sql(fill_value(pg_sql_statement_full_count.labels['query']))
            queries.append(query)

    _executor = RpcExecutor('postgres', None, None, None, None, 'public')
    # secondly, try to fetch queries from ASP
    if not queries:
        source = QUERY_SOURCE.ASP
        execute_result = _executor.execute_sqls(['show enable_asp'])
        if len(execute_result) and execute_result[0][0] == 'on':
            queries.extend(
                fetch_statement.fetch_statements(_executor, 'asp', db_name, '',
                                                 start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                 end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
                                                 )
            )
    # finally, try to fetch queries from dbe_perf.statement_history
    if not queries:
        source = QUERY_SOURCE.HISTORY
        for schema in schemas.split(','):
            for query in fetch_statement.fetch_statements(_executor, 'history', db_name, schema):
                if is_dml(query):
                    queries.append(query)
        counter = Counter(queries)
        # The number of unique_query must be greater than MIN_HISTORY_QUERY_NUM
        if len(counter.keys()) < MIN_HISTORY_QUERY_NUM:
            executor = RpcExecutor(db_name, None, None, None, None, schemas)
            source = QUERY_SOURCE.STATEMENT
            valid_queries = dict()
            execute_result = executor.execute_sqls(
                    [FETCH_UNIQUE_QUERY_FROM_STATEMENT]
            )
            for row in execute_result:
                if hasattr(row, '__iter__') and len(row) == 3:
                    query, n_calls, execution_time = row
                else:
                    logging.warning('Cannot get queries from the table '
                                    'statements for index advisor '
                                    'because cannot unpack the result %s.', row
                                    )
                    continue
                if not is_dml(query):
                    continue
                if is_valid_statement(executor, query):
                    valid_queries[query] = {
                        'samples': [query], 'cnt': n_calls, 'update': [time.time()]
                    }
                if len(valid_queries.keys()) > templatization_args.max_template_num:
                    break
            database_templates = valid_queries
            queries = []
    get_workload_template(database_templates, queries, templatization_args)
    return database_templates, source
