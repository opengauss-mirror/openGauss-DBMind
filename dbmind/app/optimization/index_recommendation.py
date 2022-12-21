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
from dbmind.app.optimization._index_recommend_client_driver import RpcExecutor
from dbmind.common.parser.sql_parsing import fill_value, standardize_sql
from dbmind.components.extract_log import get_workload_template
from dbmind.components.index_advisor import index_advisor_workload, process_bar
from dbmind.components.fetch_statement import fetch_statement
from dbmind.components.fetch_statement.fetch_statement import is_valid_statement
from dbmind.service import dai

index_advisor_workload.MAX_INDEX_NUM = global_vars.configs.getint(
    'SELF-OPTIMIZATION', 'max_index_num', fallback=10
)
index_advisor_workload.MAX_INDEX_STORAGE = global_vars.configs.getint(
    'SELF-OPTIMIZATION', 'max_index_storage', fallback=100
)
process_bar.print = lambda *args, **kwargs: None

FETCH_UNIQUE_QUERY_FROM_STATEMENT = "select query, n_calls, execution_time from dbe_perf.statement " \
                                    "order by execution_time desc;"
MIN_HISTORY_QUERY_NUM = 20


class TemplateArgs:
    def __init__(self, max_reserved_period, max_template_num):
        self.max_reserved_period = max_reserved_period
        self.max_template_num = max_template_num


def need_recommend_index():
    return True


def get_database_schemas():
    database_schemas = defaultdict(list)
    results = dai.get_latest_metric_value('pg_database_size_bytes').fetchall()
    for res in results:
        if not res.labels:
            continue
        db_name = res.labels['datname']
        executor = RpcExecutor(db_name, None, None, None, None, 'public')
        schemas = executor.execute_sqls(["select distinct(nspname) FROM pg_namespace nsp JOIN pg_class rel ON "
                                         "nsp.oid = rel.relnamespace WHERE nspname NOT IN "
                                         "('pg_catalog', 'information_schema','snapshot', "
                                         "'dbe_pldeveloper', 'db4ai', 'dbe_perf') AND rel.relkind = 'r';"])
        for schema_tuple in schemas:
            database_schemas[db_name].append(schema_tuple[0])
    return database_schemas


def is_rpc_available(db_name):
    try:
        global_vars.agent_rpc_client.call('query_in_database',
                                          'select 1',
                                          db_name,
                                          return_tuples=True)
        return True
    except Exception as e:
        logging.warning(e)
        global_vars.agent_rpc_client = None
        return False


def rpc_index_advise(executor, templates):
    # only single threads can be used
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


def do_index_recomm(templatization_args, db_name, schemas, database_templates, optimization_interval):
    if not is_rpc_available(db_name):
        return
    executor = RpcExecutor(db_name, None, None, None, None, '"$user",pubic,' + schemas)
    database_templates, source = get_queries(database_templates, db_name, schemas, optimization_interval,
                                             templatization_args)

    detail_info = rpc_index_advise(executor, database_templates)
    # Query source is used to record queries that cannot be distinguished from different databases.
    detail_info['stmt_source'] = source
    detail_info['db_name'] = db_name
    detail_info['host'] = ''
    for database_info in dai.get_latest_metric_value('pg_database_all_size').fetchall():
        if database_info.labels:
            detail_info['host'] = database_info.labels['from_instance']
            break
    return detail_info, {db_name: database_templates}


def get_queries(database_templates, db_name, schemas, optimization_interval, templatization_args):
    source = 'tsdb_metric'
    start_time = datetime.now() - timedelta(seconds=optimization_interval)
    end_time = datetime.now()
    queries = []
    for pg_sql_statement_full_count in dai.get_metric_sequence('pg_sql_statement_full_count',
                                                               start_time,
                                                               end_time).fetchall():
        if pg_sql_statement_full_count.labels and \
                pg_sql_statement_full_count.labels['datname'] == db_name:
            query = standardize_sql(fill_value(pg_sql_statement_full_count.labels['query']))
            queries.append(query)
    queries = list(queries)
    if not queries:
        source = 'asp'
        _executor = RpcExecutor('postgres', None, None, None, None, 'public')
        queries = []
        if _executor.execute_sqls(['show enable_asp'])[0] == ('on',):
            queries.extend(fetch_statement.fetch_statements(_executor, 'asp', db_name, '',
                                                            start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                            end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
                                                            )
                           )
        else:
            source = 'history'
            database_templates = {}
            for schema in schemas.split(','):
                for query in fetch_statement.fetch_statements(_executor, 'history', db_name, schema):
                    if is_dml(query):
                        queries.append(query)
            counter = Counter(queries)
            # The number of unique_query must be greater than MIN_HISTORY_QUERY_NUM
            if len(counter.keys()) < MIN_HISTORY_QUERY_NUM:
                executor = RpcExecutor(db_name, None, None, None, None, schemas)
                source = 'statement'
                results = _executor.execute_sqls([FETCH_UNIQUE_QUERY_FROM_STATEMENT])
                valid_queries = dict()
                for query, n_calls, execution_time in results:
                    if not is_dml(query):
                        continue
                    if is_valid_statement(executor, query):
                        valid_queries[query] = {'samples': [query], 'cnt': n_calls, 'update': [time.time()]}
                    if len(valid_queries.keys()) > templatization_args.max_template_num:
                        break
                database_templates = valid_queries
                queries = []
    get_workload_template(database_templates, queries, templatization_args)
    return database_templates, source
