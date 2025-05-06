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
import argparse
import logging
import os
import sys
import time
from getpass import getpass

from psycopg2.extensions import parse_dsn, make_dsn

from dbmind import global_vars, constants
from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.opengauss_driver import Driver
from dbmind.common.utils.checking import date_type, path_type, CheckWordValid
from dbmind.common.utils.component import initialize_rpc_service, initialize_tsdb_param
from dbmind.common.utils.exporter import set_logger
from dbmind.common.parser.others import parse_mixed_quotes_string
from dbmind.common.utils import escape_single_quote

STATEMENT_LENGTH_LIMIT = 1000


def try_to_get_driver(url):
    driver = Driver()
    try:
        driver.initialize(url)
    except ConnectionError:
        return None, 'Error occurred when initialized the URL, exiting...\n'
    return driver, None


def try_to_initialize_rpc_and_tsdb():
    if not initialize_tsdb_param():
        return False, 'TSDB service does not exist, exiting...\n'
    if not initialize_rpc_service():
        return False, 'RPC service does not exist, exiting...\n'
    return True, None


def _add_quote(values, delimiter=','):
    # transfer string to list: 'a,b,c' to ('a', 'b', 'c')
    d = ''
    try:
        d = [f"'{escape_single_quote(item.strip())}'" for item in values.split(delimiter)]
        d = '(' + ','.join(d) + ')'
    except Exception:
        logging.error("error occured when transfer %s to list", values)
        return d
    return d


def _to_array(values):
    # transfer string to list: 'a,b,c' to ARRAY['a', 'b', 'c']
    d = ''
    try:
        d = [f"'{escape_single_quote(item.strip())}'" for item in parse_mixed_quotes_string(values)]
        d = 'ARRAY[' + ','.join(d) + ']'
    except Exception:
        logging.error("error occured when transfer %s to list", values)
        return d
    return d


def collect_statement_from_asp(databases, start_time, end_time, db_users, sql_types):
    stmt = f"""
        SELECT distinct S.user_name, D.datname, G.query_id, S.unique_sql_id, S.n_calls,
        S.min_elapse_time, S.max_elapse_time, S.n_returned_rows / (S.n_calls + 1) as
        avg_returned_rows, S.n_tuples_fetched / (S.n_calls + 1) as avg_tuples_fetched,
        S.n_tuples_returned / (S.n_calls + 1) as avg_tuples_returned,
        S.n_tuples_inserted / (S.n_calls + 1) as avg_tuples_inserted,
        S.n_tuples_updated / (S.n_calls + 1) as avg_tuples_updated,
        S.n_tuples_deleted / (S.n_calls + 1) as avg_tuples_deleted,
        S.n_soft_parse, S.n_hard_parse, S.db_time / (S.n_calls + 1) as avg_db_time,
        S.cpu_time / (S.n_calls + 1) as avg_cpu_time, S.parse_time / (S.n_calls + 1) as
        avg_parse_time, S.plan_time / (S.n_calls + 1) as avg_plan_time,
        S.data_io_time / (S.n_calls + 1) as avg_data_io_time, S.sort_spill_count,
        S.hash_spill_count,
        query
        FROM dbe_perf.statement S INNER JOIN pg_catalog.gs_asp G ON G.unique_query_id = S.unique_sql_id
        INNER JOIN pg_catalog.pg_database D ON G.databaseid = D.oid where
        G.sample_time <= '{end_time}'"""
    if start_time is not None:
        stmt += f" and G.sample_time >= '{start_time}'"
    if databases is not None:
        databases = _add_quote(databases)
        stmt += f" and D.datname in {databases}"
    if sql_types is not None:
        sql_types = _add_quote(sql_types)
        stmt += f" and pg_catalog.upper(pg_catalog.split_part(trim(query), ' ', 1)) in {sql_types}"
    if db_users is not None:
        db_users = _add_quote(db_users)
        stmt += f" and S.user_name in {db_users}"
    stmt += f" limit {STATEMENT_LENGTH_LIMIT};"
    return stmt


def collect_statement_from_activity(databases, db_users, sql_types, duration=60):
    stmt = f"""
    SELECT usename, datname, application_name, sessionid, query_id, unique_sql_id, extract(epoch from pg_catalog.now() - query_start)
    as duration, 
    query FROM pg_catalog.pg_stat_activity
    WHERE state != 'idle' and application_name not in ('DBMind-openGauss-exporter', 'DBMind-Service')
    and query_id != 0 and duration >= {duration}
"""
    if databases is not None:
        databases = _add_quote(databases)
        stmt += f" and datname in {databases}"
    if db_users is not None:
        db_users = _add_quote(db_users)
        stmt += f" and usename in {db_users}"
    if sql_types is not None:
        sql_types = _add_quote(sql_types)
        stmt += f" and pg_catalog.upper(pg_catalog.split_part(trim(query), ' ', 1)) in {sql_types}"
    stmt += f" limit {STATEMENT_LENGTH_LIMIT};"
    return stmt


def collect_statement_from_statement_history(databases, schemas, start_time, end_time, db_users,
                                             sql_types, template_id, duration=60):
    stmt = f"""
    select user_name, db_name, schema_name, application_name, debug_query_id as query_id, unique_query_id,
    start_time, finish_time, extract(epoch from finish_time - start_time) as duration,
    n_returned_rows, n_tuples_fetched, n_tuples_returned, n_tuples_inserted, n_tuples_updated,
    n_tuples_deleted, n_blocks_fetched, n_blocks_hit, n_soft_parse, n_hard_parse, db_time,
    cpu_time, parse_time, plan_time, data_io_time, lock_wait_time, lwlock_wait_time,
    case when (client_addr is null) then '127.0.0.1' else client_addr end as client_addr,
    query,
    query_plan from dbe_perf.statement_history
    where application_name not in ('DBMind-openGauss-exporter', 'DBMind-Service') and duration >= {duration}
    """
    if start_time is not None:
        stmt += f" and start_time >= '{start_time}'"
    if end_time is not None:
        stmt += f" and finish_time <= '{end_time}'"
    if databases is not None:
        databases = _add_quote(databases)
        stmt += f" and db_name in {databases}"
    if schemas is not None:
        schemas = _to_array(schemas)
        stmt += f""" and ((pg_catalog.string_to_array(pg_catalog.replace(schema_name, ' ', ''), ',') && {schemas})
        or (pg_catalog.regexp_like(schema_name,'"\\$user"') and ARRAY[user_name::TEXT] && {schemas})) """
    if sql_types is not None:
        sql_types = _add_quote(sql_types)
        stmt += f" and pg_catalog.upper(pg_catalog.split_part(trim(query), ' ', 1)) in {sql_types}"
    if db_users is not None:
        db_users = _add_quote(db_users)
        stmt += f" and user_name in {db_users}"
    if template_id is not None:
        stmt += f" and unique_query_id = '{template_id}'"
    stmt += f" limit {STATEMENT_LENGTH_LIMIT};"
    return stmt


def check_time_parameter(args):
    if args.start_time:
        args.start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(args.start_time // 1000))
    if args.end_time:
        args.end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(args.end_time // 1000))
    else:
        # if end_time is not provied, then we use current timestamp instead
        args.end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))


def fetch_statements(args):
    stmts = ''
    if args.statement_source == 'asp':
        stmts = collect_statement_from_asp(args.statement_database, args.start_time, args.end_time, args.statement_user,
                                           args.statement_type)
    elif args.statement_source == 'pg_stat_activity':
        stmts = collect_statement_from_activity(args.statement_database, args.statement_user, args.statement_type,
                                                args.duration)
    elif args.statement_source == 'dbe_perf.statement_history':
        stmts = collect_statement_from_statement_history(args.statement_database, args.statement_schema,
                                                         args.start_time, args.end_time, args.statement_user,
                                                         args.statement_type, args.duration)
    if stmts:
        if args.data_source == 'driver':
            rows = args.driver.query(stmts, return_tuples=True)
        elif args.data_source == 'tsdb':
            rows = global_vars.agent_proxy.call('query_in_database',
                                                stmts,
                                                'postgres',
                                                return_tuples=True)
        return rows
    return []


def main(argv):
    arg_parser = argparse.ArgumentParser(description="Fetch statements online from the database.")
    arg_parser.add_argument("--data-source", choices=('tsdb', 'driver'), default='tsdb',
                            help='set database dsn when tsdb is not available. Using in diagnosis.')
    arg_parser.add_argument("-c", "--conf", metavar='DIRECTORY', required=True, type=path_type,
                            help='Set the directory of configuration files. Using when data-source is tsdb')
    arg_parser.add_argument("-o", "--output", type=os.path.realpath, help="The file containing the fetched statements")
    arg_parser.add_argument("--url", help="The URL of database. Using when data-source is driver")
    arg_parser.add_argument('--statement-source', help='The source of statements you want to fetch',
                            choices=['asp', 'dbe_perf.statement_history', 'pg_stat_activity'], default='asp')
    arg_parser.add_argument("--statement-database", help="Name of database", action=CheckWordValid)
    arg_parser.add_argument("--statement-schema",
                            help="Schema name for the current business data", action=CheckWordValid)
    arg_parser.add_argument('--statement-type',
                            help='The type of statementss you want to fetch', action=CheckWordValid)
    arg_parser.add_argument('--statement-user',
                            help='The user of statements you want to fetch', action=CheckWordValid)
    arg_parser.add_argument('--start-time', type=date_type,
                            help="Start time of fetching statements, "
                                 "support timestamp(ms) and string, example: YYYY-MM-DD hh:mm:ss")
    arg_parser.add_argument('--end-time', type=date_type,
                            help="End time of fetching statements, "
                                 "support timestamp(ms) and string, example: YYYY-MM-DD hh:mm:ss")
    arg_parser.add_argument('--duration', type=int, help='The duration of statements you want to fetch, unit is ms')
    arg_parser.add_argument('--verify', help='Whether to validate statements',
                            action='store_true')
    args = arg_parser.parse_args(argv)
    args.driver = None
    check_time_parameter(args)
    if not os.path.exists(args.conf):
        arg_parser.exit(1, 'Not found the directory %s.\n' % args.conf)
    os.chdir(args.conf)
    set_logger(os.path.join('logs', constants.FETCH_STATEMENT_LOG_NAME), "info")
    init_global_configs(args.conf)
    if args.data_source == 'driver':
        if args.url is None:
            arg_parser.exit(1, "Quiting due to lack of URL.\n")
        try:
            parsed_dsn = parse_dsn(args.url)
            if 'password' in parsed_dsn:
                arg_parser.exit(1, "sensitive information\n")
            password = getpass('Please input the password for URL:')
            parsed_dsn['password'] = password
            args.url = make_dsn(**parsed_dsn)
        except Exception:
            arg_parser.exit(1, "Quiting due to wrong URL format.\n")
        args.driver, message = try_to_get_driver(args.url)
        if not args.driver:
            arg_parser.exit(1, message)
    elif args.data_source == 'tsdb':
        if not os.path.exists(args.conf):
            arg_parser.exit(1, 'Not found the directory %s.\n' % args.conf)
        success, message = try_to_initialize_rpc_and_tsdb()
        if not success:
            arg_parser.exit(1, message)
    statements = fetch_statements(args)
    # Save the fetched query in a file.
    with open(args.output, 'w+') as fp:
        for statement in statements:
            fp.write(','.join(str(item) for item in statement) + '\n')


if __name__ == '__main__':
    main(sys.argv[1:])

