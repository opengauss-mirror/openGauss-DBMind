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
import os
import sys
import re
import time
import logging
from typing import List
import argparse

from dbmind.common.utils.checking import CheckWordValid
from dbmind.components.index_advisor.sql_generator import get_prepare_sqls
from dbmind.components.index_advisor.executors import gsql_executor
from dbmind.components.index_advisor.index_advisor_workload import get_password
from dbmind.common.parser.sql_parsing import replace_comma_with_dollar


def get_fetch_queries(statement_type, database, schema, **kwargs) -> List[str]:
    fetch_asp_query = "SELECT regexp_replace((CASE WHEN query like '%;' THEN query ELSE query || ';' END), " \
                      "E'[\\n\\r]+', ' ', 'g') as q FROM dbe_perf.statement S INNER JOIN " \
                      "gs_asp G ON G.unique_query_id = S.unique_sql_id INNER JOIN pg_database D ON " \
                      "G.databaseid = D.oid WHERE D.datname = '{database}' AND " \
                      "G.sample_time > '{start_time}' and G.sample_time < '{end_time}';"
    fetch_history_query = "select regexp_replace((CASE WHEN t1.query like '%;' THEN t1.query ELSE " \
                          "t1.query || ';' END), " \
                          "E'[\\n\\r]+', ' ', 'g') as q from dbe_perf.statement " \
                          "t1 left join dbe_perf.statement_history t2 ON " \
                          "t1.unique_sql_id = t2.unique_query_id where db_name='{database}' and schema_name='{schema}';"
    fetch_activity_query = "SELECT regexp_replace((CASE WHEN query like '%;' THEN query ELSE query || ';' END), " \
                           "E'[\\n\\r]+', ' ', 'g') as q FROM pg_stat_activity WHERE state != 'idle' and " \
                           "datname='{database}';"
    fetch_activity_slow_query = f"{fetch_activity_query.strip(';')} and now()-query_start > interval '5 s';"

    if statement_type == 'asp':
        return [fetch_asp_query.format(database=database, start_time=kwargs.get('start_time'),
                                       end_time=kwargs.get('end_time'))]
    elif statement_type == 'history':
        return [fetch_history_query.format(database=database, schema=schema)]
    elif statement_type == 'activity':
        return [fetch_activity_query.format(database=database)]
    elif statement_type == 'slow':
        return [fetch_activity_slow_query.format(database=database)]


def is_valid_statement(conn, statement):
    """Determine if the query is correct by whether the executor throws an exception."""
    queries = get_prepare_sqls(statement)
    res = conn.execute_sqls(queries)
    for _tuple in res:
        if 'ERROR' in _tuple[0].upper():
            return False
    return True


def add_semicolon(query):
    """Add a semicolon to ensure that the executor can recognize the query correctly."""
    if not query.strip().endswith(';'):
        return f'{query.strip()};'
    return query


def check_parameter(args):
    if args.statement_type == 'asp':
        if (not args.start_time) or (not args.end_time):
            raise ValueError('Please set the start_time and the end_time if you specify asp')

    if args.start_time:
        # compatible with '2022-1-4 1:2:3'
        args.start_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                        time.strptime(args.start_time,
                                                      '%Y-%m-%d %H:%M:%S')
                                        )
    if args.end_time:
        args.end_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                      time.strptime(args.end_time,
                                                    '%Y-%m-%d %H:%M:%S')
                                      )


def fetch_statements(conn, statement_type, database, schema, **kwargs):
    fetch_queries = get_fetch_queries(statement_type, database, schema, **kwargs)
    statements = []
    for _tuple in conn.execute_sqls(fetch_queries):
        # filtering non-statement results
        statement = _tuple[0]
        if statement.startswith(('SET;', 'q;', ';', 'total time')) or statement.endswith(' rows);') or \
                re.match('SELECT \d+;', statement):
            continue
        statement = add_semicolon(statement)
        statement = replace_comma_with_dollar(statement)
        statements.append(statement)
    return statements


def main(argv):
    arg_parser = argparse.ArgumentParser(description="Fetch statements online from the database.")
    arg_parser.add_argument("db_port", help="Port of database", type=int)
    arg_parser.add_argument("database", help="Name of database", action=CheckWordValid)
    arg_parser.add_argument(
        "--db-host", "--h", help="Host for database", action=CheckWordValid)
    arg_parser.add_argument(
        "-U", "--db-user", help="Username for database log-in", action=CheckWordValid)
    arg_parser.add_argument(
        "output", type=os.path.realpath, help="The file containing the fetched statements",
        action=CheckWordValid)
    arg_parser.add_argument("--schema", help="Schema name for the current business data",
                            default='public', action=CheckWordValid)
    arg_parser.add_argument('--statement-type', help='The type of statements you want to fetch',
                            choices=['asp', 'slow', 'history', 'activity'], default='asp')
    arg_parser.add_argument('--start-time', help='Start time of statements, format: 2022-10-01 00:00:00')
    arg_parser.add_argument('--end-time', help='End time of statements, format: 2022-10-01 00:10:00')
    arg_parser.add_argument('--verify', help='Whether to validate statements',
                            action='store_true')
    arg_parser.add_argument('--driver', help='Whether to use python driver, default use gsql',
                            action='store_true')
    args = arg_parser.parse_args(argv)

    args.W = get_password()
    check_parameter(args)

    executor = gsql_executor.GsqlExecutor
    if args.driver:
        try:
            from dbmind.components.index_advisor.executors import driver_executor
            executor = driver_executor.DriverExecutor
        except ImportError:
            logging.warning('Python driver import failed, '
                            'the gsql mode will be selected to connect to the database.')

    # Get queries via gsql or driver. 
    conn = executor('postgres', args.db_user, args.W, args.db_host, args.db_port, args.schema)
    statements = fetch_statements(conn, **vars(args))

    # Save the fetched query in a file.
    with open(args.output, 'w') as fp:
        for statement in statements:
            if not args.verify or (args.verify and is_valid_statement(conn, statement)):
                fp.write(statement + '\n')


if __name__ == '__main__':
    main(sys.argv[1:])

