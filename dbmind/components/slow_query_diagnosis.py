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
import datetime
import logging
import os
import sys
import time
import traceback
from getpass import getpass

from prettytable import PrettyTable
from psycopg2.extensions import parse_dsn

from dbmind import constants
from dbmind import global_vars
from dbmind.app.diagnosis.query.slow_sql import SlowQuery
from dbmind.app.diagnosis.query.slow_sql.analyzer import SlowSQLAnalyzer
from dbmind.app.diagnosis.query.slow_sql.query_info_source import QueryContextFromDriver
from dbmind.app.diagnosis.query.slow_sql.query_info_source import QueryContextFromTSDBAndRPC
from dbmind.cmd.edbmind import init_global_configs
from dbmind.common.opengauss_driver import Driver
from dbmind.common.parser.plan_parsing import Plan
from dbmind.common.parser.sql_parsing import exist_track_parameter
from dbmind.common.utils.checking import path_type, date_type
from dbmind.common.utils.cli import (keep_inputting_until_correct,
                                     write_to_terminal)
from dbmind.common.utils.component import initialize_rpc_service, initialize_tsdb_param
from dbmind.common.utils.exporter import set_logger
from dbmind.metadatabase.dao import slow_queries


def get_query_context_with_rpc(query, db_name, **params):
    if query is None:
        return [], []

    host, port = global_vars.agent_proxy.current_agent_addr().split(':')

    default_interval = 120 * 1000
    start_time, finish_time = params.pop('start_time', None), params.pop('finish_time', None)
    if finish_time is None:
        finish_time = int(datetime.datetime.now().timestamp()) * 1000
    if start_time is None:
        start_time = finish_time - default_interval
    schema_name = params.pop('schema_name', None)
    schema_name = 'public'
    track_parameter = exist_track_parameter(query)
    slow_sql_instance = SlowQuery(db_host=host, db_port=port, query=query, db_name=db_name,
                                  track_parameter=track_parameter, start_time=start_time,
                                  finish_time=finish_time, schema_name=schema_name, **params)

    return QueryContextFromTSDBAndRPC(slow_sql_instance)


def analyze_slow_query_with_rpc(query, db_name, **params):
    query_context = get_query_context_with_rpc(
        query, db_name, **params
    )
    query_plan = query_context.slow_sql_instance.query_plan
    return query_plan, _analyze_slow_query_internal(query_context)


def get_query_context_with_driver(query, driver, **params):
    host, port, db_name = driver.host, driver.port, driver.dbname

    default_interval = 120 * 1000
    start_time, finish_time = params.pop('start_time'), params.pop('finish_time')
    if finish_time is None:
        finish_time = int(datetime.datetime.now().timestamp()) * 1000
    if start_time is None:
        start_time = finish_time - default_interval

    track_parameter = exist_track_parameter(query)
    slow_sql_instance = SlowQuery(db_host=host, db_port=port, query=query, db_name=db_name,
                                  track_parameter=track_parameter, start_time=start_time,
                                  finish_time=finish_time, **params)
    return QueryContextFromDriver(slow_sql_instance, driver=driver)


def analyze_slow_query_with_driver(query, driver, **params):
    query_context = get_query_context_with_driver(query, driver, **params)
    query_plan = query_context.slow_sql_instance.query_plan
    return query_plan, _analyze_slow_query_internal(query_context)


def _analyze_slow_query_internal(query_context):
    root_causes, suggestions = [], []
    try:
        query_analyzer = SlowSQLAnalyzer()
        slow_sql_instance = query_analyzer.run(query_context)
        root_causes.append(slow_sql_instance.root_causes.split('\n'))
        suggestions.append(slow_sql_instance.suggestions.split('\n'))
    except Exception as e:
        logging.exception(e)
        return [], []
    return root_causes, suggestions


def get_query_plan(
        query, db_name, schema_name=None, data_source='tsdb', driver=None
):
    if data_source == 'tsdb':
        query_context = get_query_context_with_rpc(
            query, db_name, schema_name=schema_name
        )
    elif data_source == 'driver':
        query_context = get_query_context_with_driver(
            query=query, schema_name=schema_name, driver=driver
        )
    else:
        raise AssertionError()
    plan_tree = None
    if query_context.slow_sql_instance.query_plan is not None:
        plan_parser = Plan()
        plan_parser.parse(query_context.slow_sql_instance.query_plan)
        plan_tree = plan_parser.plan_tree()
    return query_context.slow_sql_instance.query_plan, query_context.query_type, query_context.slow_sql_instance.schema_name, plan_tree


def _is_database_exist(db_name, data_source='tsdb', driver=None):
    stmt = "select datname from pg_database where datname = '%s'" % db_name
    if data_source == 'tsdb':
        rows = global_vars.agent_proxy.call('query_in_database',
                                            stmt,
                                            db_name,
                                            return_tuples=True)
    else:
        rows = driver.query(stmt, return_tuples=True)
    return bool(rows)


def _is_schema_exist(schema, db_name=None, data_source='tsdb', driver=None):
    stmt = "select nspname from pg_namespace where nspname = '%s'" % schema
    if data_source == 'tsdb':
        rows = global_vars.agent_proxy.call('query_in_database',
                                            stmt,
                                            db_name,
                                            return_tuples=True)
    else:
        rows = driver.query(stmt, return_tuples=True)
    return bool(rows)


def try_to_initialize_rpc_and_tsdb(db_name, schema):
    if not initialize_tsdb_param():
        return False, 'TSDB service does not exist, exiting...'
    if not initialize_rpc_service():
        return False, 'RPC service does not exist, exiting...'
    if db_name is None:
        return False, "Lack the information of 'database', exiting..."
    if not _is_database_exist(db_name, data_source='tsdb'):
        return False, "Database '%s' does not exist, exiting..." % db_name
    if schema is not None and not _is_schema_exist(schema, db_name=db_name, data_source='tsdb'):
        return False, "Schema '%s' does not exist, exiting..." % schema
    return True, None


def try_to_get_driver(url, schema):
    driver = Driver()
    try:
        driver.initialize(url)
    except ConnectionError:
        return None, 'Error occurred when initialized the URL, exiting...'
    if schema is not None and not _is_schema_exist(schema, data_source='driver', driver=driver):
        return None, "Schema '%s' does not exist, exiting..." % schema
    return driver, None


def show(instance, query, start_time, finish_time):
    field_names = (
        'slow_query_id', 'schema_name', 'db_name',
        'query', 'start_at', 'duration_time',
        'root_cause', 'suggestion'
    )
    output_table = PrettyTable()
    output_table.field_names = field_names

    result = slow_queries.select_slow_queries(instance, field_names, query, start_time, finish_time)
    nb_rows = 0
    for slow_query in result:
        row = [getattr(slow_query, field) for field in field_names]
        output_table.add_row(row)
        nb_rows += 1

    if nb_rows > 50:
        write_to_terminal('The number of rows is greater than 50. '
                          'It seems too long to see.')
        char = keep_inputting_until_correct('Do you want to dump to a file? [Y]es, [N]o.', ('Y', 'N'))
        if char == 'Y':
            dump_file_name = 'slow_queries_%s.txt' % int(time.time())
            with open(dump_file_name, 'w+') as fp:
                fp.write(str(output_table))
            write_to_terminal('Dumped file is %s.' % os.path.realpath(dump_file_name))
        elif char == 'N':
            print(output_table)
            print('(%d rows)' % nb_rows)
    else:
        print(output_table)
        print('(%d rows)' % nb_rows)


def clean(retention_days):
    if retention_days is None:
        slow_queries.truncate_slow_queries()
        slow_queries.truncate_killed_slow_queries()
    else:
        start_time = int((time.time() - float(retention_days) * 24 * 60 * 60) * 1000)
        slow_queries.delete_slow_queries(start_time)
        slow_queries.delete_killed_slow_queries(start_time)
    write_to_terminal('Success to delete redundant results.')


def diagnosis(
        query, db_name, schema=None, start_time=None, finish_time=None,
        driver=None, data_source='tsdb'
):
    field_names = ('root_cause', 'suggestion')
    output_table = PrettyTable()
    output_table.field_names = field_names
    output_table.align = "l"
    if data_source == 'tsdb':
        _, (root_causes, suggestions) = analyze_slow_query_with_rpc(
            query,
            db_name,
            schema=schema,
            start_time=start_time,
            finish_time=finish_time
        )
    elif data_source == 'driver':
        _, (root_causes, suggestions) = analyze_slow_query_with_driver(
            query,
            driver,
            schema=schema,
            start_time=start_time,
            finish_time=finish_time
        )
    else:
        raise AssertionError()

    for root_cause, suggestion in zip(root_causes[0], suggestions[0]):
        output_table.add_row([root_cause, suggestion])
    print(output_table)


def get_plan(query, db_name, schema=None, driver=None, data_source='tsdb'):
    output_table = PrettyTable()
    field_names = ('normalized', 'plan')
    output_table.field_names = field_names
    output_table.align = "l"
    query_plan, query_type, _, _ = get_query_plan(query=query,
                                            db_name=db_name,
                                            schema_name=schema,
                                            data_source=data_source,
                                            driver=driver)
    if query_type == 'normalized':
        output_table.add_row([True, query_plan])
    else:
        output_table.add_row([False, query_plan])
    print(output_table)


def main(argv):
    parser = argparse.ArgumentParser(description='Slow Query Diagnosis: Analyse the root cause of slow query')
    parser.add_argument('action', choices=('show', 'clean', 'diagnosis'),
                        help='choose a functionality to perform')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=path_type,
                        help='Set the directory of configuration files')
    parser.add_argument('--instance', metavar='INSTANCE',
                        help='Set the instance of slow query. Using in show.')
    parser.add_argument('--database', metavar='DATABASE',
                        help='Set the name of database')
    parser.add_argument('--schema', metavar='SCHEMA',
                        help='Set the schema of database')
    parser.add_argument('--query', metavar='SLOW_QUERY',
                        help='Set a slow query you want to retrieve')
    parser.add_argument('--start-time', metavar='TIMESTAMP_IN_MICROSECONDS', type=date_type,
                        help='Set the start time of a slow SQL diagnosis result to be retrieved')
    parser.add_argument('--end-time', metavar='TIMESTAMP_IN_MICROSECONDS', type=date_type,
                        help='Set the end time of a slow SQL diagnosis result to be retrieved')
    parser.add_argument('--retention-days', metavar='DAYS', type=float,
                        help='clear historical diagnosis results and set '
                             'the maximum number of days to retain data')

    args = parser.parse_args(argv)
    # add dummy fields
    args.driver = None
    args.data_source = 'tsdb'

    if not os.path.exists(args.conf):
        parser.exit(1, 'Not found the directory %s.\n' % args.conf)

    # Set the global_vars so that DAO can login the meta-database.
    os.chdir(args.conf)
    init_global_configs(args.conf)
    set_logger(os.path.join('logs', constants.SLOW_SQL_RCA_LOG_NAME), "info")

    if args.action == 'show':
        if None in (args.query, args.start_time, args.end_time):
            write_to_terminal('There may be a lot of results because you did not use all filter conditions.',
                              color='red')
            inputted_char = keep_inputting_until_correct('Press [A] to agree, press [Q] to quit:', ('A', 'Q'))
            if inputted_char == 'Q':
                parser.exit(0, "Quitting due to user's instruction.\n")
    elif args.action == 'clean':
        if args.retention_days is None:
            write_to_terminal('You did not specify retention days, so we will delete all historical results.',
                              color='red')
            inputted_char = keep_inputting_until_correct('Press [A] to agree, press [Q] to quit:', ('A', 'Q'))
            if inputted_char == 'Q':
                parser.exit(0, "Quitting due to user's instruction.\n")
    elif args.action in ('diagnosis', 'get_plan'):
        if args.query is None or not len(args.query.strip()):
            write_to_terminal('You did not specify query, so we cannot diagnosis root cause.')
            parser.exit(1, "Quiting due to lack of query.\n")
        if args.data_source == 'driver':
            if args.url is None:
                parser.exit(1, "Quiting due to lack of URL.\n")
            try:
                parsed_dsn = parse_dsn(args.url)
                if 'password' in parsed_dsn:
                    parser.exit(1, "Quiting due to security considerations.\n")
                password = getpass('Please input the password for URL:')
                parsed_dsn['password'] = password
                args.url = ' '.join(['{}={}'.format(k, v) for (k, v) in parsed_dsn.items()])
            except Exception:
                parser.exit(1, "Quiting due to wrong URL format.\n")
            args.driver, message = try_to_get_driver(args.url, args.schema)
            if not args.driver:
                parser.exit(1, message)
        elif args.data_source == 'tsdb':
            success, message = try_to_initialize_rpc_and_tsdb(args.database, args.schema)
            if not success:
                parser.exit(1, message)

        if args.schema is None:
            write_to_terminal("Lack the information of 'schema', use default value: 'public'.", color='yellow')
            args.schema = 'public'

    try:
        if args.action == 'show':
            show(args.instance, args.query, args.start_time, args.end_time)
        elif args.action == 'clean':
            clean(args.retention_days)
        elif args.action == 'diagnosis':
            diagnosis(args.query, args.database, schema=args.schema,
                      start_time=args.start_time, finish_time=args.end_time,
                      driver=args.driver, data_source=args.data_source)
        elif args.action == 'get_plan':
            get_plan(args.query, args.database, schema=args.schema,
                     driver=args.driver, data_source=args.data_source)
    except Exception as e:
        write_to_terminal('An error occurred probably due to database operations, '
                          'please check database configurations. For details:\n' +
                          str(e), color='red', level='error')
        traceback.print_tb(e.__traceback__)
        return 2
    return args


if __name__ == '__main__':
    main(sys.argv[1:])

