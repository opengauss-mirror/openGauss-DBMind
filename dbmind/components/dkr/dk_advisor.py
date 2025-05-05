#!/usr/bin/env python3
# coding=utf-8
# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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

"""This file is used to implement the core function of distribution key recommendation."""

import argparse
import copy
import getpass
import json
import select
import sys
import time

import sqlparse

from .parsing import Plan
from .sqlparse_processing.extract_rules import ExtractPerSQL, WorkloadInfo
from .dao.execute import ExecuteFactory
from .dao import get_wdr_info, obtain_system_tables, get_num_cn
from .dk_advisor_alg import GraphAlg, DFSAlg, RepTblAlg
from .utils import *

# global variables
DISTINCT_THOLD_VALUE = 0.5
# database params
DATA_NODE_NUM = 0
SCHEMA = None


def uniform_dist_alg(table, group_order_list, where_dkr, workload_length):
    """
    The uniform distribution algorithm is used to
     distribute the data involved in the table as well-distributed as possible.

     Principle
     ~~~~~~~~~~~~~~
     For table names without recommended distribution key,
     continue to recommend distribution key for the table according to the
     priority value of GROUP BY/ORDER BY and distinct value in descending order.
    """
    if not table.columns:
        logging.warning("Cannot recommend a distribute key for table '%s'. "
                        "Please confirm whether the statistic information of the table exist.",
                        table.table_name)
        return False

    # group/order
    matched_list = []
    for table_column in group_order_list:
        table_name, column_name = table_column[0].split(BLANK)
        if table.table_name != table_name:
            continue
        matched_list.append(table_column)
    if matched_list:
        high_freq_table, high_freq_column = matched_list[0][0], \
                                            matched_list[0][1]
        max_cardinality_column = high_freq_table.split(BLANK)[1]
        more_than_threshold = \
            table.cardinality[table.columns.index(max_cardinality_column)] > DISTINCT_THOLD_VALUE \
            and high_freq_column > workload_length * GROUP_ORDER_PERCENTAGE
        if more_than_threshold:
            return max_cardinality_column

    # where filter condition
    if table in where_dkr.keys():
        column_name = where_dkr[table]
        if table.cardinality[table.columns.index(
                column_name)] > DISTINCT_THOLD_VALUE:
            return column_name

    columns = table.columns[:]
    cardinalitys = table.cardinality[:]
    # primary key filter
    if table.primary_key:
        if not (table.primary_key in table.column_default and table.column_default[table.primary_key].startswith(
                'nextval')):
            return table.primary_key.lower()
    # auto-increment column filter
    for column, value in table.column_default.items():
        if value.startswith('nextval'):
            cardinalitys.pop(columns.index(column))
            columns.pop(columns.index(column))
    # Get the max one of distinct values.
    pairs = sorted(zip(columns, cardinalitys), key=lambda x: x[0])
    column, cardinality = max(pairs, key=lambda x: x[1])
    return column


# The following functions are served to generate_distribution_key().
def remove_empty_list(multi_cond):
    """
    The data structure of parameter 'table' is a hash table whose key
     is string-type and whose value is nested list.
    Hereby, to remove out of empty list from the raw list,
     we should do as the following form that filters the raw list.
    """
    for k, v in multi_cond[0].items():
        multi_cond[0][k] = [item for item in v if len(item)]
    equal_cond = multi_cond[1] if multi_cond[1] and multi_cond[1][0] else None
    return multi_cond[0], equal_cond


def count_freq_grp_order_cond(grp_order_cond_list):
    """
    Calculate the weight of the relationship of GROUP BY or ORDER BY.
    :param grp_order_cond_list: a nested list whose type of element is
      [['table name', 'column name'], times], such as
      ```
         [[[['a_table', 'a_column'], 1], [['b_table', 'b_column'], 3]]]
      ```
    """
    time_cond = {}
    for query_cond in grp_order_cond_list:
        for cond in query_cond:
            if BLANK.join(cond[:-1][0]) not in time_cond.keys():
                time_cond[BLANK.join(cond[:-1][0])] = cond[-1]
            else:
                time_cond[BLANK.join(cond[:-1][0])] += cond[-1]
    # Distribution column recommendation algorithm will filter in order.
    time_cond = sorted(time_cond.items(), key=lambda x: x[1], reverse=True)
    return time_cond


def count_freq_join_cond(join_cond_list):
    """
    calculate the weight of the relationship of JOIN or WHERE
    :param join_cond_list: [[[['a_table', 'a_column'],['b_table', 'b_column'], 1],
     [['c_table', 'c_column'],['d_table', 'd_column'] 3]]]
    """
    time_cond = []
    for query_cond in join_cond_list:
        for cond in query_cond:
            for cond_item in cond:
                if len(cond_item) == 3:
                    time_cond.append(
                        [cond_item[0], cond_item[1], cond_item[2]])
    time_cond = sorted(time_cond, key=lambda x: (x[0][0], x[1][0]))
    if len(time_cond) <= 1:
        return time_cond

    time_tmp = []
    count = 1
    # Count the number of occurrences of table names and column names
    # with the same join relationship
    for i, item in enumerate(time_cond[:-1]):
        if count == 1:
            time_tmp.append(item)
        if item[0] == time_cond[i + 1][0] and item[1] == time_cond[i + 1][1]:
            count = time_tmp[-1][2] + time_cond[i + 1][2]
            time_tmp[-1][2] = count
        elif item[0] == time_cond[i + 1][1] and item[1] == time_cond[i + 1][0]:
            count = time_tmp[-1][2] + time_cond[i + 1][2]
            time_tmp[-1][2] = count
        else:
            count = 1
    if time_cond[-1][0] != time_cond[-2][0] or time_cond[-1][1] != time_cond[-2][1]:
        time_tmp.append(time_cond[-1])
    return time_tmp


def get_find_filter_callback(workload_info, where_dkr):
    def callback(node):
        if node.name is None or 'Scan' not in node.name:
            return

        for attr, info in node.properties.items():
            if 'Filter' not in attr:
                continue

            for tbl_name in workload_info.wl_tables.keys():
                if tbl_name not in node.name:
                    continue
                for col_name in workload_info.wl_tables[tbl_name].columns:
                    if col_name not in info:
                        continue
                    tuples_count = workload_info.wl_tables[tbl_name].tuple_count
                    if node.rows < tuples_count * FILTER_COND_THOLD:
                        where_dkr[tbl_name] = col_name
                        break

    return callback


def get_find_join_callback(workload_info, sql_freq, total_join_cond_list):
    def is_same_join_cond(cond1, cond2):
        pair1, pair2, _ = cond1
        pair3, pair4 = cond2
        equation1 = pair1 == pair3 and pair2 == pair4
        equation2 = pair1 == pair4 and pair2 == pair3
        if equation1 or equation2:
            return True

        return False

    def callback(node):
        if node.type != 'Join':
            return

        for attr, info in node.properties.items():
            if 'Cond' not in attr:
                continue

            if 'AND' in info:
                continue
            tables = []
            columns = []
            for tbl_name in workload_info.wl_tables.keys():
                if '%s.%s.' % (SCHEMA, tbl_name) not in info:
                    continue

                if len(tables) == 2:
                    break

                tables.append(tbl_name)

                for col_name in workload_info.wl_tables[tbl_name].columns:
                    if '%s.%s' % (tbl_name, col_name) in info:
                        columns.append(col_name)
                        break

            if len(tables) < 2 or len(columns) < 2:
                continue

            found = False
            new_join_cond = list(zip(tables, columns))
            for i, join_cond in enumerate(total_join_cond_list):
                if is_same_join_cond(join_cond, new_join_cond):
                    found = True
                    total_join_cond_list[i][2] += sql_freq * node.total_cost
            if not found:
                new_join_cond.append(sql_freq * node.total_cost)
                total_join_cond_list.append(new_join_cond)

    return callback


def get_execution_plan(db_conn, sql):
    if sql:
        sql = sqlparse.split(sql)[0]
    sql_list = []
    if SCHEMA:
        sql_list.append('set current_schema = %s;' % SCHEMA)
    sql_list.append('set explain_perf_mode = normal;EXPLAIN (format text) ' + sql)
    raw_text = re.sub(r'\w*\s+QUERY PLAN\s+-+', '', db_conn.explain('EXPLAIN ' + sql))
    plan = Plan()
    plan.parse(raw_text)

    return plan


def generate_optimal_sequence(optimal_sequences, transaction_cond):
    """
    Aggregate all repeated relationships and containment relationships
    :param optimal_sequences: result sequence set to be aggregated
    :param transaction_cond: [[{t1:c1, t2:c1}, freq],...]
    """
    subset_conds = []
    count = 1
    # aggregate all repeated relationships and containment relationships
    for key, cur_cond in enumerate(transaction_cond[:-1]):
        if count == 1:
            # aggregate containment relationships
            for subset_cond in subset_conds:
                if set(subset_cond[0].items()).issubset(cur_cond[0].items()):
                    cur_cond[1] += subset_cond[1]
            cond_count = len(optimal_sequences)
            # determine whether each record in the optimal sequence is
            # a subset of the current record, if so, delete the record and add it to subsets,
            # finally update the current record frequency value.
            pos = 0  # Used to traverse the optimal sequence
            for ind in range(cond_count):
                if set(optimal_sequences[pos][0].items()).issubset(
                        cur_cond[0].items()):
                    cur_cond[1] += optimal_sequences[pos][1]
                    subset_conds.append(optimal_sequences[pos])
                    optimal_sequences.pop(pos)
                else:
                    pos += 1

            optimal_sequences.append(cur_cond)
        # is same condition
        if str(cur_cond[0]) == str(transaction_cond[key + 1][0]):
            count = optimal_sequences[-1][1] + transaction_cond[key + 1][1]
            optimal_sequences[-1][1] = count
        # is containment scene or irrelevant scene
        else:
            count = 1
    if str(transaction_cond[-1][0]) != str(transaction_cond[-2][0]):
        optimal_sequences.append(transaction_cond[-1])
    # sort sequences from high to low in the order of count and the number of tables.
    optimal_sequences.sort(key=lambda elem: (-elem[1], -len(elem[0])))


def common_dkr_compute(transaction_cond, dkr):
    if len(transaction_cond) == 1:
        return
    optimal_sequences = []
    # generate the optimal sequence of transaction table combinations according to frequency.
    generate_optimal_sequence(optimal_sequences, transaction_cond)
    # update distribution key.
    for table_columns in optimal_sequences:
        is_existing = False
        for cur_cond in table_columns[0]:
            if dkr.get(cur_cond):
                is_existing = True
                break
        if not is_existing:
            dkr.update(table_columns[0])


def transaction_dk_recommand(global_transaction_equal_cond,
                             global_transaction_table_columns, dkr):
    """
    Recommend distributed keys based on the extracted transaction information.
    :param dkr: distribution key recommendation results
    :param global_transaction_table_columns: [[{t1:c1, t2:c1}, freq],...]
    :param global_transaction_equal_cond: [[{t1:c1, t2:c2}, freq],...]
    :return: NA
    """
    if not global_transaction_equal_cond or not global_transaction_table_columns:
        return
    # process same value combination.
    # sort according to the number of tables and the table name in each transaction.
    global_transaction_equal_cond.sort(key=lambda elem: (len(elem[0]), str(elem[0]).strip('{}')))
    common_dkr_compute(global_transaction_equal_cond, dkr)

    # process frequent column combination
    # sort according to the number of tables and table name in each transaction.
    global_transaction_table_columns.sort(key=lambda elem: (len(elem[0]), str(elem[0]).strip('{}')))
    common_dkr_compute(global_transaction_table_columns, dkr)


def record_single_query_cost(sql_rules, workload_info):
    """Calculate the cost of replication tables in single-table query statement."""
    if len(sql_rules.sql_tables) != 1:
        return
    for table in sql_rules.sql_tables.keys():
        # record replication tables and its cost for workload.
        if table not in workload_info.wl_replication_tables:
            break
        workload_info.wl_replication_tables[table].replication_cost += \
            workload_info.wl_replication_tables[table].tuple_count * sql_rules.freq
        workload_info.wl_replication_tables[table].distribution_cost += \
            workload_info.wl_replication_tables[table].tuple_count \
            / DATA_NODE_NUM * sql_rules.freq


def generate_cost_rules(workload, workload_info, mode, db_conn, tbl_stat,
                        cost_type, prior_dist_trans):
    """Extract relationships from JOIN\\GROUP BY\\ORDER BY clause"""
    naive_cost_dict = dict(JOIN=[], GROUP_ORDER=[])
    join_cond_from_opt = []  # Extract join conditions from operators.
    where_dkr_from_opt = {}  # Recommend distribution key from where clauses.
    global_transaction_equal_cond = []  # record same value combination in all transactions.
    global_transaction_table_columns = []  # record frequent column combination in all transactions.
    for query in workload:
        if not query.statement:
            continue
        transaction_combination = []  # record all combination in a transaction.
        for stmt in query.statement.split(';'):
            try:
                if not any(tp in stmt.upper() for tp in DIST_SUPPORT_TYPE):
                    continue
                stmt = stmt.replace(' JOIN ONLY ', ' JOIN ').replace(' FROM ONLY ', ' FROM ')
                sql_rules = ExtractPerSQL(stmt, query.frequency, DATA_NODE_NUM, workload_info)
                local_cost_dict, equal_cond_tuple = remove_empty_list(
                    sql_rules.extract_cost_rules(mode, db_conn, tbl_stat))

                naive_cost_dict["JOIN"].append(local_cost_dict["JOIN"])
                naive_cost_dict["GROUP_ORDER"].append(
                    local_cost_dict["GROUP_ORDER"])
                if cost_type == 'optimizer':
                    # traverse the operators in the execution plan,
                    # and then analyze the relationship between them
                    # and prepare for the subsequent construction of the Join relationship graph.
                    plan = get_execution_plan(db_conn, query.statement)
                    plan.traverse(get_find_join_callback(workload_info,
                                                         query.frequency,
                                                         join_cond_from_opt))
                    plan.traverse(get_find_filter_callback(workload_info, where_dkr_from_opt))
                if equal_cond_tuple:
                    transaction_combination.append(equal_cond_tuple)
                # record cost for replication table in single table query scene.
                record_single_query_cost(sql_rules, workload_info)
            except ConnectionError as e:
                raise ConnectionError(e)
            except Exception as e:
                logging.warning(e)
        if len(transaction_combination) > 1 or \
                (prior_dist_trans and len(transaction_combination) > 0):
            # extract the same value combination and
            # frequent column combination for each transaction.
            transaction_combination = sorted(transaction_combination,
                                             key=lambda item: len(item[1]))
            dfs_alg = DFSAlg(transaction_combination, query.frequency)
            dfs_alg.process_transaction_equal_cond(global_transaction_equal_cond,
                                                   global_transaction_table_columns)
    return naive_cost_dict, [join_cond_from_opt, where_dkr_from_opt], \
           [global_transaction_equal_cond, global_transaction_table_columns]


def generate_distribution_key(workload, dml_workload, mode, db_conn, tbl_stat, cost_type,
                              repl_table_size, prior_dist_trans):
    logging.info('Extracting each query from workload. If there are many SQL statements, '
                 'the whole process may be relatively long.')
    workload_info = WorkloadInfo(DISTINCT_THOLD_VALUE)
    # extract all cost rules for workload
    naive_cost_dict, optimizer_cost_list, transaction_record_list = \
        generate_cost_rules(workload, workload_info, mode, db_conn, tbl_stat,
                            cost_type, prior_dist_trans)
    if db_conn:
        db_conn.close_conn()
    # firstly, recommend distribution keys based on the transaction.
    dkr = {}
    transaction_dk_recommand(transaction_record_list[0],
                             transaction_record_list[1], dkr)

    # secondly, recommend distribution keys based on the join relationship in the workload.
    logging.info("Performing naive maximum algorithm to estimate the cost.")
    join_cond_list = count_freq_join_cond(naive_cost_dict['JOIN'])
    group_order_list = count_freq_grp_order_cond(naive_cost_dict['GROUP_ORDER'])
    graph_alg = GraphAlg(workload_info, DATA_NODE_NUM)
    if cost_type == 'naive':
        graph_alg.naive_maximum_alg(dkr, join_cond_list, cost_type)
    else:
        graph_alg.naive_maximum_alg(dkr, optimizer_cost_list[0], cost_type)

    # thirdly, use the data uniform distribution algorithm
    # to ensure that the data is distributed as evenly as possible.
    for table in workload_info.wl_tables:
        if workload_info.wl_tables[table].table_name not in dkr.keys() and \
                workload_info.wl_tables[table].table_size != 0:
            dk = uniform_dist_alg(workload_info.wl_tables[table], group_order_list,
                                  optimizer_cost_list[1], len(workload))
            if dk:
                dkr[workload_info.wl_tables[table].table_name] = dk

    # finally, call the replication table selection algorithm.
    logging.info("Recommending replications.")
    rep_tbl_alg = RepTblAlg(dml_workload, workload_info, DATA_NODE_NUM)
    replications = rep_tbl_alg.replication_table_recommend(dkr, repl_table_size)

    return dkr, replications, workload_info


def workload_compression(sqls, prior_distribute_transaction):
    wd_dict = {}
    dml_wd_dict = {}
    workload = []
    dml_workload = []
    wd_index = 0
    dml_wd_index = 0
    for query in sqls:
        sql_template = common_format(query)
        if ';' not in query and not prior_distribute_transaction:
            for pattern in SQL_PATTERN:
                sql_template = re.sub(pattern, PLACEHOLDER, sql_template)
        if sql_template not in wd_dict.keys():
            wd_dict[sql_template] = wd_index
            wd_index += 1
            workload.append(QueryItem(query, 0))
        workload[wd_dict[sql_template]].frequency += 1
        if any(tp in sql_template.upper() for tp, v in REPL_SUPPORT_TYPE.items()):
            if sql_template not in dml_wd_dict:
                dml_wd_dict[sql_template] = dml_wd_index
                dml_wd_index += 1
                dml_workload.append(QueryItem(query, 0))
            dml_workload[dml_wd_dict[sql_template]].frequency += 1
    return workload, dml_workload


def load_workload_from_wdr(args):
    # need to access postgres database to get wdr.
    wdr_args = copy.copy(args)
    wdr_args.database = 'postgres'
    db_conn = None
    try:
        if args.driver:
            db_conn = ExecuteFactory.get_executor('driver', wdr_args)
        else:
            db_conn = ExecuteFactory.get_executor('gsql', wdr_args)
        tuples = get_wdr_info(wdr_args, db_conn, args.database)
    finally:
        if db_conn is not None:
            db_conn.close_conn()

    if len(tuples) == 0:
        logging.error("Not found SQL statements on current schema (%s) and database (%s).",
                      args.schema if args.schema else '$user,public', args.database)
        sys.exit(1)
    return workload_compression(get_wdr_sqls(tuples), args.prior_distribute_transaction)


def read_pipe():
    """
    Read stdin input such as "echo 'str1 str2' | python xx.py",
     return the input string.
    """
    input_str = ""
    r_handle, _, _ = select.select([sys.stdin], [], [], 0)
    if not r_handle:
        return ""

    for item in r_handle:
        if item == sys.stdin:
            input_str = sys.stdin.read().strip()
    return input_str


def get_password():
    password = read_pipe()
    if password:
        logging.warning("Read password from pipe.")
    else:
        password = getpass.getpass("Password for database user:")
    if not password:
        raise ValueError('Please input the password')
    return password


def initial_db_conn(args):
    # if no python driver, gsql mode is called by default.
    if args.driver:
        try:
            import psycopg2
            db_conn = ExecuteFactory.get_executor('driver', args)
        except ImportError:
            logging.warning('Python driver import failed, '
                            'the gsql mode will be selected to connect to the database.')
            db_conn = ExecuteFactory.get_executor('gsql', args)
            args.driver = None
    else:
        db_conn = ExecuteFactory.get_executor('gsql', args)
    return db_conn


def check_parameter(args):
    global DATA_NODE_NUM, DISTINCT_THOLD_VALUE, SCHEMA
    if args.mode == 'offline':
        if not (args.file and args.statistics):
            raise ValueError(
                "Enter the parameters --file and -s for offline mode.")
        if args.cost_type == 'optimizer':
            raise ValueError(
                "Offline mode does not support 'optimizer' cost type.")
        if args.driver:
            raise ValueError("Offline mode does not support driver")
    else:
        if not (args.port and args.database):
            raise ValueError('Please set database name and port.')
        args.password = get_password()
        is_legal = re.search(r'^[A-Za-z0-9~!@#$%^&*()-_=+\|\[{}\];:,<.>/?]+$', args.password)
        if not is_legal:
            raise ValueError("The password contains illegal characters.")
        if args.start_time:
            args.start_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                            time.strptime(args.start_time,
                                                          '%Y-%m-%d %H:%M:%S')
                                            )
        if args.end_time:
            args.end_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                          time.strptime(args.end_time,
                                                        '%Y-%m-%d %H:%M:%S')
                                          )
            if args.start_time:
                if time.strptime(args.end_time, '%Y-%m-%d %H:%M:%S') < time.strptime(args.start_time,
                                                                                     '%Y-%m-%d %H:%M:%S'):
                    raise ValueError(
                        "The end time '%s' must be after the start time '%s'." % (args.end_time, args.start_time)
                    )

    if args.dn is None:
        raise ValueError('Please set the number of data nodes.')
    if args.dn <= 0:
        raise ValueError('Please enter the correct number of data nodes.')
    if args.max_replication_table_size is not None and args.max_replication_table_size <= 0:
        raise ValueError('Please enter the correct max_replication_table_size and '
                         'the max_replication_table_size must be greater than 0.')
    DATA_NODE_NUM = args.dn
    SCHEMA = args.schema

    if args.min_distinct_threshold or args.min_distinct_threshold == 0:
        DISTINCT_THOLD_VALUE = args.min_distinct_threshold


class CheckValid(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        ill_character = [" ", "|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                         "{", "}", "(", ")", "[", "]", "~", "*", "?", "!", "\n"]
        if not values.strip():
            return
        if any(ill_char in values for ill_char in ill_character):
            raise Exception(
                "There are illegal characters in the %s." % self.dest)
        setattr(namespace, self.dest, values)


def main(argv):
    parser = argparse.ArgumentParser(
        description='Generate a set of distributed keys for workload. '
                    'two modes are supported:'
                    'offline: (--file -m -s) '
                    'online: (--file -p -d --host -U --start_time '
                    '--end_time --schema)',
        allow_abbrev=False)
    parser.add_argument(
        "-f",
        "--file",
        type=argparse.FileType('r'),
        help="File contains workload queries (One query per line) "
             "or oracle stored procedure")
    parser.add_argument("--standard",
                        action='store_true',
                        help="queries in the file as standard format")
    parser.add_argument("-s",
                        "--statistics",
                        type=argparse.FileType('r'),
                        help="File contains statistical information")
    parser.add_argument("-m",
                        "--mode",
                        required=True,
                        choices=["offline", "online"],
                        help="the current mode")
    parser.add_argument("--dn", type=int, help="The number of data nodes")
    parser.add_argument("-p", "--port", type=int, help="Port of database")
    parser.add_argument("-d", "--database", help="Name of database", action=CheckValid)
    parser.add_argument("--host", help="Host for database", action=CheckValid)
    parser.add_argument("--schema",
                        help="Schema name for the current business data",
                        action=CheckValid)
    parser.add_argument("-U", "--user",
                        default=getpass.getuser(),
                        help="Username for database log-in", action=CheckValid)
    parser.add_argument("--start_time", help="Collect WDR report start time")
    parser.add_argument("--end_time", help="Collect WDR report end time")
    parser.add_argument("--min_distinct_threshold",
                        type=float,
                        help="Minimum value of distinct value")
    parser.add_argument("--cost_type",
                        default='naive',
                        choices=["naive", "optimizer"],
                        help="Which cost estimation algorithm to use")
    parser.add_argument("--max_replication_table_size",
                        type=int,
                        help="Maximum number of rows in the replication table")
    parser.add_argument(
        "--prior_distribute_transaction",
        action='store_true',
        help="Prioritize the processing of distributed transaction")
    parser.add_argument("--driver",
                        action='store_true',
                        help="Whether to employ python-driver",
                        default=False)
    args = parser.parse_args(argv)
    check_parameter(args)
    db_conn = None
    tbl_stat = None
    if args.mode == 'offline':
        tbl_stat = json.load(args.statistics)
    else:
        # Initialize the connection
        db_conn = initial_db_conn(args)
        if get_num_cn(db_conn) is None:
            parser.exit(1, 'Can not get the deployment mode.\n')
        if get_num_cn(db_conn) == 0:
            parser.exit(1, 'The dkr module does not support centralized mode.\n')
        db_conn.system_tables = obtain_system_tables(db_conn)
    try:
        if args.file:
            workload, dml_workload = workload_compression(
                get_sqls(args.file, args.standard),
                args.prior_distribute_transaction)
        else:
            workload, dml_workload = load_workload_from_wdr(args)
        dkr, replications, workload_info = \
            generate_distribution_key(workload, dml_workload, args.mode, db_conn, tbl_stat,
                                      args.cost_type, args.max_replication_table_size,
                                      args.prior_distribute_transaction)
    finally:
        if db_conn:
            db_conn.close_conn()
    display_recommend_result(workload_info, dkr, replications)


def offline_interface(sql_file, stat_file, dn_num, replication_size=None,
                      pass_filepath=True, prior_distribute_transaction=False):
    global DATA_NODE_NUM, DISTINCT_THOLD_VALUE

    DATA_NODE_NUM = dn_num
    DISTINCT_THOLD_VALUE = 0.5

    if pass_filepath:
        with open(sql_file) as f, open(stat_file) as s:
            workload, dml_workload = workload_compression(
                get_sqls(f), prior_distribute_transaction)
            tbl_stat = json.load(s)
            dkr, replications, workload_info = generate_distribution_key(
                workload, dml_workload, 'offline', None, tbl_stat, 'naive',
                replication_size, prior_distribute_transaction)
    else:
        sqls = sql_file.split(';')
        workload, dml_workload = workload_compression(
            sqls, prior_distribute_transaction)
        tbl_stat = json.loads(stat_file)
        dkr, replications, workload_info = generate_distribution_key(
            workload, dml_workload, 'offline', None, tbl_stat, 'naive',
            replication_size, prior_distribute_transaction)

    for repl in replications:
        if repl in dkr:
            dkr.pop(repl)

    return dkr, replications


if __name__ == '__main__':
    main(sys.argv[1:])
