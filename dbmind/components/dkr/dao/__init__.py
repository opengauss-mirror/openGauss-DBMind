#!/usr/bin/env python3
# coding=utf-8
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

"""This file provide related functions to connect to the database."""
import ast

try:
    from ..utils import SYSTEM_TABLE_SEARCH, DEPLOY_MODE_SEARCH
except ValueError:
    from utils import SYSTEM_TABLE_SEARCH, DEPLOY_MODE_SEARCH
from dbmind.common.utils import escape_single_quote


class TableContext:
    def __init__(self, table_name):
        self.table_name = table_name
        self.table_size = 0
        self.tuple_count = 0
        self.columns = []
        self.cardinality = []
        self.primary_key = ''
        self.replication_cost = 0
        self.distribution_cost = 0
        self.most_common_vals_list = []
        self.most_common_freqs_list = []
        self.max_min_freq_ratio = {}
        self.column_default = {}

    def generate_dn_freqs(self, dn_num, column_index):
        # generate data frequency distribution in all data nodes
        dn_freqs = dict()
        for index in range(dn_num):
            dn_freqs[index] = 0
        for value, freq in zip(self.most_common_vals_list[column_index], self.most_common_freqs_list[column_index]):
            if value.isdigit():
                dn_freqs[hash(int(value)) % dn_num] += freq
            else:
                dn_freqs[hash(value) % dn_num] += freq
        return dn_freqs

    def filter_column_by_freqs(self, dn_num, column_index, max_min_ratio_threshold):
        # cannot obtain most_common_freqs_list for offline mode
        if not self.most_common_freqs_list:
            return False
        if column_index in self.max_min_freq_ratio:
            return self.max_min_freq_ratio[column_index]
        dn_freqs = self.generate_dn_freqs(dn_num, column_index)
        max_freq, min_freq = max(dn_freqs.values()), min(dn_freqs.values())
        if max_freq == 0:
            self.max_min_freq_ratio[column_index] = False
            return False
        if min_freq / max_freq >= max_min_ratio_threshold:
            self.max_min_freq_ratio[column_index] = True
            return True
        self.max_min_freq_ratio[column_index] = False
        return False


def obtain_system_tables(db_conn):
    result_list = db_conn.execute([SYSTEM_TABLE_SEARCH])
    return [res[0] for res in result_list]


def get_num_cn(db_conn):
    """

    Args:
        db_conn: The connection to db.

    Returns:

    """
    result = db_conn.execute([DEPLOY_MODE_SEARCH])
    return int(result[0][0]) if result and result[0] else None


def get_table_online(table_name, db_conn):
    table_context = TableContext(table_name)
    if table_name.split('.')[-1] in db_conn.system_tables:
        return table_context
    table_info = get_table_info_from_db(db_conn, table_name)
    if not table_info:
        return table_context
    [table_size_res, table_rows_res, table_distinct_res, primary_key_res, column_default] = table_info
    # parse result
    if table_size_res:
        table_context.table_size = int(table_size_res[0][0])
    if table_rows_res:
        table_context.tuple_count = int(table_rows_res[0][0])
    for n_distinct, attname, most_common_vals, most_common_freqs in table_distinct_res:
        n_distinct = float(n_distinct)
        if most_common_vals:
            table_context.most_common_vals_list.append([common_val.strip()
                                                        for common_val in
                                                        most_common_vals.strip().strip('{}').split(',')])
            if isinstance(most_common_freqs, list):
                table_context.most_common_freqs_list.append(most_common_freqs)
            else:
                table_context.most_common_freqs_list.append(
                    ast.literal_eval('{}{}{}'.format('[', most_common_freqs.strip().strip('{}'), ']')))
        else:
            table_context.most_common_vals_list.append([])
            table_context.most_common_freqs_list.append([])
        if n_distinct < 0:
            table_context.cardinality.append(-n_distinct)
        else:
            table_context.cardinality.append(n_distinct / table_context.tuple_count if
                                             table_context.tuple_count else 0)
        for column, value in column_default:
            if value:
                table_context.column_default[column] = value
        table_context.columns.append(attname)
    if primary_key_res:
        table_context.primary_key = primary_key_res[0][0]
    return table_context


def get_table_offline(table_name, tbl_stat):
    table_context = TableContext(table_name)
    # process offline mode
    if tbl_stat.get(table_name):
        table_context.table_size = tbl_stat[table_name]['size']
    if tbl_stat.get(table_name):
        table_context.tuple_count = tbl_stat[table_name]['rows']
        table_context.columns = list(tbl_stat[table_name]['column'].keys())
        distinct_value = list(tbl_stat[table_name]['column'].values())
        for item in distinct_value:
            table_context.cardinality.append(item / table_context.tuple_count if
                                             table_context.tuple_count else 0)
    if tbl_stat.get(table_name):
        table_context.primary_key = tbl_stat[table_name].get('primaryKey', '')
    return table_context


def get_table_info_from_db(db_conn, table_name):
    sql = []
    if db_conn.schema:
        sql = ["set current_schema = '%s';" % escape_single_quote(db_conn.schema)]
    # get table size
    sql.append("select * from pg_catalog.pg_relation_size('%s');" % escape_single_quote(table_name))
    sql_count = 1
    if db_conn.schema:
        # get table rows
        sql.append("select reltuples from pg_catalog.pg_class where lower(relname::text) like lower('%s') and "
                "relnamespace = (select oid from pg_catalog.pg_namespace where nspname = '%s');" %
                (escape_single_quote(table_name), escape_single_quote(db_conn.schema)))
        # get table column and cardinality
        sql.append("select n_distinct, attname, most_common_vals, most_common_freqs from pg_catalog.pg_stats "
                "where lower(tablename::text) like lower('%s') and "
                "schemaname = '%s';" % (escape_single_quote(table_name), escape_single_quote(db_conn.schema)))
        # get primary key
        sql.append("select c.attname from pg_catalog.pg_class a left join pg_catalog.pg_constraint b on  "
                "b.conrelid = a.oid left join pg_catalog.pg_attribute c on  c.attrelid = a.oid left "
                "join pg_catalog.pg_namespace d on d.oid = a.relnamespace where c.attnum > 0 and "
                "arraycontained(ARRAY[c.attnum], b.conkey) and lower(a.relname::text) like lower('%s') "
                "and d.nspname = '%s';" %
                (escape_single_quote(table_name), escape_single_quote(db_conn.schema)))
        # get column_default like nextval('bmsql_hist_id_seq'::regclass)
        sql.append(
            "select column_name, column_default from information_schema.columns where table_name='%s' and "
            "table_schema='%s';" % (
                escape_single_quote(table_name), escape_single_quote(db_conn.schema)))
    else:
        sql.append("select reltuples from pg_catalog.pg_class where lower(relname::text) like "
        "lower('%s') limit 1;" %
                escape_single_quote(table_name))
        sql.append("select n_distinct, attname, most_common_vals, most_common_freqs "
                   "from pg_catalog.pg_stats where lower(tablename::text) like lower('%s');" %
                   escape_single_quote(table_name))
        sql.append("select c.attname from pg_catalog.pg_class a left join pg_catalog.pg_constraint b on  "
                   "b.conrelid = a.oid left join pg_catalog.pg_attribute c on  c.attrelid = a.oid  "
                   "where c.attnum > 0 and arraycontained(ARRAY[c.attnum], b.conkey) and "
                   "lower(a.relname::text) like lower('%s') limit 1 ;" % escape_single_quote(table_name))
        sql.append(
            "select column_name, column_default from information_schema.columns where "
            "table_name='%s';" % escape_single_quote(table_name))
    sql_count += 4
    table_info = db_conn.execute(sql, sql_count=sql_count)
    return table_info


def get_wdr_info(args, db_conn, database):
    if args.schema:
        schema_temp = args.schema
    else:
        schema_temp = '%$user%'
    sql = "select db_name, schema_name, query, session_id from dbe_perf.statement_history where "
    if args.start_time:
        sql += " start_time >= '%s' and " % args.start_time
    if args.end_time:
        sql += " start_time <= '%s' and " % args.end_time
    sql += " db_name = '%s' and schema_name like '%s' and user_name = '%s' order by thread_id, start_time" % (
        escape_single_quote(database), escape_single_quote(schema_temp), args.user)

    tuples = db_conn.execute([sql])
    return tuples

