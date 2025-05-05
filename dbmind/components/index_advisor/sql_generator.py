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

from itertools import count
from functools import lru_cache
import logging

from .utils import get_placeholders, has_dollar_placeholder, replace_comma_with_dollar, replace_function_comma, \
    quote_columns, quote_table
from dbmind.common.utils import escape_single_quote

counter = count(start=0, step=1)


def get_existing_index_sql(schema, tables):
    tables_string = ','.join(["'%s'" % escape_single_quote(table) for table in tables])
    # Query all table indexes information and primary key information.
    sql = "SELECT c.relname AS tablename, i.relname AS indexname, " \
          "pg_catalog.pg_get_indexdef(i.oid) AS indexdef, p.contype AS pkey from " \
          "pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN " \
          "pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n " \
          "ON n.oid = c.relnamespace LEFT JOIN pg_constraint p ON (i.oid = p.conindid " \
          "AND p.contype = 'p') WHERE (c.relkind = ANY (ARRAY['r'::char, " \
          "'m'::char])) AND (i.relkind = ANY (ARRAY['i'::char, 'I'::char, 'G'::char])) " \
          "AND n.nspname = '%s' AND c.relname in (%s) order by c.relname;" % \
          (escape_single_quote(schema), tables_string)
    logging.info(f"{escape_single_quote(schema)} {tables_string}")
    return sql


@lru_cache(maxsize=None)
def get_prepare_sqls(statement, verbose=False, is_m_compat=False, json=False):
    if has_dollar_placeholder(statement):
        statement = replace_function_comma(statement)
        statement = replace_comma_with_dollar(statement)
    prepare_id = 'prepare_' + str(next(counter))
    placeholder_size = len(get_placeholders(statement))
    prepare_args = '' if not placeholder_size else '(%s)' % (','.join(['NULL'] * placeholder_size))
    prepare_sqls = []
    prepare_sqls.append("SET explain_perf_mode = 'normal'; ")
    prepare_sqls.append("SET plan_cache_mode = 'force_generic_plan';")
    if is_m_compat:
        prepare_sqls.append(f"""prepare {prepare_id} from '{escape_single_quote(statement)}'""")
    else:
        prepare_sqls.append(f'prepare {prepare_id} as {statement}')
    if verbose:
        prepare_sqls.append(f'explain verbose execute {prepare_id}{prepare_args}')
    elif json:
        prepare_sqls.append(f'explain (format json) execute {prepare_id}{prepare_args}')
    else:
        prepare_sqls.append(f'explain execute {prepare_id}{prepare_args}')
    prepare_sqls.append(f'deallocate prepare {prepare_id}')
    return prepare_sqls


def get_workload_cost_sqls(statements, indexes, is_multi_node, is_m_compat=False):
    sqls = []
    if indexes:
        # Create hypo-indexes.
        sqls.append('SET enable_hypo_index = on;\n')
        for index in indexes:
            if index.get_index_type() != 'gsi':
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE INDEX ON %s(%s) %s');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat),
                             index.get_index_type()))
            elif index.get_containing():
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX ON %s(%s) CONTAINING (%s)');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat),
                             quote_columns(index.get_containing(), is_m_compat)))
            else:
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX ON %s(%s)');" %
                            (quote_table(index.get_table(), is_m_compat),
                             quote_columns(index.get_columns(), is_m_compat)))
    if is_multi_node:
        sqls.append('SET enable_fast_query_shipping = off;')
        sqls.append('SET enable_stream_operator = on; ')
    sqls.append("SET explain_perf_mode = 'normal'; ")
    sqls.append("SET plan_cache_mode = 'force_generic_plan';")
    for index, statement in enumerate(statements):
        sqls.extend(get_prepare_sqls(statement, verbose=False, is_m_compat=is_m_compat))
    return sqls


def get_lp_check_sqls(query, is_multi_node):
    if not is_multi_node:
        return None
    sqls = get_hypo_index_head_sqls(is_multi_node, True)[:]
    sqls.append('explain ' + query)
    return sqls


def get_index_setting_sqls(indexes, is_multi_node, is_m_compat=False):
    sqls = get_hypo_index_head_sqls(is_multi_node)[:]
    if indexes:
        # Create hypo-indexes.
        for index in indexes:
            if index.get_index_type() != 'gsi':
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE INDEX ON %s(%s) %s');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat),
                             index.get_index_type()))
            elif index.get_containing():
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX ON %s(%s) CONTAINING (%s)');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat),
                             quote_columns(index.get_containing(), is_m_compat)))
            else:
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX ON %s(%s)');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat)))
    return sqls


def session_get_index_setting_sqls(indexes, is_multi_node, is_m_compat=False):
    sqls = get_hypo_index_head_sqls(is_multi_node)[:]
    if indexes:
        # Create hypo-indexes.
        for index in indexes:
            if index.get_index_type() != 'gsi':
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE INDEX ON %s(%s) %s');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat),
                             index.get_index_type()))
            elif index.get_containing():
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX "
                            "ON %s(%s) CONTAINING (%s)');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat),
                             quote_columns(index.get_containing(), is_m_compat)))
            else:
                sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX ON %s(%s)');" %
                            (quote_table(index.get_table(), is_m_compat), quote_columns(index.get_columns(),
                                                                                        is_m_compat)))
    return sqls


def session_get_index_storage_sqls(indexes, is_multi_node, is_m_compat=False):
    session_index_set_sqls = session_get_index_setting_sqls(indexes, is_multi_node, is_m_compat)
    index_storage_sqls = []
    for sql in session_index_set_sqls:
        if 'hypopg_create_index' in sql:
            index_storage_sqls.append(f"""with temp as (select indexrelid from {sql[len('SELECT'):].strip(';')} as indexrelid)
                                      select (select indexrelid from temp), (select 
                                      pg_catalog.hypopg_estimate_size((select indexrelid from temp))
                                      );""")
        else:
            index_storage_sqls.append(sql)
    return index_storage_sqls


def get_single_advisor_sql(ori_sql):
    advisor_sql = 'select pg_catalog.gs_index_advise(\''
    for elem in ori_sql:
        if elem == '\'':
            advisor_sql += '\''
        advisor_sql += elem
    advisor_sql += '\');'
    return advisor_sql


@lru_cache(maxsize=None)
def get_hypo_index_head_sqls(is_multi_node, is_fqs_on=False):
    sqls = ['SET enable_hypo_index = on;']
    if is_multi_node:
        if is_fqs_on:
            sqls.append('SET enable_fast_query_shipping = on;')
            sqls.append('SET max_datanode_for_plan = 0;')
            sqls.append('SET enable_stream_operator = off;')
        else:
            sqls.append('SET enable_fast_query_shipping = off;')
            sqls.append('SET enable_stream_operator = on;')
        sqls.append('SET enable_gsiscan = on;')
    sqls.append("SET explain_perf_mode = 'normal'; ")
    sqls.append("SET plan_cache_mode = 'force_generic_plan';")
    return sqls


def get_index_check_sqls(query, indexes, is_multi_node, is_fqs_on=False, is_m_compat=False):
    sqls = get_hypo_index_head_sqls(is_multi_node, is_fqs_on=is_fqs_on)[:]
    for index in indexes:
        table = index.get_table()
        columns = index.get_columns()
        index_type = index.get_index_type()
        containing = index.get_containing()
        if index_type != "gsi":
            sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE INDEX ON %s(%s) %s')" %
                        (quote_table(table, is_m_compat), quote_columns(columns, is_m_compat), index_type))
        elif containing:
            sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX "
                        "ON %s(%s) CONTAINING (%s)')" %
                        (quote_table(table, is_m_compat), quote_columns(columns, is_m_compat),
                         quote_columns(containing, is_m_compat)))
        else:
            sqls.append("SELECT pg_catalog.hypopg_create_index('CREATE GLOBAL INDEX ON %s(%s)')" %
                        (quote_table(table, is_m_compat), quote_columns(columns, is_m_compat)))
    sqls.append("SELECT pg_catalog.hypopg_display_index()")
    sqls.append("SET explain_perf_mode = 'normal';")
    sqls.append("SET plan_cache_mode = 'force_generic_plan';")
    if is_fqs_on:
        prepare_sqls = get_prepare_sqls(query, verbose=True, is_m_compat=is_m_compat)
    else:
        prepare_sqls = get_prepare_sqls(query, verbose=False, is_m_compat=is_m_compat)
    sqls.extend(prepare_sqls)
    sqls.append("SELECT pg_catalog.hypopg_reset_index()")
    return sqls


def get_table_info_sql(table, schema, is_m_compat=False):
    table_info_sql = f"select reltuples, parttype from pg_catalog.pg_class " \
                     f"where relname ilike '{escape_single_quote(table)}' " \
                     f"and relnamespace = (select oid from pg_catalog.pg_namespace where " \
                     f"nspname = '{escape_single_quote(schema)}');"
    if is_m_compat:
        table_info_sql = f"select reltuples, parttype from pg_catalog.pg_class " \
                         f"where lower(relname::text) like lower('{escape_single_quote(table)}') " \
                         f"and relnamespace = (select oid from pg_catalog.pg_namespace where " \
                         f"nspname = '{escape_single_quote(schema)}');"
    return table_info_sql


def get_column_info_sql(table, schema, is_m_compat=False):
    column_info_sql = f"select n_distinct, attname from pg_catalog.pg_stats " \
                      f"where tablename ilike '{escape_single_quote(table)}' " \
                      f"and schemaname = '{escape_single_quote(schema)}';"
    if is_m_compat:
        column_info_sql = f"select n_distinct, attname from pg_catalog.pg_stats " \
                          f"where lower(tablename::text) like lower('{escape_single_quote(table)}') " \
                          f"and schemaname = '{escape_single_quote(schema)}';"
    return column_info_sql
