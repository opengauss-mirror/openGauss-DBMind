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

from dataclasses import dataclass, field
from typing import List
from functools import lru_cache

from .sql_generator import get_table_info_sql, get_column_info_sql
from .executors.common import BaseExecutor
from .utils import is_multi_node, is_m_compat
from dbmind.common.utils import escape_single_quote, escape_double_quote


@lru_cache(maxsize=None)
def get_table_dist_info(executor, table, schema):
    sql = ("select p.pcattnum, p.pclocatortype, c.oid from pg_catalog.pgxc_class p, "
           " pg_catalog.pg_class c, pg_catalog.pg_namespace n "
           "where p.pcrelid=c.oid and c.relname='%s' "
           "and c.relnamespace = n.oid and n.nspname='%s';") % (escape_single_quote(table), escape_single_quote(schema))
    result = executor.execute_sqls([sql])
    dist_key = ""
    locator_type = ""
    oid = ""
    for cur_tuple in result:
        if len(cur_tuple) == 3:
            dist_key, locator_type, oid = cur_tuple
    dist_key = dist_key.replace(' ', ',')
    dist_key = "(" + escape_double_quote(dist_key) + ")"
    sql = "select attnum, attname from pg_catalog.pg_attribute where attrelid=%s and attnum in %s;" % (oid, dist_key)
    result = executor.execute_sqls([sql])
    dist_cols = []
    for cur_tuple in result:
        if len(cur_tuple) == 2:
            dist_cols.append(cur_tuple[1])
    return dist_cols, locator_type


@lru_cache(maxsize=None)
def get_table_context(origin_table, executor: BaseExecutor):
    reltuples, parttype = None, None

    _is_m_compat = is_m_compat(executor)
    if '.' in origin_table:
        schema, table = origin_table.split('.')
        schemas = [schema]
    else:
        table = origin_table
        schemas = executor.get_schemas()
    for _schema in schemas:
        table_info_sqls = [get_table_info_sql(table, _schema, _is_m_compat)]
        column_info_sqls = [get_column_info_sql(table, _schema, _is_m_compat)]
        for _tuple in executor.execute_sqls(table_info_sqls):
            if len(_tuple) == 2:
                reltuples, parttype = _tuple
                reltuples = int(float(reltuples))
        if not reltuples:
            continue
        is_partitioned_table = True if parttype == 'p' else False
        columns = []
        n_distincts = []
        for _tuple in executor.execute_sqls(column_info_sqls):
            if len(_tuple) != 2:
                continue
            n_distinct, column = _tuple
            if column not in columns:
                columns.append(column)
                n_distincts.append(float(n_distinct))
        if is_multi_node(executor):
            dist_cols, dist_type = get_table_dist_info(executor, table, _schema)
            table_context = TableContext(_schema, table, int(reltuples), columns, n_distincts, is_partitioned_table,
                                         dist_cols, dist_type)
        else:
            table_context = TableContext(_schema, table, int(reltuples), columns, n_distincts, is_partitioned_table)
        return table_context


@dataclass(eq=False)
class TableContext:
    schema: str
    table: str
    reltuples: int
    columns: List = field(default_factory=lambda: [])
    n_distincts: List = field(default_factory=lambda: [])
    is_partitioned_table: bool = field(default=False)
    dist_cols: List = field(default_factory=lambda: [])
    dist_type: str = field(default='')

    @lru_cache(maxsize=None)
    def has_column(self, column):
        is_same_table = True
        if '.' in column:
            if column.split('.')[0].upper() != self.table.split('.')[-1].upper():
                is_same_table = False
            column = column.split('.')[1].lower()
        return is_same_table and column in self.columns

    @lru_cache(maxsize=None)
    def get_n_distinct(self, column):
        column = column.split('.')[-1].lower()
        idx = self.columns.index(column)
        n_distinct = self.n_distincts[idx]
        if float(n_distinct) == float(0):
            return 1
        return 1 / (-n_distinct * self.reltuples) if n_distinct < 0 else 1 / n_distinct
