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
import re
from collections import OrderedDict

JOIN_KEYWORDS = ['join', 'left join', 'right join', 'inner join', 'full join']

# Align with dbmind.common.utils.base.is_valid_obj, plus optional schema.table.
_SAFE_SQL_IDENTIFIER_RE = re.compile(
    r'^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)?$'
)


def is_safe_sql_identifier(name):
    """Return True if name is a safe SQL identifier (no quote/injection chars)."""
    if not isinstance(name, str) or not name:
        return False
    return bool(_SAFE_SQL_IDENTIFIER_RE.fullmatch(name))


def validate_sql_identifier(name, kind='identifier'):
    if not is_safe_sql_identifier(name):
        raise ValueError('Invalid SQL %s: %r' % (kind, name))
    return name


def quote_sql_literal(value):
    """Escape and quote a value as a SQL string literal."""
    if not isinstance(value, str):
        raise ValueError('SQL literal must be a string.')
    return "'" + value.replace("'", "''") + "'"


def quote_sql_identifier(name):
    """Double-quote a validated SQL identifier (supports schema.table)."""
    validate_sql_identifier(name)
    return '.'.join('"%s"' % part.replace('"', '""') for part in name.split('.'))


def get_table_names(from_clause, table_names=None):
    if table_names is None:
        table_names = OrderedDict()
    if isinstance(from_clause, str):
        table_names[from_clause] = table_names.get(from_clause, []) + [from_clause]
    elif isinstance(from_clause, dict) and 'value' in from_clause:
        table_names[from_clause['value']] = table_names.get(from_clause['value'], []) + [from_clause['name']]
    elif isinstance(from_clause, dict):
        isjoin = False
        for join_keyword in JOIN_KEYWORDS:
            if join_keyword in from_clause:
                get_table_names(from_clause[join_keyword], table_names)
                isjoin = True
        if not isjoin:
            return dict()
    elif isinstance(from_clause, list):
        for sub_from_clause in from_clause:
            get_table_names(sub_from_clause, table_names)
    return table_names


def get_columns(table2columns, parsed_sql):
    select_values = []
    table_names = get_table_names(parsed_sql['from'])
    for table_name, alias_names in table_names.items():
        columns = table2columns.get(table_name, [])
        for alias_name in alias_names:
            for column in columns:
                select_values.append(
                    {'value': column if len(table_names.keys()) == 1 and all(
                        len(alias_names) == 1 for alias_names in
                        table_names.values()) else alias_name + '.' + column})
    return select_values
