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

import ast
import re
from datetime import datetime
from itertools import count
import logging

import sqlparse
from mo_sql_parsing import parse, format
from sqlparse.sql import Identifier
from sqlparse.sql import Where, Comparison
from sqlparse.tokens import Keyword, DML
from sqlparse.tokens import Name

OPERATOR = ('lt', 'lte', 'gt', 'gte', 'eq', 'neq')


def analyze_column(column, where_clause):
    for tokens in where_clause.tokens:
        if isinstance(tokens, Comparison) and isinstance(tokens.left, Identifier):
            column.add(tokens.left.value)


def get_columns(sql):
    column = set()
    parsed_tree = sqlparse.parse(sql)[0]
    for item in parsed_tree:
        if isinstance(item, Where):
            analyze_column(column, item)
    return list(column)


def get_indexes(dbagent, sql, timestamp):
    """
    Get indexes of SQL from dataset.
    :param timestamp:
    :param dbagent: obj, interface for sqlite3.
    :param sql: str, query.
    :return: list, the set of indexes.
    """
    indexes = []
    indexes_dict = dbagent.fetch_all_result("SELECT indexes from wdr where timestamp ==\"{timestamp}\""
                                            " and query == \"{query}\"".format(timestamp=timestamp,
                                                                               query=sql))
    if len(indexes_dict):
        try:
            indexes_dict = ast.literal_eval(indexes_dict[0][0])
            indexes_def_list = list(list(indexes_dict.values())[0].values())
            for sql_index in indexes_def_list:
                value_in_bracket = re.compile(r'[(](.*?)[)]', re.S)
                indexes.append(re.findall(value_in_bracket, sql_index)[0].split(',')[0])
        except Exception as e:
            logging.exception(e)
            return indexes
    return indexes


def wdr_sql_processing(sql):
    standard_sql = standardize_sql(sql)
    standard_sql = re.sub(r';', r'', standard_sql)
    standard_sql = re.sub(r'VALUES (\(.*\))', r'VALUES', standard_sql)
    standard_sql = re.sub(r'\$\d+?', r'?', standard_sql)
    return standard_sql


def check_select(parsed_sql):
    if not parsed_sql.is_group:
        return False
    for token in parsed_sql.tokens:
        if token.ttype is DML and token.value.upper() == 'SELECT':
            return True
    return False


def get_table_token_list(parsed_sql, token_list):
    flag = False
    for token in parsed_sql.tokens:
        if not flag:
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                flag = True
        else:
            if check_select(token):
                get_table_token_list(token, token_list)
            elif token.ttype is Keyword:
                return
            else:
                token_list.append(token)


def standardize_sql(sql):
    return sqlparse.format(
        sql, keyword_case='upper', identifier_case='lower', strip_comments=True,
        use_space_around_operators=True, strip_whitespace=True
    )


def is_num(input_str):
    if isinstance(input_str, str) and re.match(r'^\d+\.?\d+$', input_str):
        return True
    return False


def str2int(input_str):
    return int(re.match(r'^(\d+)\.?\d+$', input_str).groups()[0])


def to_ts(obj):
    if isinstance(obj, str):
        if '.' in obj:
            obj = obj.split('.')[0]
        try:
            timestamp = int(datetime.strptime(obj, '%Y-%m-%d %H:%M:%S').timestamp())
            return timestamp
        except Exception as e:
            logging.exception(e)
            return 0
    elif isinstance(obj, datetime):
        return int(obj.timestamp())
    elif isinstance(obj, int):
        return obj
    else:
        return 0


def fill_value(query_content):
    """
    Fill specific values into the SQL statement for parameters,
      input: select id from table where info = $1 and id_d < $2; PARAMETERS: $1 = 1, $2 = 4;
      output: select id from table where info = '1' and id_d < '4';
    """
    if len(query_content.split(';')) == 2 and 'parameters: $1' in query_content.lower():
        template, parameter = query_content.split(';')
    else:
        return query_content
    param_list = re.search(r'parameters: (.*)', parameter,
                           re.IGNORECASE).group(1).split(', $')
    param_list = list(param.split('=', 1) for param in param_list)
    param_list.sort(key=lambda x: int(x[0].strip(' $')),
                    reverse=True)
    for item in param_list:
        template = template.replace(item[0].strip() if re.match(r'\$', item[0]) else
                                    ('$' + item[0].strip()), item[1].strip())
    return template


def exists_regular_match(query):
    """Check if there is such a regular case in SQL: like '%xxxx', 'xxxx%', '%xxxx%'"""
    if re.search(r"(like\s+'(%.+)'|like\s+'(.+%)')", query):
        return True
    return False


def remove_parameter_part(query):
    return re.sub(r";\s*parameters: \$.+", ";", query, flags=re.IGNORECASE)


def exists_function(query):
    """
    Determine if a function is used in a subquery,
      select * from table where abs(l_quantity) <= 8;
    """
    flag = []

    def _parser(parsed_sql):
        if flag:
            return
        if isinstance(parsed_sql, list):
            if isinstance(parsed_sql[0], dict):
                flag.append(True)
                return
        if isinstance(parsed_sql, dict):
            if 'where' in parsed_sql:
                _parser(parsed_sql['where'])
            elif not any(item in parsed_sql for item in OPERATOR):
                for _, sub_parsed_sql in parsed_sql.items():
                    if isinstance(sub_parsed_sql, list):
                        for item in sub_parsed_sql:
                            _parser(item)
                    else:
                        _parser(sub_parsed_sql)
            elif any(item in parsed_sql for item in OPERATOR):
                for _, sub_parsed_sql in parsed_sql.items():
                    _parser(sub_parsed_sql)
    try: 
        parsed_sql = parse(query)
        _parser(parsed_sql)
    except Exception as e:
        logging.exception(e)
    return flag


def _regular_match(string, pattern):
    """
    Provides simple regularization functions.
    """
    if re.search(pattern, string):
        return True
    return False


def exists_bool_clause(query):
    """
    Get boolean expression in SQL, there are two cases:
      1. select * from table where col in (xx, xx, xx, ...);
      2. select * from table where col in (select xxx);
    Our purpose is to get: '(xx, xx, xx, ...)' and 'select xxx'
    """
    flags = []

    def _parser(parsed_sql):
        if isinstance(parsed_sql, list):
            for item in parsed_sql:
                _parser(item)
        if isinstance(parsed_sql, dict):
            for key, value in parsed_sql.items():
                if key in ('in', 'nin') and isinstance(value, list):
                    if isinstance(value[1], dict):
                        flags.append(format(value[1]))
                    else:
                        flags.append(value[1])
                    _parser(value[-1])
                else:
                    _parser(value)
    try:
        parsed_sql = parse(query)
        _parser(parsed_sql)
    except Exception as e:
        logging.exception(e)
    return flags


def exists_related_select(query):
    """
    Determine whether there is a correlated subquery in the where condition of SQL,
    the current method is inaccurate and can only be roughly judged.
    """
    flags = []

    def _parser(parsed_sql):
        if isinstance(parsed_sql, list):
            for item in parsed_sql:
                _parser(item)
        if isinstance(parsed_sql, dict):
            for key, value in parsed_sql.items():
                if key in ('eq', 'lt', 'gt') and all(isinstance(item, str) for item in value) and len(value) == 2:
                    flags.append(value[1])
                else:
                    _parser(value)
    try:
        parsed_sql = parse(query)
        _parser(parsed_sql)
    except Exception as e:
        logging.exception(e)
    return flags


def exists_subquery(query):
    """
    Determine if there is a subquery in SQL.
    """
    flags = []

    def _parser(parsed_sql):
        if isinstance(parsed_sql, list):
            for item in parsed_sql:
                _parser(item)
        if isinstance(parsed_sql, dict):
            for key, value in parsed_sql.items():
                if key == 'select':
                    subquery = format(parsed_sql)
                    if parse(subquery) != parse(query):
                        flags.append(subquery)
                _parser(value)
    try:
        parsed_sql = parse(query)
        _parser(parsed_sql)
    except Exception as e:
        logging.exception(e)
    return flags


def get_placeholders(query):
    placeholders = set()
    for item in sqlparse.parse(query)[0].flatten():
        if item.ttype is Name.Placeholder:
            placeholders.add(item.value)
    return placeholders


def get_generate_prepare_sqls_function():
    counter = count(start=0, step=1)

    def get_prepare_sqls(statement):
        prepare_id = 'prepare_' + str(next(counter))
        placeholder_size = len(get_placeholders(statement))
        prepare_args = '' if not placeholder_size else '(%s)' % (','.join(['NULL'] * placeholder_size))
        return [f'prepare {prepare_id} as {statement}', f'explain execute {prepare_id}{prepare_args}',
                f'deallocate prepare {prepare_id}']

    return get_prepare_sqls


def replace_comma_with_dollar(query):
    """
    Replacing '?' with '$+Numbers' in SQL:
      input: UPDATE bmsql_customer SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + ?
      WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4 and c_info = ?;
      output: UPDATE bmsql_customer SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + $5
      WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4 and c_info = $6;
    note: if track_stmt_parameter is off, all '?' in SQL need to be replaced
    """
    if '?' not in query:
        return query
    max_dollar_number = 0
    dollar_parts = re.findall(r'(\$\d+)', query)
    if dollar_parts:
        max_dollar_number = max(int(item.strip('$')) for item in dollar_parts)
    while '?' in query:
        dollar = "$%s" % (max_dollar_number + 1)
        query = query.replace('?', dollar, 1)
        max_dollar_number += 1
    return query

