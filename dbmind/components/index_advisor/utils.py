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
import getpass
import logging
import os

import re
from functools import lru_cache
from itertools import chain, combinations
from logging.handlers import RotatingFileHandler
from contextlib import contextmanager

import sqlparse
from sqlparse.tokens import Name
from sqlparse.sql import Function, Parenthesis, IdentifierList

from dbmind.common.utils import escape_double_quote, escape_back_quote
from dbmind.common.utils.cli import read_input_from_pipe
from dbmind.components.index_advisor.process_bar import bar_print

COLUMN_DELIMITER = ', '
QUERY_PLAN_SUFFIX = 'QUERY PLAN'
EXPLAIN_SUFFIX = 'EXPLAIN'
ERROR_KEYWORD = 'ERROR'
PREPARE_KEYWORD = 'PREPARE'
SHARP = '#'


def replace_function_comma(statement):
    """Replace the ? in function to the corresponding value to ensure that prepare execution can be executed properly"""
    function_value = {'count': '1', 'decode': "'1'"}
    new_statement = ''
    for token in get_tokens(statement):
        value = token.value
        if token.ttype is Name.Placeholder and token.value == '?':
            function_token = None
            if isinstance(token.parent, Parenthesis) and isinstance(token.parent.parent, Function):
                function_token = token.parent.parent
            elif isinstance(token.parent, IdentifierList) \
                    and isinstance(token.parent.parent, Parenthesis) \
                    and isinstance(token.parent.parent.parent, Function):
                function_token = token.parent.parent.parent
            if function_token:
                replaced_value = function_value.get(function_token.get_name().lower(), None)
                value = replaced_value if replaced_value else value
        new_statement += value
    return new_statement


def singleton(cls):
    instances = {}

    def _singleton(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return _singleton


def match_table_name(table_name, tables):
    for elem in tables:
        item_tmp = '_'.join(elem.split('.'))
        if table_name == item_tmp:
            table_name = elem
            break
        elif 'public_' + table_name == item_tmp:
            table_name = 'public.' + table_name
            break
    else:
        return False, table_name
    return True, table_name


@lru_cache(maxsize=None)
def get_tokens(query):
    return list(sqlparse.parse(query)[0].flatten())


@lru_cache(maxsize=None)
def has_dollar_placeholder(query):
    tokens = get_tokens(query)
    return any(item.ttype is Name.Placeholder for item in tokens)


@lru_cache(maxsize=None)
def get_placeholders(query):
    placeholders = set()
    for item in get_tokens(query):
        if item.ttype is Name.Placeholder:
            placeholders.add(item.value)
    return placeholders


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


@lru_cache(maxsize=None)
def is_multi_node(executor):
    sql = "select pg_catalog.count(*) from pg_catalog.pgxc_node where node_type='C';"
    for cur_tuple in executor.execute_sqls([sql]):
        if str(cur_tuple[0]).isdigit():
            return int(cur_tuple[0]) > 0


@lru_cache(maxsize=None)
def is_m_compat(executor):
    sql = "SHOW sql_compatibility;"
    for cur_tuple in executor.execute_sqls([sql]):
        if str(cur_tuple[0]) == 'M':
            return True
    return False


@contextmanager
def hypo_index_ctx(executor):
    yield
    executor.execute_sqls(['SELECT pg_catalog.hypopg_reset_index();'])


def split_integer(m, n):
    quotient = int(m / n)
    remainder = m % n
    if m < n:
        return [1] * m
    if remainder > 0:
        return [quotient] * (n - remainder) + [quotient + 1] * remainder
    if remainder < 0:
        return [quotient - 1] * -remainder + [quotient] * (n + remainder)
    return [quotient] * n


def split_iter(iterable, n):
    size_list = split_integer(len(iterable), n)
    index = 0
    res = []
    for size in size_list:
        res.append(iterable[index:index + size])
        index += size
    return res


def flatten(iterable):
    for _iter in iterable:
        if hasattr(_iter, '__iter__') and not isinstance(_iter, str):
            for item in flatten(_iter):
                yield item
        else:
            yield _iter


def quote_columns(values, is_m_compat=False):
    if is_m_compat:
        escape_func = escape_back_quote
        ident_quoting_char = '`'
    else:
        escape_func = escape_double_quote
        ident_quoting_char = '"'

    res = escape_func(values).replace(', ', f'{ident_quoting_char}, {ident_quoting_char}')
    res = f'{ident_quoting_char}{res}{ident_quoting_char}'
    return res


def quote_table(name, is_m_compat=False):
    if is_m_compat:
        escape_func = escape_back_quote
        ident_quoting_char = '`'
    else:
        escape_func = escape_double_quote
        ident_quoting_char = '"'

    schemaname = escape_func(name.split('.')[0])
    tablename = escape_func(name.split('.')[1])
    res = f'{ident_quoting_char}{schemaname}{ident_quoting_char}.{ident_quoting_char}{tablename}{ident_quoting_char}'
    return res


def is_valid_string(s):
    """
    判断字符串是否只包含 [a-z, A-Z, 0-9, _] 字符。

    :param s: 输入字符串
    :return: 如果字符串有效返回 True，否则返回 False
    """
    # 使用正则表达式匹配
    pattern = r'^[a-zA-Z0-9_]+$'
    return bool(re.match(pattern, s))


def path_type(path):
    realpath = os.path.realpath(path)
    if os.path.exists(realpath):
        return realpath
    raise argparse.ArgumentTypeError('%s is not a valid path.' % path)


def set_logger():
    logfile = 'dbmind_index_advisor.log'
    handler = RotatingFileHandler(
        filename=logfile,
        maxBytes=100 * 1024 * 1024,
        backupCount=5,
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def get_password():
    password = read_input_from_pipe()
    if password:
        logging.warning("Read password from pipe.")
    else:
        password = getpass.getpass("Password for database user:")
    if not password:
        raise ValueError('Please input the password')
    return password


def green(text):
    return '\033[32m%s\033[0m' % text


def powerset(iterable):
    """ powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3) """
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s) + 1))


def print_header_boundary(header):
    # Output a header first, which looks more beautiful.
    try:
        term_width = os.get_terminal_size().columns
        # Get the width of each of the two sides of the terminal.
        side_width = (term_width - len(header)) // 2
    except (AttributeError, OSError):
        side_width = 0
    title = SHARP * side_width + header + SHARP * side_width
    bar_print(green(title))

