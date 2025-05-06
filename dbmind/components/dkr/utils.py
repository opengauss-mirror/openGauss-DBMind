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

import logging
import os
import re
from enum import Enum
from itertools import groupby

import sqlparse

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# types of DML queries involved in distributed scenarios.
DIST_SUPPORT_TYPE = ['SELECT', 'UPDATE', 'INSERT', 'DELETE']
# types of DML queries involved in replication table scenarios.
REPL_SUPPORT_TYPE = {
    'UPDATE': len(['UPDATE']),
    'INSERT': len(['INSERT', 'INTO']),
    'DELETE': len(['DELETE FROM'])
}
# sql formatting
SQL_PATTERN = [
    r'([^\\])\'((\')|(.*?([^\\])\'))',  # match all content in single quotes
    r'\((\s*(\-|\+)?\d+(\.\d+)?\s*)(,\s*(\-|\+)?\d+(\.\d+)?\s*)*\)',  # match integer set in the IN collection
    r'(([<>=]+\s*)|(\s+))(\-|\+)?\d+(\.\d+)?'  # match single integer
]
PLACEHOLDER = r'@@@'

# threshold params
TUPLE_COUNT_THOLD = 2e+05
FILTER_COND_THOLD = 0.05
GROUP_ORDER_PERCENTAGE = 0.3
DATA_SKEW_THOLD = 0.5

# const strings
DOT = '.'
BLANK = ' '
SHARP = '#'
TRANS = '::'
QUOTE = '"'
COMMA = ','
SEMICOLON = ';'
SYSTEM_TABLE_SEARCH = 'select relname from pg_catalog.pg_class where relnamespace=11;'
DEPLOY_MODE_SEARCH = "select pg_catalog.count(*) from pg_catalog.pgxc_node where node_type='C';"


class PGSysTbl(Enum):
    PG_series = 'PG_'
    GS_series = 'GS_'
    PGXC_series = 'PGXC_'
    STATEMENT_HISTORY = 'STATEMENT_HISTORY'
    PLAN_TABLE_DATA = 'PLAN_TABLE'


class QueryItem:

    def __init__(self, sql, frequency):
        self.statement = _format(sql)
        self.frequency = frequency
        self.table_names = None


class SqlRecord:

    def __init__(self):
        self.transaction_stacked = []
        self.sql = ''


def common_format(sql):
    sql = sql.replace('\n', BLANK).replace('\t', BLANK)
    # Make only one blank around operators.
    while BLANK * 2 in sql:
        sql = sql.replace(BLANK * 2, BLANK)
    return sql


def _format(sql):
    formatted = sqlparse.format(sql,
                                strip_comments=True,
                                use_space_around_operators=True,
                                indent_tabs=True,
                                keyword_case='upper')
    formatted = common_format(formatted)

    return formatted


def _if_trans_start(sql):
    if BLANK.join(sql.strip().lower().split()).endswith(('begin transaction', 'begin',
                                                         'start transaction')):
        return True
    return False


def _if_trans_end(sql):
    if sql.strip().lower().endswith(('rollback', 'commit', 'end', 'end transaction')):
        return True
    return False


def is_valid_sql(sql):
    # filter system table
    if any(re.search(r'\sFROM\s+%s' % item.value, sql.upper()) for item in PGSysTbl):
        return False
    # filter minimalist select statement of watching live
    if DIST_SUPPORT_TYPE[0] in sql.upper():
        if not re.search(r'\sFROM\s', sql.upper()):
            return False
    elif not any(tp in sql.upper() for tp in DIST_SUPPORT_TYPE[1:]):
        return False
    return True


def _common_format_sqls(line, temp_record, if_parse_transaction=True):
    def dealwith_end():
        if is_valid_sql(SEMICOLON.join(temp_record.transaction_stacked[1:])):
            yield SEMICOLON.join(temp_record.transaction_stacked[1:])
        temp_record.transaction_stacked = []
        temp_record.sql = ''

    def dealwith_start(sql):
        temp_record.transaction_stacked.append(sql)
        temp_record.sql = ''

    for item in line.strip().split(SEMICOLON)[:-1]:
        if temp_record.sql:
            item = '{}{}{}'.format(temp_record.sql, BLANK, item)
        if if_parse_transaction and _if_trans_end(item.strip()):
            yield from dealwith_end()
        elif if_parse_transaction and (_if_trans_start(item.strip()) or temp_record.transaction_stacked):
            dealwith_start(item.strip())
        elif is_valid_sql(item.strip()):
            yield item.strip()
        temp_record.sql = ''
    if line.strip().split(SEMICOLON)[-1]:
        temp_record.sql += line.split(SEMICOLON)[-1]
        if if_parse_transaction and _if_trans_end(temp_record.sql):
            yield from dealwith_end()
        elif if_parse_transaction and _if_trans_start(temp_record.sql):
            dealwith_start(temp_record.sql)


def get_wdr_sqls(tuples):
    temp_sql = ''
    query_id_index = 2
    session_id_index = 3
    for key, session_group in groupby(tuples, key=lambda x: x[session_id_index]):
        temp_record = SqlRecord()
        for item in session_group:
            if item[query_id_index].endswith('+'):
                temp_sql = '{}{}{}'.format(temp_sql, item[query_id_index][:-1].strip(), BLANK)
            else:
                temp_sql = '{}{}'.format(temp_sql, item[query_id_index].strip())
                if not temp_sql.endswith(SEMICOLON):
                    temp_sql = '{}{}'.format(temp_sql, SEMICOLON)
                yield from _common_format_sqls(temp_sql, temp_record)
                temp_sql = ''


def get_sqls(file, standard=False):
    line = file.readline()
    if standard:
        while line:
            yield line
            line = file.readline()
    temp_record = SqlRecord()
    while line:
        yield from _common_format_sqls(line, temp_record)
        line = file.readline()


def _yellow(text):
    return '\033[33m%s\033[0m' % text


def _green(text):
    return '\033[32m%s\033[0m' % text


def display_recommend_result(workload_info, dkr, replications):
    # Output a header first, which looks more beautiful.
    header = ' Distribution Key Recommended Result '
    try:
        term_width = os.get_terminal_size().columns
        # The width of each of the two sides of the terminal.
        side_width = (term_width - len(header)) // 2
    except:
        side_width = 0
    title = SHARP * side_width + header + SHARP * side_width
    print(_green(title))

    if not dkr and not replications:
        logging.error('No recommended results for distribution key and replication table.')
        return

    # Sorted by table name.
    dkr = sorted(dkr.items(), key=lambda elem: elem[0])
    for table_name, column_name in dkr:
        if table_name not in replications:
            print('CREATE TABLE %s(...) DISTRIBUTE BY HASH(%s);' %
                  (_yellow(table_name), _yellow(column_name)))
    for table_name in sorted(replications):
        print('HINT: table %s can be set as a replication table. (table rows = %s)' %
              (_yellow(table_name), _green(workload_info.wl_tables[table_name].tuple_count)))
