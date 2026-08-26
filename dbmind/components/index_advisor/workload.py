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

import json
import random
import re
from functools import lru_cache

import sqlparse

from dbmind.components.index_advisor.models import QueryItem
from dbmind.components.index_advisor.process_bar import ProcessBar
from dbmind.components.index_advisor.sql_generator import get_prepare_sqls
from dbmind.components.index_advisor.utils import is_m_compat, ERROR_KEYWORD, has_dollar_placeholder, \
    replace_function_comma, replace_comma_with_dollar


SAMPLE_NUM = 5
MAX_INDEX_COLUMN_NUM = 5
JSON_TYPE = False
GLOBAL_PROCESS_BAR = ProcessBar()
SQL_TYPE = ['select', 'delete', 'insert', 'update']
BLANK = ' '
NUMBER_SET_PATTERN = r'\((\s*(\-|\+)?\d+(\.\d+)?\s*)(,\s*(\-|\+)?\d+(\.\d+)?\s*)*[,]?\)'
SQL_PATTERN = [r'([^\\])\'((\')|(.*?([^\\])\'))',  # match all content in single quotes
               NUMBER_SET_PATTERN,  # match integer set in the IN collection
               r'(([^<>]\s*=\s*)|([^<>]\s+))(\d+)(\.\d+)?']  # match single integer
MULTI_THREAD_NUM = 0


@lru_cache(maxsize=None)
def is_valid_statement(conn, statement):
    """Determine if the query is correct by whether the executor throws an exception."""
    queries = get_prepare_sqls(statement, verbose=False, is_m_compat=is_m_compat(conn), json=True)
    res = conn.execute_sqls(queries)
    # Rpc executor return [] if  the statement is not executed successfully.
    if not res:
        return False
    for _tuple in res:
        if isinstance(_tuple[0], str) and \
                (_tuple[0].upper().startswith(ERROR_KEYWORD) or f' {ERROR_KEYWORD}: ' in _tuple[0].upper()):
            return False
    return res[-1][0]


def multi_is_valid_statement(conn, queries, valid_queries, number):
    l = len(queries)
    for i in range(number, l, MULTI_THREAD_NUM):
        query = queries[i]
        if is_valid_statement(conn, query.get_statement()):
            valid_queries.append(query)
        GLOBAL_PROCESS_BAR.next_bar()


def load_workload(file_path):
    wd_dict = {}
    workload = []
    global BLANK
    with open(file_path, 'r', errors='ignore') as file:
        raw_text = ''.join(file.readlines())
        sqls = sqlparse.split(raw_text)
        for sql in sqls:
            if any(re.search(r'((\A|[\s(,])%s[\s*(])' % tp, sql.lower()) for tp in SQL_TYPE):
                TWO_BLANKS = BLANK * 2
                while TWO_BLANKS in sql:
                    sql = sql.replace(TWO_BLANKS, BLANK)
                if sql.strip() not in wd_dict.keys():
                    wd_dict[sql.strip()] = 1
                else:
                    wd_dict[sql.strip()] += 1
    for sql, freq in wd_dict.items():
        workload.append(QueryItem(sql, freq))

    return workload


def get_workload_template(workload):
    templates = {}
    placeholder = r'@@@'

    for item in workload:
        sql_template = item.get_statement()
        for pattern in SQL_PATTERN:
            sql_template = re.sub(pattern, placeholder, sql_template)
        if sql_template not in templates:
            templates[sql_template] = {}
            templates[sql_template]['cnt'] = 0
            templates[sql_template]['samples'] = []
        templates[sql_template]['cnt'] += item.get_frequency()
        # reservoir sampling
        statement = item.get_statement()
        if has_dollar_placeholder(statement):
            statement = replace_function_comma(statement)
            statement = replace_comma_with_dollar(statement)
        if len(templates[sql_template]['samples']) < SAMPLE_NUM:
            templates[sql_template]['samples'].append(statement)
        else:
            if random.randint(0, templates[sql_template]['cnt']) < SAMPLE_NUM:
                templates[sql_template]['samples'][random.randint(0, SAMPLE_NUM - 1)] = \
                    statement

    return templates


def compress_workload(input_path):
    compressed_workload = []
    if isinstance(input_path, dict):
        templates = input_path
    elif JSON_TYPE:
        with open(input_path, 'r', errors='ignore') as file:
            templates = json.load(file)
    else:
        workload = load_workload(input_path)
        templates = get_workload_template(workload)

    for _, elem in templates.items():
        for sql in elem['samples']:
            compressed_workload.append(
                QueryItem(sql.strip(), elem['cnt'] / len(elem['samples'])))

    return compressed_workload

