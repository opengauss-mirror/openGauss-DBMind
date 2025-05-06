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

import re

import sqlparse
from sqlparse.tokens import Name, Punctuation, Whitespace, Keyword, DML
from sqlparse.sql import IdentifierList, Identifier, Comparison, Parenthesis

TABLE_RELATED_KEYWORDS = {"FROM",
                          "JOIN",
                          "CROSSJOIN",
                          "INNERJOIN",
                          "FULLJOIN",
                          "FULLOUTERJOIN",
                          "LEFTJOIN",
                          "RIGHTJOIN",
                          "LEFTOUTERJOIN",
                          "RIGHTOUTERJOIN",
                          "INTO",
                          "UPDATE",
                          "TABLE"}

KEYWORDS = {'WHERE', 'VALUES', 'LEFTJOIN', 'UPDATE',
            'OFFSET', 'ON', 'GROUPBY', 'RIGHTJOIN',
            'FULLJOIN', 'WINDOW', 'SET', 'WITH',
            'DELETE', 'INNERJOIN', 'TABLE', 'LIMIT',
            'USING', 'REPLACE', 'FULLOUTERJOIN',
            'RETURNING', 'LEFTOUTERJOIN', 'INDEX',
            'FROM', 'INSERT', 'ORDERBY', 'INTO',
            'SELECT', 'CROSSJOIN', 'RIGHTOUTERJOIN', 'JOIN'}


def get_query_tables(sql: str):
    sql = sql.replace('"', '')
    tables = []
    parsed = sqlparse.parse(sql)
    in_parenthesis = 0
    previous_token = None
    last_related_keyword_token = None
    tokens = list([token for token in parsed[0].flatten()
                   if not token.ttype is Whitespace and not token.ttype.parent is Whitespace])
    for index, token in enumerate(tokens):
        if str(token) == '(':
            in_parenthesis += 1
        elif str(token) == ')':
            in_parenthesis -= 1
        is_keyword = token.is_keyword or (token.ttype.parent is Name and token.ttype is not Name)
        if (token.ttype is Name or is_keyword) \
                and last_related_keyword_token \
                and ''.join(last_related_keyword_token.normalized.split()) in TABLE_RELATED_KEYWORDS \
                and (previous_token and previous_token.normalized not in ['AS', 'WITH']) \
                and token.normalized not in ['AS', 'SELECT', 'IF', 'SET', 'WITH']:
            is_table = True
            if str(previous_token) == ')':
                is_table = False
            elif previous_token and previous_token != last_related_keyword_token \
                    and not previous_token.ttype is Punctuation \
                    and not previous_token.normalized == 'EXISTS':
                is_table = False
            elif token.normalized == 'WITH':
                is_table = False
            elif in_parenthesis and last_related_keyword_token.normalized == 'INTO':
                is_table = False

            if is_table:
                table_name = str(token.value.strip("`"))
                if str(previous_token) == '.' and tables:
                    table_name = f'{tables[-1]}.{table_name}'
                    if table_name not in tables:
                        tables[-1] = table_name 
                elif table_name not in tables:
                    tables.append(table_name)
        if is_keyword and "".join(token.normalized.split()) in KEYWORDS \
                and not (token.normalized == 'FROM' and index - 3 >= 0 and tokens[index - 3].normalized == 'EXTRACT') \
                and not (token.normalized == 'UPDATE' and index - 1 >= 0 and tokens[index - 1].normalized == 'KEY'):
            last_related_keyword_token = token
        previous_token = token
    return tables


def parse_plan(plan, tables=None, columns=None):
    if not tables:
        tables = set()
    if not columns:
        columns = set()
    # 提取表名
    if "Relation Name" in plan:
        tables.add(plan["Relation Name"].lower())

    # 提取列名：检查 Filter, Index Cond, Hash Cond 等字段
    for key in ["Filter", "Index Cond", "Hash Cond"]:
        if key in plan:
            # 使用正则提取列名
            cols = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", plan[key])
            cols = [col.lower() for col in cols]
            columns.update(cols)

    # 递归解析子计划
    if "Plans" in plan:
        for sub_plan in plan["Plans"]:
            parse_plan(sub_plan, tables, columns)

    return tables, columns


def get_potential_columns(sql: str):
    sql = sql.replace('"', '')
    potential_columns = []
    parsed = sqlparse.parse(sql)
    previous_token = None
    for token in parsed[0].flatten():
        if token.ttype is Name:
            if not str(token).lower() in potential_columns:
                if previous_token.normalized != '.':
                    potential_columns.append(str(token).lower())
                elif str(previous_second_token).lower() in potential_columns:
                    potential_columns.append(str(token).lower())
                elif potential_columns:
                    potential_columns[-1] = f'{potential_columns[-1]}.{str(token).lower()}'
        previous_second_token = previous_token
        previous_token = token

    return list(set(potential_columns))


def get_updated_columns(sql: str):

    def contain_schema_name(item):
        if item.is_group and len(item.tokens) >= 3:
            if (item[0].ttype is Name and item[1].ttype is Punctuation
                    and item[1].value == '.' and item[2].ttype is Name):
                return True
        return False

    def extract_item(item):
        if isinstance(item, Identifier):
            if contain_schema_name(item):
                yield item[0].value + '.' + item.get_real_name()
            else:
                yield item.get_real_name()
        if isinstance(item, IdentifierList) or isinstance(item, Comparison) or isinstance(item, Parenthesis):
            for token in item.tokens:
                yield from extract_item(token)

    updated_columns = set()
    updated_table = []
    sql = sqlparse.format(sql, keyword_case="upper")
    if "UPDATE" not in sql or "FOR UPDATE" in sql:
        return updated_table, updated_columns
    sql = sql.replace("UPDATE ONLY", "UPDATE")
    parsed = sqlparse.parse(sql)[0]
    found_set = False
    found_update = False
    for item in parsed.tokens:
        if found_update:
            if item.ttype is Keyword:
                found_update = False
            else:
                updated_table.extend(extract_item(item))
        if found_set:
            if item.ttype is Keyword:
                found_set = False
            else:
                updated_columns.update(extract_item(item))
        if item.ttype is Keyword and item.value == 'SET':
            found_set = True
        if item.ttype is DML and item.value == 'UPDATE':
            found_update = True
    return updated_table[0], updated_columns
