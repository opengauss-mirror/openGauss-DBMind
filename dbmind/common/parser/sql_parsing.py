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
from datetime import datetime
from itertools import count
import logging

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Token
from sqlparse.sql import Where, Comparison, Function, Parenthesis
from sqlparse.tokens import Keyword, DML, Operator, Whitespace, Literal
from sqlparse.tokens import Name

from dbmind.common.utils import dbmind_assert


OPERATOR = ('lt', 'lte', 'gt', 'gte', 'eq', 'neq')


def analyze_column(column, cond_item):
    if isinstance(cond_item, Comparison):
        if isinstance(cond_item.left, Identifier):
            column.add(cond_item.left.value)
        else:
            for item in cond_item.left.tokens:
                if isinstance(item, Identifier) and '"' not in item.value:
                    column.add(item.value)


def get_columns(sql):
    column = set()
    parsed_tree = sqlparse.parse(sql)[0]
    for item in parsed_tree:
        if isinstance(item, Where):
            for cond_item in item:
                analyze_column(column, cond_item)
    return list(column)


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
    """Standardized processing of SQL format"""
    return sqlparse.format(
        sql, keyword_case='upper', identifier_case='lower', strip_comments=True,
        use_space_around_operators=True, strip_whitespace=True
    )


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
    case: select id from table where info = $1 and id_d < $2; PARAMETERS: $1 = 1, $2 = 4;
    result: select id from table where info = '1' and id_d < '4';
    """
    if len(sqlparse.split(query_content)) == 2 and 'parameters: $1' in query_content.lower():
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
    """
    Extract all left-matching LIKE patterns in SQL statements (starting with '%' but not ending with '%')
    Param: sql (str): SQL statement to be analyzed
    Returns:
     list: A list containing all left-matching results (e.g. ["%sxf"])
    """
    results = []

    try:
        parsed = sqlparse.parse(query)

        for statement in parsed:
            # Iterate over all tokens in the SQL statement
            tokens = list(statement.flatten())
            for i, token in enumerate(tokens):
                # Checks whether it is a LIKE comparison operation
                if token.ttype is Operator.Comparison:
                    # Find LIKE or NOT LIKE operations
                    if token.value.upper() == 'LIKE' or token.value.upper() == 'NOT LIKE':
                        # Get the value after LIKE
                        j = i + 1
                        while j < len(tokens):
                            like_token = tokens[j]
                            if like_token.ttype is not Whitespace:
                                if like_token.ttype is Literal.String.Single:
                                    # Get the pattern string and remove the quotes
                                    # Checks for a left-matching pattern (starts with %)
                                    if like_token.value.strip().strip("'").startswith('%'):
                                        results.append("%s %s" % (token.value, like_token.value.strip()))
                                break
                            j += 1
    except Exception as e:
        logging.error('parse error in exists_regular_match function.')

    return results


def exist_track_parameter(query):
    """Determine if SQL contains parameters"""
    return True if '; parameters: $1 = ' in query.lower() else False


def is_query_normalized(query):
    """Determine if SQL is normalized or not"""
    placeholders = []
    for item in sqlparse.parse(query)[0].flatten():
        if item.ttype is Name.Placeholder:
            if not re.match(r"\$\d+|\?", item.value):
                return False
            placeholders.append(item.value)
    if not placeholders:
        return False
    return True


def remove_parameter_part(query):
    """
    remove parameter part when GUC 'track_parameter ' is ON, for example:
      case: SELECT no_o_id FROM bmsql_new_order WHERE no_w_id = $1 AND no_d_id = $2
      ORDER BY no_o_id ASC; parameters: $1 = '10', $2 = '2'
      result: SELECT no_o_id FROM bmsql_new_order WHERE no_w_id = $1 AND no_d_id = $2
      ORDER BY no_o_id ASC;
    """
    return re.sub(r";\s*parameters: \$.+", ";", query, flags=re.IGNORECASE)


def exists_function(query):
    """
    Determine if a function is used in Where clause, for example:
      case1: select * from table where abs(l_quantity) <= 8;
      result: abs(l_quantity)
      case2: select col from table2 where id >
      (select max(id2) from table2 where substring(info from 1 for 2) = 'xxx')
      result: substring(info from 1 for 2)
    """
    flags = []

    def get_function(parsed):
        for item in parsed:
            if item.is_group:
                if isinstance(item, Comparison) and isinstance(item.parent, Where):
                    for sub_item in item.tokens:
                        if isinstance(sub_item, Function):
                            flags.append(sub_item.value)
                        elif isinstance(sub_item, Parenthesis):
                            get_function(sub_item)
                else:
                    get_function(item)

    parsed_tree = sqlparse.parse(query)[0]
    get_function(parsed_tree)
    return flags


def existing_computation(query):
    """
    whether there is a computation in the where clause
    """
    flags = []
    related_columns = set()
    parsed = sqlparse.parse(query)[0]
    for token in parsed.tokens:
        if isinstance(token, Where):
            for where_clause in token.tokens:
                if isinstance(where_clause, Comparison):
                    existing = False
                    if where_clause.left.is_group \
                            and any(isinstance(t, Token) and t.ttype in Operator for t in where_clause.left):
                        existing = True
                        flags.append(where_clause.left.value)
                    if where_clause.right.is_group \
                            and any(isinstance(t, Token) and t.ttype in Operator for t in where_clause.right):
                        existing = True
                        flags.append(where_clause.right.value)
                    if existing:
                        analyze_column(related_columns, where_clause)
    return flags, list(related_columns)


def existing_inequality_compare(query):
    """
    whether there is a computation in the where clause
    """
    inequality_operators = ['<', '>', '<=', '>=', '<>', '!=']
    flags = []
    related_columns = set()
    parsed = sqlparse.parse(query)[0]
    for token in parsed.tokens:
        if isinstance(token, Where):
            for where_token in token.tokens:
                if isinstance(where_token, Comparison):
                    if any(t.value in inequality_operators and t.ttype in Operator for t in where_token):
                        flags.append(where_token.value)
                        analyze_column(related_columns, where_token)
    return flags, list(related_columns)


def regular_match(pattern, string, **kwargs):
    """Provides simple regularization functions."""
    if re.search(pattern, string, **kwargs):
        return True
    return False


def remove_bracket(string):
    """
    Remove bracket in string.
    case: "substring"(c1, 2, 4)"
    result: "substring"
    """
    return re.sub(r"\(.*?\)", '', string)


def exists_bool_clause(query):
    """
    Get boolean expression in SQL, there are two cases:
      case1: select * from table where col in (xx, xx, xx, ...);
      case2: select * from table where col not in (xx, xx, ...);
      result: '(xx, xx, xx, ...)'
    """
    flags = []

    def get_in_clause(parsed):
        for item in parsed:
            if item.is_group:
                if isinstance(item, Parenthesis) and isinstance(item.parent, Where):
                    comparisons = [subitem.value for subitem in item.parent.tokens if
                                   subitem.ttype == sqlparse.tokens.Token.Keyword]
                    if any(comparisons[i - 1] == 'not' for i, x in enumerate(comparisons) if x == 'in') \
                            or any(op in comparisons for op in ('not in',)):
                        for sub_item in item.tokens:
                            if isinstance(sub_item, IdentifierList):
                                flags.append(sub_item.value.split(','))
                            elif sub_item.is_group:
                                get_in_clause(sub_item)
                else:
                    get_in_clause(item)

    parsed_tree = sqlparse.parse(query)[0]
    get_in_clause(parsed_tree)
    return flags


def exists_subquery(query):
    """
    Determine if there is a subquery in SQL, for example:
    case: select id from (select id from table2);
    result: ["select id from table2"]
    """
    flags = []

    def get_subquery(parsed, height):
        for item in parsed:
            if item.is_group:
                get_subquery(item, height + 1)
            elif item.ttype == DML and item.value.upper() == "SELECT":
                if height == 0:
                    continue
                formatted_query = standardize_sql(item.parent.value).strip("()")
                flags.append((formatted_query, height))
    parsed_tree = sqlparse.parse(query)[0]
    get_subquery(parsed_tree, 0)
    return flags


def get_placeholders(query):
    placeholders = set()
    for item in sqlparse.parse(query)[0].flatten():
        if item.ttype is Name.Placeholder:
            placeholders.add(item.value)
    return placeholders


def get_generate_prepare_sqls_function():
    counter = count(start=0, step=1)

    def get_prepare_sqls(statement, is_m_compat=False):
        #  Ensure that the end of sql does not exist ';'
        statement = statement.strip().strip(';')
        prepare_id = 'prepare_' + str(next(counter))
        placeholder_size = len(get_placeholders(statement))
        prepare_args = '' if not placeholder_size else '(%s)' % (','.join(['NULL'] * placeholder_size))
        dbmind_assert(len(sqlparse.split(statement)) == 1)
        if is_m_compat:
            return [f'prepare {prepare_id} from "{statement}"', f'explain execute {prepare_id}{prepare_args}',
                    f'deallocate prepare {prepare_id}']
        return [f'prepare {prepare_id} as {statement}', f'explain execute {prepare_id}{prepare_args}',
                f'deallocate prepare {prepare_id}']

    return get_prepare_sqls


def replace_question_mark_with_value(query):
    """
    PBE does not support the following situations, we can solve it by replacing the '?' with a fixed value.
      1. col >= date ?
      2. interval ? year
      3. fetch first ? row
      4. count(?)
      5. decode(?, xx, xx)
      6. extract(? from o_year) as year
      7. concat(?, col1, col2, ?)
    """
    # replace '?' with '1999-01-01' when meeting 'date ?'
    query = re.sub(r"([\s+|\s*,]date\s+)\?", r"\1'1999-01-01'", query, flags=re.IGNORECASE)
    # replace '?' with '1' when meeting 'interval ?'
    query = re.sub(r"(\s+interval\s+)\?", r"\1'1'", query, flags=re.IGNORECASE)
    # replace '?' with 1 when meeting 'fetch first ?'
    query = re.sub(r"(\s+fetch first\s+)\?", r"\g<1>1", query, flags=re.IGNORECASE)
    # replace '?' with 1 when meeting 'count(?)'
    query = re.sub(r"([\s+|\s*,]count\(\s*)\?(\s*\)\s+)", r"\g<1>1\g<2>", query, flags=re.IGNORECASE)
    # replace '?' with '1' when meeting 'decode(?, xx, xx)'
    query = re.sub(r"([\s+|\s*,]decode\(\s*)\?(\s*,)", r"\1'1'\2", query, flags=re.IGNORECASE)
    # replace '?' with day when meeting 'extract(year from col) as year'
    query = re.sub(r"([\s+|\s*,]extract\(\s*)\?(\s+from)", r"\1'day'\2", query, flags=re.IGNORECASE)
    # replace '?' with day when meeting 'concat(?, col, ?)'
    query = re.sub(r"([\s+|\s*,]concat)(\(.+\))", lambda x: x.group(1) + x.group(2).replace('?', '\'1\''), query)
    return query


def replace_question_mark_with_dollar(query):
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


def exists_count_operation(query):
    try:
        parsed = sqlparse.parse(query)
        for statement in parsed:
            for token in statement.flatten():
                # Examining the COUNT function
                if isinstance(token, Function) and token.get_name().upper() == 'COUNT':
                    return True

                # Checks for COUNT as part of an identifier (e.g. COUNT(*))
                if isinstance(token, Identifier) and 'COUNT' in token.value.upper():
                    return True

                # Check COUNT keyword
                if token.ttype is Keyword and token.value.upper() == 'COUNT':
                    return True

                # Check COUNT as name
                if token.ttype is Name and token.value.upper() == 'COUNT':
                    return True
    except Exception as e:
        logging.error('parse error in exist_count_operation function.')
        return False
