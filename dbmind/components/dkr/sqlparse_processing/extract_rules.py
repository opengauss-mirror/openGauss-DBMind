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

import copy
from collections import defaultdict
from itertools import chain

from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Parenthesis, Operation, \
    Function, Values
from sqlparse.tokens import Keyword, DML, Punctuation, Literal

try:
    from ..dao import get_table_online, get_table_offline
    from ..utils import *
except ValueError:
    from dao import get_table_online, get_table_offline
    from utils import *


class KeyWords(Enum):
    SELECT = 'SELECT'
    UPDATE = 'UPDATE'
    INSERT = 'INSERT'
    DELETE = 'DELETE'

    JOIN = 'JOIN'
    USING = 'USING'
    ON = 'ON'
    FROM = 'FROM'
    AND = 'AND'
    NOT = 'NOT'
    LIKE = 'LIKE'


class WorkloadInfo:
    """Classes for workload information"""

    def __init__(self, distinct_thold_value=0.5):
        self.wl_tables = {}
        self.wl_replication_tables = {}
        self.wl_replication_pairs = []

        self.distinct_thold_value = distinct_thold_value


class ExtractPerSQL:
    """Classes for single sql information"""

    def __init__(self, sql, freq, dn_num, workload_info):
        self.sql = sql
        self.parsed_sql = sqlparse.parse(sql)[0]
        self.freq = freq
        self.dn = dn_num
        # record table real name and alias name.
        self.sql_tables = defaultdict(list)
        self.constraint_pairs = dict(JOIN=[], GROUP_ORDER=[])
        # record where t1.c1... in a transaction.
        self.equal_const_dict = {}

        self.workload_info = workload_info

    # the following functions are responsible for extracting specific clauses
    # and corresponding information from SQL statements,
    # serving the extract_cost_rules() function.
    def extract_cost_rules(self, mode, db_conn, tbl_stat):
        transaction_equal_cond = tuple()
        # extract columns and values from insert for transaction scene.
        if KeyWords.INSERT.value in self.sql:
            self.extract_insert_clause()
        # extract the JOIN/WHERE/group/order
        self.extract_major_rules(self.parsed_sql, mode, db_conn, tbl_stat)

        # distributed transaction only supports single table query.
        if len(self.sql_tables.keys()) == 1:
            transaction_equal_cond = (list(self.sql_tables.keys())[0], self.equal_const_dict)
        return self.constraint_pairs, transaction_equal_cond

    def process_grp_order_cond(self, token, table_column_pair, parsed_sql, is_join):
        """
        Process column and table correspondence of GROUP BY and ORDER BY clauses.
        """
        # handle the 'order by col1 desc' scene.
        token.value = token.value.split(BLANK)[0]
        if DOT in token.value:
            table_name, column = token.value.split(TRANS)[0].split(DOT)
            table_name = [table_name]
            # process table alias.
            if table_name[0] not in self.sql_tables.keys() and \
                    table_name[0] in chain(*(self.sql_tables.values())):
                table_name = [k for k, v in self.sql_tables.items() if table_name[0] in v]
            # JOIN USING.
            if is_join:
                self.record_table_column(table_column_pair, column, table_name,
                                         parsed_sql, column, table_name)
            # GROUP BY/ORDER BY.
            else:
                self.record_table_column(table_column_pair, column, table_name, parsed_sql)
        else:
            if is_join:
                table_left = dict(
                    self.extract_table_identifiers(
                        self.extract_from_clause(parsed_sql))).keys()
                table_right = dict(
                    self.extract_table_identifiers(
                        self.extract_join_clause(parsed_sql))).keys()
                self.record_table_column(table_column_pair, token.value, table_left, parsed_sql,
                                         token.value, table_right)
            else:
                self.record_table_column(table_column_pair, token.value, self.sql_tables.keys(),
                                         parsed_sql)

    def update_table_column(self, token, table_column_pair, parsed_sql, is_join):
        if isinstance(token, Identifier):
            self.process_grp_order_cond(token, table_column_pair, parsed_sql, is_join)
        if isinstance(token, IdentifierList):
            for sub_item in token.get_identifiers():
                self.process_grp_order_cond(sub_item, table_column_pair, parsed_sql, is_join)

    def process_child_equal_cond(self, token):
        column = None
        table = []
        table_column_list = []
        if isinstance(token, Identifier):
            table_column_list.append(token.value.split(DOT))
        elif isinstance(token, Operation):
            for item in token:
                if len(table_column_list) >= 2:
                    break
                if isinstance(item, Identifier):
                    table_column_list.append(item.value.split(DOT))
        else:
            # record constant.
            table_column_list.append(token.value.split(DOT))
        if len(table_column_list) == 1:
            if len(table_column_list[0]) == 1:
                column = table_column_list[0][0]
            if len(table_column_list[0]) == 2:
                table, column = [table_column_list[0][0]], table_column_list[0][1]
        if table:
            if table[0] in self.sql_tables.keys():
                return column, table
            if table[0] in chain(*(self.sql_tables.values())):
                table = [k for k, v in self.sql_tables.items() if table[0] in v]
        else:
            table = self.sql_tables.keys()
        return column, table

    def process_equal_cond(self, table_column_pair, equation, parsed_sql):
        """
        Process the relationship between the table name and the column name
         in the equation after the JOIN ON or WHERE.
        """
        left_column, left_table = self.process_child_equal_cond(equation.left)
        right_column, right_table = self.process_child_equal_cond(equation.right)
        self.record_table_column(table_column_pair, left_column, left_table, parsed_sql,
                                 right_column, right_table)
        # only single table query supports transaction recommendation.
        if len(self.sql_tables) != 1:
            return
        table_name = list(self.sql_tables.keys())[0]
        is_table_valid = \
            self.workload_info.wl_tables.get(table_name) and left_column and left_column.lower() \
            in [column.lower() for column in self.workload_info.wl_tables[table_name].columns]
        if self.equal_const_dict is not None and right_column and is_table_valid:
            if equation.right.ttype in Literal or isinstance(
                    equation.right, Operation):
                self.equal_const_dict[right_column] = self.equal_const_dict.get(right_column, [])
                self.equal_const_dict[right_column].append(left_column.lower())

    def find_table_by_column(self, column, lmt_tbl_name, parsed_sql):
        """
        Match the column name with table name.
        :return tuple-type: (success, result)
        """
        if not column:
            return False, None
        # process the renamed column name.
        sql_flatten = parsed_sql.flatten()
        for item in sql_flatten:
            if column == item.value and isinstance(item.parent.parent, Identifier) \
                    and item.parent.parent.has_alias():
                column = item.parent.parent.get_real_name()
                break

        # matches the column name and table name.
        for table_name in lmt_tbl_name:
            if self.workload_info.wl_tables.get(table_name) and column in \
                    self.workload_info.wl_tables[table_name].columns:
                return True, [table_name, column]
        return False, None

    def record_table_column(self, table_column_pair, left_equal, left_candidates, parsed_sql,
                            right_equal=None, right_candidates=None):
        """
        Record table and column correspondence.
        left_candidates and right_candidates are a list of all possible/candidate table names
        where the corresponding column of the record is located.
        """
        left_success, left_result = self.find_table_by_column(left_equal,
                                                              left_candidates,
                                                              parsed_sql)
        if left_success:
            ind_left = self.workload_info.wl_tables[left_result[0]].columns.index(left_result[1])
            is_left_match_threshold = \
                len(self.workload_info.wl_tables[left_result[0]].cardinality) and \
                (self.workload_info.wl_tables[left_result[0]].cardinality[ind_left] >
                 self.workload_info.distinct_thold_value
                 or self.workload_info.wl_tables[left_result[0]].filter_column_by_freqs(self.dn,
                                                                                        ind_left,
                                                                                        DATA_SKEW_THOLD)
                 )
        if right_equal:
            # for WHERE or JOIN ON scenario.
            right_success, right_result = self.find_table_by_column(right_equal,
                                                                    right_candidates,
                                                                    parsed_sql)
            if left_success and right_success:
                ind_right = self.workload_info.wl_tables[right_result[0]].columns.index(right_result[1])
                is_right_match_threshold = \
                    len(self.workload_info.wl_tables[right_result[0]].cardinality) \
                    and (self.workload_info.wl_tables[right_result[0]].cardinality[ind_right]
                         > self.workload_info.distinct_thold_value
                         or self.workload_info.wl_tables[right_result[0]].filter_column_by_freqs(self.dn,
                                                                                                 ind_right,
                                                                                                 DATA_SKEW_THOLD)
                         )
                if is_left_match_threshold and is_right_match_threshold:
                    table_column_pair.append([left_result, right_result, self.freq])

                # record replication cost rules.
                if left_result[0] in self.workload_info.wl_replication_tables or \
                        right_result[0] in self.workload_info.wl_replication_tables:
                    join_cond = [left_result, right_result, self.freq]
                    self.workload_info.wl_replication_pairs.append(join_cond)
        elif not right_candidates and left_success:
            # for JOIN USING or GROUP BY or ORDER BY scenario.
            if is_left_match_threshold:
                table_column_pair.append([left_result, self.freq])
            if left_result[0] in self.workload_info.wl_replication_tables:
                group_order_cond = (left_result[0] + BLANK + left_result[1], self.freq)
                self.workload_info.wl_replication_pairs.append(group_order_cond)

    def is_subquery(self, parsed, iterate=True):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value == KeyWords.SELECT.value:
                return True
            if iterate and self.is_subquery(item):
                return True
        return False

    @staticmethod
    def extract_group_order_clause(parsed):
        """
        Extract GROUP BY and ORDER BY clauses from parsed AST.
        """
        for item in parsed:
            # recognize type.
            is_punctuation = item.ttype is Punctuation
            is_table_group = isinstance(item,
                                        (Identifier, IdentifierList, Parenthesis))
            if item.ttype is Keyword or (is_punctuation and item.value != COMMA):
                break
            if is_table_group:
                yield item

    def extract_using_name(self, token, parsed_sql, is_join=False):
        """
        Extract the relationship between the table name and the column name
         in the equation after the JOIN USING or GROUP/ORDER BY.
        """
        # JOIN USING/GROUP BY/ORDER BY.
        table_column_pair = []
        for item in token:
            if isinstance(item, Parenthesis):
                for sub_item in item.flatten():
                    is_table_group = isinstance(sub_item.parent,
                                                (Identifier, IdentifierList))
                    if is_table_group:
                        self.update_table_column(sub_item.parent, table_column_pair,
                                                 parsed_sql, is_join)
            else:
                self.update_table_column(item, table_column_pair, parsed_sql, is_join)

        return table_column_pair

    def extract_equal_name(self, token, parsed_sql):
        """
        Extract the relationship between the table name and the column name
         in the equation after the JOIN ON or WHERE.
        """
        table_column_pair = []
        for item in token:
            if isinstance(item.left, Identifier) and isinstance(
                    item.right, Identifier):
                self.process_equal_cond(table_column_pair, item, parsed_sql)
        return table_column_pair

    @staticmethod
    def extract_using_clause(parsed):
        """
        Extract all join conditions of JOIN ON clause.
        """
        for item in parsed:
            if item.ttype is Keyword:
                break
            if isinstance(item, (Identifier, Parenthesis)):
                yield item

    @staticmethod
    def extract_on_clause(parsed):
        """
        Extract all join conditions of JOIN ON.
        """
        for token in parsed:
            # extract all AND join conditions of JOIN ON.
            is_join_sign = token.normalized == KeyWords.AND.value or \
                           token.normalized == KeyWords.NOT.value and \
                           token.normalized == KeyWords.LIKE.value
            if token.ttype is Keyword and not is_join_sign:
                break
            if isinstance(token, Comparison):
                yield token
            if isinstance(token, Parenthesis):
                for item in token:
                    if isinstance(item, Comparison):
                        yield item

    def extract_where_clause(self, parsed, parsed_sql):
        """
        Extract the relationship after WHERE clause.
        """
        table_column_pair = []
        for token in parsed.tokens:
            if isinstance(token, Comparison):
                self.process_equal_cond(table_column_pair, token, parsed_sql)
            if isinstance(token, Parenthesis):
                for item in token:
                    if isinstance(item, Comparison):
                        self.process_equal_cond(table_column_pair, item, parsed_sql)
        return table_column_pair

    def subquery_extract(self, token):
        found_from = False
        sub_tables = self.extract_from_join_table(token)
        for table in sub_tables:
            yield table
        # extract all subqueries that appear before FROM of current subquery.
        for item in token.tokens:
            if item.ttype is Keyword and item.value == KeyWords.FROM.value:
                found_from = True
            if not found_from and self.is_subquery(item):
                yield from self.subquery_extract(item)

    def common_extract(self, token):
        # process the subquery after from.
        if isinstance(token, IdentifierList):
            for item in token.tokens:
                yield from self.common_extract(item)
        elif isinstance(token, Identifier):
            # (select ...) as t.
            if self.is_subquery(token):
                for item in token.tokens:
                    yield from self.common_extract(item)
            else:
                yield token
        elif isinstance(token, Parenthesis):
            if self.is_subquery(token):
                yield from self.subquery_extract(token)
            # when parenthesis is a combination of subquery and table name.
            if not self.is_subquery(token, False):
                for item in token.tokens:
                    yield from self.common_extract(item)

    def extract_join_clause(self, parsed):
        """
        Extract join token from JOIN clause and subquery.
        """
        if KeyWords.JOIN.value not in parsed.value:
            return
        found_join = False
        for item in parsed.tokens:
            if found_join:
                if item.ttype is Keyword:
                    found_join = False
                yield from self.common_extract(item)
            if item.ttype is Keyword and KeyWords.JOIN.value in item.value:
                found_join = True

    def extract_from_clause(self, parsed):
        """
        Extract FROM clause from tokens.
        """
        found_from = False
        for item in parsed.tokens:
            if found_from:
                if item.ttype is Keyword:
                    found_from = False
                yield from self.common_extract(item)
            if item.ttype is Keyword and item.value == KeyWords.FROM.value:
                found_from = True

    def extract_from_join_table(self, parsed):
        from_join_tables = []
        from_join_tables.extend(self.extract_from_clause(parsed))
        from_join_tables.extend(self.extract_join_clause(parsed))
        return from_join_tables

    @staticmethod
    def extract_table_identifiers(tokens):
        """
        Extract table name from tokens.
        """
        for token in tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    if isinstance(identifier, Identifier):
                        yield identifier.get_real_name().strip(
                            QUOTE), identifier.get_name().strip(QUOTE)
            elif isinstance(token, Identifier):
                yield token.get_real_name().strip(QUOTE), token.get_name().strip(QUOTE)

    def append_to_global_table(self, base_table, mode, db_conn, tbl_stat):
        """
        Initialize TableItem object from table information.
        Append the new TableItem object to workload object.
        """
        temp_table = copy.deepcopy(base_table)
        for table_name in temp_table:
            if table_name not in self.workload_info.wl_tables:
                self.workload_info.wl_tables[table_name] = get_table_online(table_name, db_conn) \
                    if mode == 'online' else get_table_offline(table_name, tbl_stat)
                if not self.workload_info.wl_tables[table_name].columns:
                    base_table.pop(table_name)
            # record replication for current sql.
            tuple_count = self.workload_info.wl_tables[table_name].tuple_count
            if tuple_count < TUPLE_COUNT_THOLD:
                self.workload_info.wl_replication_tables[table_name] = \
                    self.workload_info.wl_tables[table_name]

    def extract_iud_clause(self, parsed):

        def extract_insert_table(token):
            if isinstance(token, Function):
                for elem in token.tokens:
                    if isinstance(elem, Identifier):
                        yield elem
            if isinstance(token, Identifier):
                yield token

        def extract_update_delete_table(token):
            if isinstance(token, Identifier):
                yield token

        for item in parsed.tokens:
            if KeyWords.INSERT.value in self.sql:
                yield from extract_insert_table(item)
            if KeyWords.UPDATE.value in self.sql:
                yield from extract_update_delete_table(item)
            if KeyWords.DELETE.value in self.sql:
                yield from extract_update_delete_table(item)

    def extract_base_tables(self, parsed):
        """
        Extract the table name from the 'FROM' and 'JOIN' of each statement.
        """
        table_name = set()
        if any(tp in self.sql for tp, v in REPL_SUPPORT_TYPE.items()):
            iud_clause = self.extract_table_identifiers(self.extract_iud_clause(parsed))
            table_name.update(iud_clause)
        table_name.update(
            self.extract_table_identifiers(self.extract_from_join_table(parsed)))
        base_table = defaultdict(list)
        for table in table_name:
            if table[0].lower().strip() and (not table[1].lower() in base_table[table[0].lower()]):
                base_table[table[0].lower()].append(table[1].lower())
        return base_table

    def extract_major_rules(self, parsed_sql, mode, db_conn, tbl_stat):
        """
        Extract all rules of the current select statement.
        """
        base_table = self.extract_base_tables(parsed_sql)
        self.append_to_global_table(base_table, mode, db_conn, tbl_stat)
        for table_real, table_aliases in base_table.items():
            for table_alias in table_aliases:
                if table_alias not in self.sql_tables[table_real]:
                    self.sql_tables[table_real].append(table_alias)
        subquery_container = []
        for ind, token in enumerate(parsed_sql.tokens):
            if isinstance(token, Where):
                self.constraint_pairs[KeyWords.JOIN.value].append(
                    self.extract_where_clause(token, parsed_sql))
            elif token.ttype is Keyword and token.value == KeyWords.ON.value:
                self.constraint_pairs[KeyWords.JOIN.value].append(
                    self.extract_equal_name(
                        self.extract_on_clause(parsed_sql.tokens[ind + 1:]), parsed_sql))
            elif token.ttype is Keyword and token.value == KeyWords.USING.value:
                self.constraint_pairs[KeyWords.JOIN.value].append(self.extract_using_name(
                    self.extract_using_clause(parsed_sql.tokens[ind + 1:]), parsed_sql, True))
            elif token.value in ('GROUP BY', 'ORDER BY'):
                pairs_group_order = self.extract_using_name(
                    self.extract_group_order_clause(parsed_sql.tokens[ind + 1:]), parsed_sql)
                self.constraint_pairs['GROUP_ORDER'].extend(pairs_group_order)
            if self.is_subquery(token):
                subquery_container.append(token)
        for subquery in subquery_container:
            self.extract_major_rules(subquery, mode, db_conn, tbl_stat)

    def extract_insert_clause(self):
        """
        Extract inserted columns and values for transaction.
        """
        columns = []
        if self.is_subquery(self.parsed_sql):
            return
        for token in self.parsed_sql.tokens:
            if isinstance(token, Function):
                # end when the column name cannot be obtained.
                if not self.workload_info.wl_tables and len(token.tokens) == 1:
                    break
                if len(token.tokens) == 1:
                    columns = self.workload_info.wl_tables[token.tokens[0].value].columns
                    continue
                for item in token.tokens:
                    if isinstance(item, Parenthesis):
                        columns = [item.strip() for item in item.value.strip('()').split(COMMA)]
            if isinstance(token, Values):
                for item in token:
                    if not isinstance(item, Parenthesis):
                        continue
                    # exit when the number of values ​​does not match
                    # the number of extracted columns.
                    if len(item.value.strip('()').split(COMMA)) != len(columns):
                        break
                    # match the correspondence between column names and values.
                    for ind, elem in enumerate(item.value.strip('()').split(COMMA)):
                        self.equal_const_dict[elem.strip()] = \
                            self.equal_const_dict.get(elem.strip(), [])
                        self.equal_const_dict[elem.strip()].append(columns[ind].lower())
