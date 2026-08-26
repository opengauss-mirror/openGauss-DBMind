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
import copy
import heapq
import json
import logging
import os
import re
import sys
import threading
from collections import defaultdict
from functools import lru_cache
from itertools import groupby, permutations
from multiprocessing import Pool
from typing import Tuple, List

import dbmind.components.index_advisor.workload
from dbmind.common.utils.checking import CheckWordValid
from dbmind.common.utils import escape_single_quote, escape_double_quote
from dbmind.components.index_advisor.workload import is_valid_statement, multi_is_valid_statement, compress_workload, \
    MAX_INDEX_COLUMN_NUM, GLOBAL_PROCESS_BAR, MULTI_THREAD_NUM, NUMBER_SET_PATTERN

from .executors.common import BaseExecutor
from .executors.gsql_executor import GsqlExecutor
from .mcts import MCTS
from .parser import get_query_tables, get_potential_columns, get_updated_columns, parse_plan
from .process_bar import bar_print
from .sql_output_parser import (
    get_checked_indexes,
    is_lp_plan,
    parse_existing_indexes_results,
    parse_explain_plan,
    parse_hypo_index,
    parse_plan_cost,
    parse_single_advisor_results,
    parse_table_sql_results
)
from .sql_generator import (
    get_existing_index_sql,
    get_hypo_index_head_sqls,
    get_index_check_sqls,
    get_index_setting_sqls,
    get_lp_check_sqls,
    get_prepare_sqls,
    get_single_advisor_sql,
    session_get_index_setting_sqls,
    session_get_index_storage_sqls
)
from .table import get_table_context
from .utils import (
    has_dollar_placeholder,
    hypo_index_ctx,
    is_m_compat,
    is_multi_node,
    split_iter,
    COLUMN_DELIMITER,
    is_valid_string, path_type, set_logger, get_password, powerset, print_header_boundary
)
from dbmind.components.index_advisor.models import QueryType, IndexType, UniqueList, ExistingIndex, AdvisedIndex, \
    IndexItemFactory, QueryItem, WorkLoad, infer_workload_benefit, match_columns, generate_placeholder_indexes, \
    generate_placeholder_gsis

MAX_CANDIDATE_COLUMNS = 40
MAX_INDEX_NUM = None
MAX_INDEX_STORAGE = None
FULL_ARRANGEMENT_THRESHOLD = 20
NEGATIVE_RATIO_THRESHOLD = 0.2
MAX_BENEFIT_THRESHOLD = float('inf')
SQL_DISPLAY_PATTERN = [r'\'((\')|(.*?\'))',  # match all content in single quotes
                       NUMBER_SET_PATTERN,  # match integer set in the IN collection
                       r'([^\_\d])\d+(\.\d+)?']  # match single integer
RELATED_TABLE_SET = set()
query_tables = dict()


os.umask(0o0077)


def get_positive_sql_count(candidate_indexes: List[AdvisedIndex], workload: WorkLoad):
    positive_sql_count = 0
    for query in workload.get_queries():
        for index in candidate_indexes:
            if workload.is_positive_query(index, query):
                positive_sql_count += query.get_frequency()
                break
    return int(positive_sql_count)


def print_statement(index_list: List[Tuple[str]], schema_table: str):
    for index in index_list:
        columns, containing, index_type, statement = '', '', '', ''
        if len(index) == 2:
            columns, index_type = index
        elif len(index) == 3:
            columns, containing, index_type = index
        else:
            continue
        if containing and index_type != 'gsi':
            continue
        table = re.split(r'\.', schema_table, 1)[-1]
        table = table if not table.startswith('"') else table[1:-1]
        index_name = 'idx_%s_%s%s' % (table,
                                      (index_type + '_' if index_type else ''),
                                      '_'.join(columns.split(COLUMN_DELIMITER)))
        if not is_valid_string(index_name):
            index_name = f'"{escape_double_quote(index_name)}"' 
        if index_type != 'gsi':
            statement = 'CREATE INDEX %s ON %s%s%s;' % (index_name, schema_table,
                                                        '(' + columns + ')',
                                                        (' ' + index_type if index_type else ''))
        else:
            statement = 'CREATE GLOBAL INDEX %s ON %s%s' % (index_name, schema_table, '(' + columns + ')')
            if containing:
                statement += ' CONTAINING (' + containing + ');'
            else:
                statement += ';'
        bar_print(statement)


class IndexAdvisor:
    def __init__(self, executor: BaseExecutor, workload: WorkLoad, multi_iter_mode: bool):
        self.executor = executor
        self.workload = workload
        self.multi_iter_mode = multi_iter_mode

        self.determine_indexes = []
        self.determine_indexes_fqs = []
        self.integrate_indexes = {}

        self.display_detail_info = {}
        self.index_benefits = []
        self.redundant_indexes = []

    def complex_index_advisor(self, candidate_indexes: List[AdvisedIndex]):
        atomic_config_total = generate_sorted_atomic_config(self.workload.get_queries(), candidate_indexes)
        same_columns_config = generate_atomic_config_containing_same_columns(candidate_indexes)
        for atomic_config in same_columns_config:
            if atomic_config not in atomic_config_total:
                atomic_config_total.append(atomic_config)
        if atomic_config_total and len(atomic_config_total[0]) != 0:
            raise ValueError("The empty atomic config isn't generated!")
        if MULTI_THREAD_NUM:
            thread_list = []
            self.executor.execute_sqls(["select * from hypopg_reset_index();"])
            GLOBAL_PROCESS_BAR.reset_bar(len(atomic_config_total), 'Optimal indexes')
            for number in range(MULTI_THREAD_NUM):
                thread_list.append(threading.Thread(target=multi_estimate_workload_cost_file, args=(
                                                    self.executor, self.workload, atomic_config_total, number)))
            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()
            # session-level hypoindex will be clear after session closed,
            # it's unlikely to estimate size of session-level hypoindex,
            # so we estimate workload cost and estimate size of hypoindex separately.
            with hypo_index_ctx(self.executor):
                for indexes in self.workload._WorkLoad__indexes_list:
                    index_setting_sqls = get_index_setting_sqls(indexes, is_multi_node(self.executor),
                                                                is_m_compat(self.executor))
                    hypo_index_ids = parse_hypo_index(self.executor.execute_sqls(index_setting_sqls))
                    update_index_storage(indexes, hypo_index_ids, self.executor)
        else:
            for atomic_config in GLOBAL_PROCESS_BAR.process_bar(atomic_config_total, 'Optimal indexes'):
                estimate_workload_cost_file(self.executor, self.workload, atomic_config)
        self.workload.set_index_benefit()
        if MAX_INDEX_STORAGE:
            opt_config = MCTS(self.workload, atomic_config_total, candidate_indexes,
                              MAX_INDEX_STORAGE, MAX_INDEX_NUM)
        else:
            opt_config = greedy_determine_opt_config(self.workload, atomic_config_total,
                                                     candidate_indexes)
        self.filter_redundant_indexes_with_diff_types(opt_config)
        self.filter_same_columns_indexes(opt_config, self.workload)
        self.display_detail_info['positive_stmt_count'] = get_positive_sql_count(opt_config,
                                                                                 self.workload)
        if len(opt_config) == 0:
            bar_print("No optimal indexes generated!")
            return None
        return opt_config

    @staticmethod
    def filter_same_columns_indexes(opt_config, workload, rate=0.8):
        """If the columns in two indexes have a containment relationship,
        for example, index1 is table1(col1, col2), index2 is table1(col3, col1, col2),
        then when the gain of one index is close to the gain of both indexes as a whole,
        the addition of the other index obviously does not improve the gain much,
        and we filter it out."""
        same_columns_config = generate_atomic_config_containing_same_columns(opt_config)
        origin_cost = workload.get_total_origin_cost()
        filtered_indexes = UniqueList()
        for short_index, long_index in same_columns_config:
            if workload.has_indexes((short_index, long_index)):
                combined_benefit = workload.get_total_index_cost((short_index, long_index)) - origin_cost
            elif workload.has_indexes((long_index, short_index)):
                combined_benefit = workload.get_total_index_cost((long_index, short_index)) - origin_cost
            else:
                continue
            short_index_benefit = workload.get_total_index_cost((short_index,)) - origin_cost
            long_index_benefit = workload.get_total_index_cost((long_index,)) - origin_cost
            if combined_benefit and short_index_benefit / combined_benefit > rate:
                filtered_indexes.append(long_index)
                continue
            if combined_benefit and long_index_benefit / combined_benefit > rate:
                filtered_indexes.append(short_index)
        for filtered_index in filtered_indexes:
            opt_config.remove(filtered_index)
            logging.info('filtered: %s is removed due to similar benefits with other same column indexes.',
                         filtered_index)

    def simple_index_advisor(self, candidate_indexes: List[AdvisedIndex]):
        estimate_workload_cost_file(self.executor, self.workload)
        if MULTI_THREAD_NUM:
            thread_list = []
            GLOBAL_PROCESS_BAR.reset_bar(len(candidate_indexes), 'Optimal indexes')
            self.executor.execute_sqls(["select * from hypopg_reset_index();"])
            for number in range(MULTI_THREAD_NUM):
                thread_list.append(threading.Thread(target=multi_estimate_workload_cost_file, args=(
                                                    self.executor, self.workload, candidate_indexes, number)))
            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()
            # session-level hypoindex will be clear after session closed,
            # it's unlikely to estimate size of session-level hypoindex,
            # so we estimate workload cost and estimate size of hypoindex separately.
            with hypo_index_ctx(self.executor):
                for indexes in self.workload._WorkLoad__indexes_list:
                    index_setting_sqls = get_index_setting_sqls(indexes, is_multi_node(self.executor),
                                                                is_m_compat(self.executor))
                    hypo_index_ids = parse_hypo_index(self.executor.execute_sqls(index_setting_sqls))
                    update_index_storage(indexes, hypo_index_ids, self.executor)
        else:
            for index in GLOBAL_PROCESS_BAR.process_bar(candidate_indexes, 'Optimal indexes'):
                estimate_workload_cost_file(self.executor, self.workload, (index,))
        self.workload.set_index_benefit()
        self.filter_redundant_indexes_with_diff_types(candidate_indexes)
        if not candidate_indexes:
            bar_print("No optimal indexes generated!")
            return None

        self.display_detail_info['positive_stmt_count'] = get_positive_sql_count(candidate_indexes,
                                                                                 self.workload)
        return candidate_indexes

    def filter_low_benefit_index(self, opt_indexes: List[AdvisedIndex], improved_rate):
        index_current_storage = 0
        cnt = 0
        for key, index in enumerate(opt_indexes):
            sql_optimized = 0
            negative_sql_ratio = 0
            insert_queries, delete_queries, \
            update_queries, select_queries, \
            positive_queries, ineffective_queries, \
            negative_queries = self.workload.get_index_related_queries(index, query_tables)
            sql_num = self.workload.get_index_sql_num(index, query_tables)
            total_benefit = 0
            # Calculate the average benefit of each positive SQL.
            for query in positive_queries:
                current_cost = self.workload.get_indexes_cost_of_query(query, (index,))
                origin_cost = self.workload.get_origin_cost_of_query(query)
                sql_optimized += (1 - current_cost / origin_cost) * query.get_frequency()
                benefit = origin_cost - current_cost
                total_benefit += benefit
            total_queries_num = sql_num['negative'] + sql_num['ineffective'] + sql_num['positive']
            if total_queries_num:
                negative_sql_ratio = sql_num['negative'] / total_queries_num
            # Filter the candidate indexes that do not meet the conditions of optimization.
            logging.info('filter low benefit index for %s.', index)
            if not positive_queries:
                logging.info('filtered: positive_queries not found for the index')
                continue
            if sql_optimized / sql_num['positive'] < improved_rate and total_benefit < MAX_BENEFIT_THRESHOLD:
                logging.info("filtered: improved_rate %s less than %s.",
                             sql_optimized / sql_num['positive'], improved_rate)
                continue
            if sql_optimized / sql_num['positive'] < \
                    NEGATIVE_RATIO_THRESHOLD < negative_sql_ratio:
                logging.info('filtered: improved_rate %s < negative_ratio_threshold < negative_sql_ratio, '
                             '%s is not met.', sql_optimized / sql_num["positive"], negative_sql_ratio)
                continue
            logging.info('%s has benefit of %s', index, self.workload.get_index_benefit(index))
            if MAX_INDEX_STORAGE and (index_current_storage + index.get_storage()) > MAX_INDEX_STORAGE:
                logging.info('filtered: if add the index %s, it reaches the max index storage.', index)
                continue
            if MAX_INDEX_NUM and cnt == MAX_INDEX_NUM:
                logging.info('filtered: reach the maximum number for the index.')
                break
            if not self.multi_iter_mode and index.benefit <= 0:
                logging.info('filtered: benefit not above 0 for the index.')
                continue
            index_current_storage += index.get_storage()
            cnt += 1
            self.determine_indexes.append(index)

    def print_benefits(self, created_indexes: List[ExistingIndex]):
        print_header_boundary('Index benefits')
        table_indexes = defaultdict(UniqueList)
        for index in created_indexes:
            table_indexes[index.get_schema_table()].append(index)
        total_origin_cost = self.workload.get_total_origin_cost()
        for i, index in enumerate(self.determine_indexes):
            useless_indexes = []
            existing_indexes = []
            improved_queries = []
            indexdef = index.get_index_statement()
            bar_print(f'INDEX {i}: {indexdef}')
            workload_benefit = sum([query.get_benefit() for query in index.get_positive_queries()])
            workload_improved_rate = workload_benefit / total_origin_cost
            bar_print('\tCost benefit for workload: %.2f' % workload_benefit)
            bar_print('\tCost improved rate for workload: %.2f%%'
                      % (workload_improved_rate * 100))

            # invalid indexes caused by recommended indexes
            source_index = index.get_source_index()
            if source_index and (not source_index.is_primary_key()) and (not source_index.get_is_unique()):
                bar_print('\tCurrently existing useless indexes:')
                bar_print(f'\t\t{source_index.get_indexdef()}')
                useless_indexes.append(source_index.get_indexdef())

            # information about existing indexes
            created_indexes = table_indexes.get(index.get_table(), [])
            if created_indexes:
                bar_print('\tExisting indexes of this relation:')
                for created_index in created_indexes:
                    bar_print(f'\t\t{created_index.get_indexdef()}')
                    existing_indexes.append(created_index.get_indexdef())

            bar_print('\tImproved query:')
            # get benefit rate for subsequent sorting and display
            query_benefit_rate = []
            for query in sorted(index.get_positive_queries(), key=lambda query: -query.get_benefit()):
                query_origin_cost = self.workload.get_origin_cost_of_query(query)
                current_cost = self.workload.get_indexes_cost_of_query(query, tuple([index]))
                query_improved_rate = (query_origin_cost - current_cost) / current_cost
                query_benefit_rate.append((query, query_improved_rate))
            # sort query by benefit rate
            for j, (query, query_improved_rate) in enumerate(sorted(query_benefit_rate, key=lambda x: -x[1])):
                other_related_indexes = []
                bar_print(f'\t\tQuery {j}: {query.get_statement()}')
                query_origin_cost = self.workload.get_origin_cost_of_query(query)
                current_cost = self.workload.get_indexes_cost_of_query(query, tuple([index]))
                query_benefit = query_origin_cost - current_cost
                origin_plan = self.workload.get_indexes_plan_of_query(query, None)
                current_plan = self.workload.get_indexes_plan_of_query(query, tuple([index]))
                bar_print('\t\t\tCost benefit for the query: %.2f' % query_benefit)
                bar_print('\t\t\tCost improved rate for the query: %.2f%%' % (query_improved_rate * 100))
                bar_print(f'\t\t\tQuery number: {int(query.get_frequency())}')
                if len(query.get_indexes()) > 1:
                    bar_print('\t\t\tOther optimal indexes:')
                    for temp_index in query.get_indexes():
                        if temp_index is index:
                            continue
                        bar_print(f'\t\t\t\t{temp_index.get_index_statement()}')
                        other_related_indexes.append(temp_index.get_index_statement())
                improved_queries.append({'query': query.get_statement(),
                                         'query_benefit': query_benefit,
                                         'query_improved_rate': query_improved_rate,
                                         'query_count': int(query.get_frequency()),
                                         'origin_plan': origin_plan,
                                         'current_plan': current_plan,
                                         'other_related_indexes': other_related_indexes
                                         })
            self.index_benefits.append({'indexdef': indexdef,
                                        'workload_benefit': workload_benefit,
                                        'workload_improved_rate': workload_improved_rate,
                                        'useless_indexes': useless_indexes,
                                        'existing_indexes': existing_indexes,
                                        'improved_queriies': improved_queries,
                                        })

    def record_info(self, index: AdvisedIndex, sql_info, table_name: str, statement: str):
        sql_num = self.workload.get_index_sql_num(index, query_tables)
        total_sql_num = int(sql_num['positive'] + sql_num['ineffective'] + sql_num['negative'])
        workload_optimized = index.benefit / self.workload.get_total_origin_cost() * 100
        sql_info['workloadOptimized'] = '%.2f' % \
                                        (workload_optimized if workload_optimized > 1 else 1)
        sql_info['schemaName'] = index.get_table().split('.')[0]
        sql_info['tbName'] = table_name
        sql_info['columns'] = index.get_columns()
        sql_info['index_type'] = index.get_index_type()
        sql_info['statement'] = statement
        sql_info['storage'] = index.get_storage()
        sql_info['dmlCount'] = total_sql_num
        sql_info['selectRatio'] = 1
        sql_info['insertRatio'] = sql_info['deleteRatio'] = sql_info['updateRatio'] = 0
        if total_sql_num:
            sql_info['selectRatio'] = round(
                (sql_num['select']) * 100 / total_sql_num, 2)
            sql_info['insertRatio'] = round(
                sql_num['insert'] * 100 / total_sql_num, 2)
            sql_info['deleteRatio'] = round(
                sql_num['delete'] * 100 / total_sql_num, 2)
            sql_info['updateRatio'] = round(
                100 - sql_info['selectRatio'] - sql_info['insertRatio'] - sql_info['deleteRatio'], 2)
        sql_info['associationIndex'] = index.association_indexes
        self.display_detail_info['recommendIndexes'].append(sql_info)

    def compute_index_optimization_info(self, index: AdvisedIndex, table_name: str, statement: str):
        sql_info = {'sqlDetails': []}
        insert_queries, delete_queries, update_queries, select_queries, \
        positive_queries, ineffective_queries, negative_queries = \
            self.workload.get_index_related_queries(index, query_tables)

        for category, queries in zip([QueryType.INEFFECTIVE, QueryType.POSITIVE, QueryType.NEGATIVE],
                                     [ineffective_queries, positive_queries, negative_queries]):
            sql_count = int(sum(query.get_frequency() for query in queries))
            # Record 5 ineffective or negative queries.
            if category in [QueryType.INEFFECTIVE, QueryType.NEGATIVE]:
                queries = queries[:5]
            for query in queries:
                sql_detail = {}
                sql_template = query.get_statement()
                for pattern in SQL_DISPLAY_PATTERN:
                    sql_template = re.sub(pattern, '?', sql_template)

                sql_detail['sqlTemplate'] = sql_template
                sql_detail['sql'] = query.get_statement()
                sql_detail['sqlCount'] = int(round(sql_count))

                if category == QueryType.POSITIVE:
                    origin_cost = self.workload.get_origin_cost_of_query(query)
                    current_cost = self.workload.get_indexes_cost_of_query(query, tuple([index]))
                    sql_optimized = (origin_cost - current_cost) / current_cost * 100
                    sql_detail['optimized'] = '%.1f' % sql_optimized
                sql_detail['correlationType'] = category.value
                sql_info['sqlDetails'].append(sql_detail)
        self.record_info(index, sql_info, table_name, statement)

    def display_advise_indexes_info(self, show_detail: bool):
        self.display_detail_info['workloadCount'] = int(
            sum(query.get_frequency() for query in self.workload.get_queries()))
        self.display_detail_info['recommendIndexes'] = []
        logging.info('filter advised indexes by using max-index-storage and max-index-num.')
        for key, index in enumerate(self.determine_indexes):
            # display determine indexes
            table_name = index.get_table().split('.')[-1]
            statement = index.get_index_statement()
            bar_print(statement)
            if show_detail:
                # Record detailed SQL optimization information for each index.
                self.compute_index_optimization_info(
                    index, table_name, statement)
        if self.determine_indexes_fqs:
            print_header_boundary(" Determine optimal indexes(FQS GSI) ")
        for index_fqs in self.determine_indexes_fqs:
            if index_fqs not in self.determine_indexes:
                statement = index_fqs.get_index_statement()
                bar_print(statement)

    def generate_incremental_index(self, history_advise_indexes):
        self.integrate_indexes = copy.copy(history_advise_indexes)
        self.integrate_indexes['currentIndexes'] = {}
        for key, index in enumerate(self.determine_indexes + self.determine_indexes_fqs):
            self.integrate_indexes['currentIndexes'][index.get_quoted_table()] = \
                self.integrate_indexes['currentIndexes'].get(index.get_quoted_table(), [])
            if index.get_index_type() != 'gsi':
                self.integrate_indexes['currentIndexes'][index.get_quoted_table()].append(
                    (index.get_columns(), index.get_index_type()))
            else:
                self.integrate_indexes['currentIndexes'][index.get_quoted_table()].append(
                    (index.get_columns(), index.get_containing(), index.get_index_type()))

    def generate_redundant_useless_indexes(self, history_invalid_indexes):
        created_indexes = fetch_created_indexes(self.executor)
        record_history_invalid_indexes(self.integrate_indexes['historyIndexes'], history_invalid_indexes,
                                       created_indexes)
        print_header_boundary(" Created indexes ")
        self.display_detail_info['createdIndexes'] = []
        if not created_indexes:
            bar_print("No created indexes!")
        else:
            self.record_created_indexes(created_indexes)
            for index in created_indexes:
                bar_print("%s: %s;" % (index.get_schema(), index.get_indexdef()))
        workload_indexnames = self.workload.get_used_index_names()
        display_useless_redundant_indexes(created_indexes, workload_indexnames,
                                          self.display_detail_info)
        unused_indexes = [index for index in created_indexes if index.get_indexname() not in workload_indexnames]
        self.redundant_indexes = get_redundant_created_indexes(created_indexes, unused_indexes)

    def record_created_indexes(self, created_indexes):
        for index in created_indexes:
            index_info = {'schemaName': index.get_schema(), 'tbName': index.get_table(),
                          'columns': index.get_columns(), 'statement': index.get_indexdef() + ';'}
            self.display_detail_info['createdIndexes'].append(index_info)

    def display_incremental_index(self, history_invalid_indexes,
                                  workload_file_path):

        # Display historical effective indexes.
        if self.integrate_indexes['historyIndexes']:
            print_header_boundary(" Historical effective indexes ")
            for table_name, index_list in self.integrate_indexes['historyIndexes'].items():
                print_statement(index_list, table_name)
        # Display historical invalid indexes.
        if history_invalid_indexes:
            print_header_boundary(" Historical invalid indexes ")
            for table_name, index_list in history_invalid_indexes.items():
                print_statement(index_list, table_name)
        # Save integrate indexes result.
        if not isinstance(workload_file_path, dict):
            integrate_indexes_file = os.path.join(os.path.realpath(os.path.dirname(workload_file_path)),
                                                  'index_result.json')
            for table, indexes in self.integrate_indexes['currentIndexes'].items():
                self.integrate_indexes['historyIndexes'][table] = \
                    self.integrate_indexes['historyIndexes'].get(table, [])
                self.integrate_indexes['historyIndexes'][table].extend(indexes)
                self.integrate_indexes['historyIndexes'][table] = \
                    list(
                        set(map(tuple, (self.integrate_indexes['historyIndexes'][table]))))
            with open(integrate_indexes_file, 'w') as file:
                json.dump(self.integrate_indexes['historyIndexes'], file)

    @staticmethod
    def filter_redundant_indexes_with_diff_types(candidate_indexes: List[AdvisedIndex]):
        sorted_indexes = sorted(candidate_indexes, key=lambda x: (x.get_table(), x.get_columns()))
        for table, _index_group in groupby(sorted_indexes, key=lambda x: x.get_table()):
            index_group = list(_index_group)
            for i in range(len(index_group) - 1):
                cur_index = index_group[i]
                next_index = index_group[i + 1]
                if match_columns(cur_index, next_index):
                    if cur_index.benefit == next_index.benefit:
                        if cur_index.get_index_type() == 'global':
                            candidate_indexes.remove(next_index)
                            index_group[i + 1] = index_group[i]
                        else:
                            candidate_indexes.remove(cur_index)
                    else:
                        if cur_index.benefit < next_index.benefit:
                            candidate_indexes.remove(cur_index)
                        else:
                            candidate_indexes.remove(next_index)
                            index_group[i + 1] = index_group[i]


def generate_single_column_indexes(advised_indexes: List[AdvisedIndex]):
    """ Generate single column indexes. """
    single_column_indexes = []
    if len(advised_indexes) == 0:
        return single_column_indexes

    for index in advised_indexes:
        table = index.get_table()
        columns = index.get_columns()
        index_type = index.get_index_type()
        if index_type == 'gsi':
            continue
        for column in columns.split(COLUMN_DELIMITER):
            single_column_index = IndexItemFactory().get_index(table, column, index_type)
            if single_column_index not in single_column_indexes:
                single_column_indexes.append(single_column_index)
    return single_column_indexes


def add_more_column_index(indexes, table, columns_info, single_col_info, containing='', dist_cols=''):
    columns, columns_index_type = columns_info
    single_column, single_index_type = single_col_info
    if columns_index_type != 'gsi' and single_index_type != 'gsi':
        if columns_index_type.strip('"') != single_index_type.strip('"'):
            add_more_column_index(indexes, table, (columns, 'local'), (single_column, 'local'))
            add_more_column_index(indexes, table, (columns, 'global'), (single_column, 'global'))
        else:
            current_columns_index = IndexItemFactory().get_index(table, columns + COLUMN_DELIMITER + single_column,
                                                                 columns_index_type)
            if current_columns_index in indexes:
                return
            # To make sure global is behind local
            if single_index_type == 'local':
                global_columns_index = IndexItemFactory().get_index(table, columns + COLUMN_DELIMITER + single_column,
                                                                    'global')
                if global_columns_index in indexes:
                    global_pos = indexes.index(global_columns_index)
                    indexes[global_pos] = current_columns_index
                    current_columns_index = global_columns_index
            indexes.append(current_columns_index)
    elif columns_index_type == 'gsi' and single_index_type == 'gsi' and containing and dist_cols:
        current_containing_list = containing.split(COLUMN_DELIMITER)
        current_columns_list = columns.split(COLUMN_DELIMITER)
        dist_cols_list = dist_cols.split(COLUMN_DELIMITER)
        if single_column in containing and single_column not in current_columns_list:
            current_containing_list.remove(single_column)
            current_columns_list.append(single_column)
            if (len(current_columns_list) < len(dist_cols_list) and
                    current_columns_list == dist_cols_list[:len(current_columns_list)]):
                return
            current_columns_gsi = IndexItemFactory().get_index(table, COLUMN_DELIMITER.join(current_columns_list),
                                                               columns_index_type,
                                                               COLUMN_DELIMITER.join(current_containing_list))
            if current_columns_gsi in indexes:
                return
            indexes.append(current_columns_gsi)
    else:
        return


def query_index_advise(executor, query):
    """ Call the single-indexes-advisor in the database. """

    sql = get_single_advisor_sql(query)
    results = executor.execute_sqls([sql])
    advised_indexes = parse_single_advisor_results(results)

    return advised_indexes


def get_index_storage(executor, hypo_index_id):
    sqls = get_hypo_index_head_sqls(is_multi_node(executor))
    index_size_sqls = sqls + ['select * from pg_catalog.hypopg_estimate_size(%s);' % hypo_index_id]
    results = executor.execute_sqls(index_size_sqls)
    for cur_tuple in results:
        if re.match(r'\d+', str(cur_tuple[0]).strip()):
            return float(str(cur_tuple[0]).strip()) / 1024 / 1024


def update_index_storage(indexes, hypo_index_ids, executor):
    if indexes:
        for index, hypo_index_id in zip(indexes, hypo_index_ids):
            storage = index.get_storage()
            if not storage:
                storage = get_index_storage(executor, hypo_index_id)
            index.set_storage(storage)
            index.add_hypo_index_id(hypo_index_id)


def session_get_plan_cost(statements, executor, indexes):
    plan_sqls = []
    plan_sqls.extend(session_get_index_setting_sqls(indexes, is_multi_node(executor), is_m_compat(executor)))
    for statement in statements:
        plan_sqls.extend(get_prepare_sqls(statement, verbose=False, is_m_compat=is_m_compat(executor)))
    with executor.session():
        results = executor.execute_sqls(plan_sqls)
    cost, index_names_list, plans = parse_explain_plan(results, len(statements))
    return cost, index_names_list, plans


def get_plan_cost(statements, executor):
    plan_sqls = []
    plan_sqls.extend(get_hypo_index_head_sqls(is_multi_node(executor)))
    for statement in statements:
        plan_sqls.extend(get_prepare_sqls(statement, verbose=False, is_m_compat=is_m_compat(executor)))
    results = executor.execute_sqls(plan_sqls)
    cost, index_names_list, plans = parse_explain_plan(results, len(statements))
    return cost, index_names_list, plans


def session_get_workload_costs(statements, executor, indexes, threads=20):
    costs = []
    index_names_list = []
    plans = []
    results = [session_get_plan_cost(statements, executor, indexes)]
    for _costs, _index_names_list, _plans in results:
        costs.extend(_costs)
        index_names_list.extend(_index_names_list)
        plans.extend(_plans)
    return costs, index_names_list, plans


def get_workload_costs(statements, executor, threads=20):
    costs = []
    index_names_list = []
    plans = []
    statements_blocks = split_iter(statements, threads)
    try:
        with Pool(threads) as p:
            results = p.starmap(get_plan_cost, [[_statements, executor] for _statements in statements_blocks])
    except TypeError:
        results = [get_plan_cost(statements, executor)]
    for _costs, _index_names_list, _plans in results:
        costs.extend(_costs)
        index_names_list.extend(_index_names_list)
        plans.extend(_plans)
    return costs, index_names_list, plans


def estimate_workload_cost_file(executor, workload, indexes=None):
    select_queries = []
    select_queries_pos = []
    query_costs = [0] * len(workload.get_queries())
    for i, query in enumerate(workload.get_queries()):
        select_queries.append(query.get_statement())
        select_queries_pos.append(i)
    session_index_storage_sqls = session_get_index_storage_sqls(indexes, is_multi_node(executor), is_m_compat(executor))
    get_workload_costs_sqls = []
    for query in workload.get_queries():
        get_workload_costs_sqls.extend(get_prepare_sqls(query.get_statement(), verbose=False,
                                                        is_m_compat=is_m_compat(executor)))
    results = executor.execute_sqls(session_index_storage_sqls + get_hypo_index_head_sqls(is_multi_node(executor)) +
                                    get_workload_costs_sqls +
                                    ['''select pg_catalog.hypopg_reset_index();'''])
    update_index_storage_using_results(indexes, results)
    costs, index_names, plans = parse_explain_plan([res for res in results if not len(res) == 2],
                                                   len(workload.get_queries()))
    # Update query cost for select queries and positive_pos for indexes.
    for cost, query_pos in zip(costs, select_queries_pos):
        query_costs[query_pos] = cost * workload.get_queries()[query_pos].get_frequency()
    workload.add_indexes(indexes, query_costs, index_names, plans)


def update_index_storage_using_results(indexes, results):
    results = [res for res in results if len(res) == 2]
    if indexes:
        for _tuple, index in zip(results, indexes):
            hypo_index_id, storage = _tuple
            index.set_storage(int(storage)/1024/1024)
            index.add_hypo_index_id(str(hypo_index_id))


def multi_estimate_workload_cost_file(executor, workload, atomic_config_total, number):
    for i in range(number, len(atomic_config_total), MULTI_THREAD_NUM):
        indexes = atomic_config_total[i]
        # used for simple_index_advisor()
        if not isinstance(indexes, tuple):
            indexes = (indexes,)
        select_queries = []
        select_queries_pos = []
        query_costs = [0] * len(workload.get_queries())
        for j, query in enumerate(workload.get_queries()):
            select_queries.append(query.get_statement())
            select_queries_pos.append(j)
        costs, index_names, plans = session_get_workload_costs([query.get_statement() for query in
                                                                workload.get_queries()], executor, indexes)
        # Update query cost for select queries and positive_pos for indexes.
        for cost, query_pos in zip(costs, select_queries_pos):
            query_costs[query_pos] = cost * workload.get_queries()[query_pos].get_frequency()
        workload.add_indexes(indexes, query_costs, index_names, plans)
        GLOBAL_PROCESS_BAR.next_bar()


def query_index_check(executor, query, indexes, sort_by_column_no=True, is_fqs_on=False):
    """ Obtain valid indexes based on the optimizer. """
    valid_indexes = []
    if len(indexes) == 0:
        return valid_indexes, None
    if sort_by_column_no:
        # When the cost values are the same, the execution plan picks the last index created.
        # Sort indexes to ensure that short indexes have higher priority and the gsi have lower priority.
        indexes = sorted(indexes, key=lambda index: (index.get_index_type() != 'gsi', -len(index.get_columns())))
    indexable_indexes = list(get_indexable_indexes(indexes, executor, is_fqs_on=is_fqs_on))
    index_check_results = executor.execute_sqls(get_index_check_sqls(query, indexable_indexes, is_multi_node(executor),
                                                                     is_fqs_on=is_fqs_on,
                                                                     is_m_compat=is_m_compat(executor)))
    valid_indexes = get_checked_indexes(indexable_indexes, index_check_results)
    cost = None
    for res in index_check_results:
        if '(cost' in res[0]:
            cost = parse_plan_cost(res[0])
            if cost == 0:
                cost = None
            break
    return valid_indexes, cost


def get_indexable_indexes(indexes, executor, is_fqs_on=False):
    for index in indexes:
        index_check_results = executor.execute_sqls(get_index_check_sqls('select 1', [index], is_multi_node(executor),
                                                                         is_fqs_on=is_fqs_on,
                                                                         is_m_compat=is_m_compat(executor)))
        for cur_tuple in index_check_results:
            text = cur_tuple[0]
            if text.strip('("').startswith('<') and 'btree' in text:
                yield index


def query_support_lp(executor, query):
    results = executor.execute_sqls(get_lp_check_sqls(query, is_multi_node(executor)))
    return is_lp_plan(results)


def remove_unused_indexes(executor, statement, valid_indexes):
    """ Remove invalid indexes by creating virtual indexes in different order. """
    least_indexes = valid_indexes
    for indexes in permutations(valid_indexes, len(valid_indexes)):
        cur_indexes, cost = query_index_check(executor, statement, indexes, False)
        if len(cur_indexes) < len(least_indexes):
            least_indexes = cur_indexes
    return least_indexes


def filter_candidate_columns_by_cost(valid_indexes, statement, executor, max_candidate_columns):
    indexes = []
    for table, index_group in groupby(valid_indexes, key=lambda x: x.get_table()):
        cost_index = []
        index_group = list(index_group)
        if len(index_group) <= max_candidate_columns:
            indexes.extend(index_group)
            continue
        for _index in index_group:
            _indexes, _cost = query_index_check(executor, statement, [_index])
            if _indexes:
                heapq.heappush(cost_index, (_cost, _indexes[0]))
        for _cost, _index in heapq.nsmallest(max_candidate_columns, cost_index):
            indexes.append(_index)
    return indexes


def set_source_indexes(indexes, source_indexes):
    """Record the original index of the recommended index."""
    for index in indexes:
        table = index.get_table()
        columns = index.get_columns()
        for source_index in source_indexes:
            if not source_index.get_source_index():
                continue
            if not source_index.get_table() == table:
                continue
            if f'{columns}{COLUMN_DELIMITER}'.startswith(f'{source_index.get_columns()}{COLUMN_DELIMITER}'):
                index.set_source_index(source_index.get_source_index())
                continue


def get_sort_pos(gsi, sort_dict):
    table = gsi.get_table()
    if table not in sort_dict:
        return sys.maxsize
    sort_columns = sort_dict[table]
    pos = 0
    for column in gsi.get_columns().split(COLUMN_DELIMITER):
        if column in sort_columns:
            pos = sort_columns.index(column)
            break
        else:
            pos = len(sort_columns)
            break
    return pos


def get_valid_indexes_fqs(advised_gsis, original_base_indexes, statement, executor, n_distinct):
    if not advised_gsis:
        return None
    if query_support_lp(executor, statement):
        return None
    advised_indexes = query_index_advise(executor, statement)
    sort_dict = {}
    for index in advised_indexes:
        table = index.get_table()
        table_context = get_table_context(table, executor)
        if not table_context:
            logging.info('filtered: table_context is %s and does not meet the requirements', table_context)
            continue
        if table in sort_dict:
            sort_columns = sort_dict[table]
        else:
            sort_columns = []
        for column in index.get_columns().split(COLUMN_DELIMITER):
            if column not in sort_columns and table_context.get_n_distinct(column) <= n_distinct:
                sort_columns.append(column)
        if sort_columns:
            sort_dict[table] = sort_columns
    # use the result of single query index advise to sort candidate gsi.
    valid_indexes = sorted(advised_gsis, key=lambda gsi: (get_sort_pos(gsi, sort_dict),
                                                          len(gsi.get_containing().split(COLUMN_DELIMITER))))
    valid_indexes, cost = query_index_check(executor, statement, valid_indexes,
                                            sort_by_column_no=False, is_fqs_on=True)
    for index in valid_indexes:
        if index.get_table() not in sort_dict:
            continue
        new_index = index
        # try to generate multi-column fqs gsi.
        for single_column in sort_dict[index.get_table()]:
            if single_column in index.get_containing().split(COLUMN_DELIMITER):
                columns = new_index.get_columns().split(COLUMN_DELIMITER)
                containing = new_index.get_containing().split(COLUMN_DELIMITER)
                columns.append(single_column)
                containing.remove(single_column)
                new_index = IndexItemFactory().get_index(new_index.get_table(), COLUMN_DELIMITER.join(columns),
                                                         'gsi', COLUMN_DELIMITER.join(containing))
                new_valid_index, cost = query_index_check(executor, statement, [new_index],
                                                          sort_by_column_no=False, is_fqs_on=True)
                if not new_valid_index:
                    new_index = index
        if new_index != index:
            valid_indexes[valid_indexes.index(index)] = new_index
    set_source_indexes(valid_indexes, original_base_indexes)
    return valid_indexes


def get_valid_indexes(advised_indexes, advised_gsis, original_base_indexes, statement, executor):
    need_check = False
    single_column_indexes = generate_single_column_indexes(advised_indexes)
    valid_indexes, cost = query_index_check(executor, statement, single_column_indexes + advised_gsis)
    valid_indexes = filter_candidate_columns_by_cost(valid_indexes, statement, executor, MAX_CANDIDATE_COLUMNS)
    valid_indexes, cost = query_index_check(executor, statement, valid_indexes)
    pre_indexes = valid_indexes[:]
    multi_node = is_multi_node(executor)

    # Increase the number of index columns in turn and check their validity.
    for column_num in range(2, MAX_INDEX_COLUMN_NUM + 1):
        for table, index_group in groupby(valid_indexes, key=lambda x: x.get_table()):
            _original_base_indexes = [index for index in original_base_indexes if index.get_table() == table]
            table_context = get_table_context(table, executor)
            dist_cols = COLUMN_DELIMITER.join(table_context.dist_cols)
            for index in list(index_group) + _original_base_indexes:
                columns = index.get_columns()
                containing = index.get_containing()
                index_type = index.get_index_type()
                # only validate indexes with column number of column_num
                if index.get_columns_num() != column_num - 1:
                    continue
                need_check = True
                if index_type != 'gsi':
                    for single_column_index in single_column_indexes:
                        _table = single_column_index.get_table()
                        if _table != table:
                            continue
                        single_column = single_column_index.get_columns()
                        single_index_type = single_column_index.get_index_type()
                        if single_column not in columns.split(COLUMN_DELIMITER):
                            add_more_column_index(valid_indexes, table, (columns, index_type),
                                                  (single_column, single_index_type))
                elif multi_node and containing:
                    for single_column in containing.split(COLUMN_DELIMITER):
                        single_index_type = index_type
                        if single_column not in columns.split(COLUMN_DELIMITER):
                            add_more_column_index(valid_indexes, table, (columns, index_type),
                                                  (single_column, single_index_type), containing, dist_cols)
        if need_check:
            cur_indexes, cur_cost = query_index_check(executor, statement, valid_indexes)
            # If the cost reduction does not exceed 5%, return the previous indexes.
            if cur_cost != 0 and cur_cost is not None and cost / cur_cost < 1.05:
                set_source_indexes(pre_indexes, original_base_indexes)
                return pre_indexes
            valid_indexes = cur_indexes
            pre_indexes = valid_indexes[:]
            cost = cur_cost
            need_check = False
        else:
            break

    # filtering of functionally redundant indexes due to index order
    valid_indexes = remove_unused_indexes(executor, statement, valid_indexes)
    set_source_indexes(valid_indexes, original_base_indexes)
    return valid_indexes


def get_redundant_created_indexes(indexes: List[ExistingIndex], unused_indexes: List[ExistingIndex]):
    sorted_indexes = sorted(indexes, key=lambda i: (i.get_table(), len(i.get_columns().split(COLUMN_DELIMITER))))
    redundant_indexes = []
    for table, index_group in groupby(sorted_indexes, key=lambda i: i.get_table()):
        cur_table_indexes = list(index_group)
        for pos, index in enumerate(cur_table_indexes[:-1]):
            is_redundant = False
            for next_index in cur_table_indexes[pos + 1:]:
                if match_columns(index, next_index):
                    is_redundant = True
                    index.redundant_objs.append(next_index)
            if is_redundant:
                redundant_indexes.append(index)
    remove_list = []
    for pos, index in enumerate(redundant_indexes):
        is_redundant = False
        for redundant_obj in index.redundant_objs:
            # Redundant objects are not in the useless index set, or
            # both redundant objects and redundant index in the useless index must be redundant index.
            index_exist = redundant_obj not in unused_indexes or \
                          (redundant_obj in unused_indexes and index in unused_indexes)
            if index_exist:
                is_redundant = True
        if not is_redundant:
            remove_list.append(pos)
    for item in sorted(remove_list, reverse=True):
        redundant_indexes.pop(item)
    return redundant_indexes


def record_history_invalid_indexes(history_indexes, history_invalid_indexes, indexes):
    for index in indexes:
        # Update historical indexes validity.
        schema_table = index.get_schema_table()
        cur_columns = index.get_columns()
        if not history_indexes.get(schema_table):
            continue
        for column in history_indexes.get(schema_table, dict()):
            history_index_column = list(map(str.strip, column[0].split(',')))
            existed_index_column = list(map(str.strip, cur_columns[0].split(',')))
            if len(history_index_column) > len(existed_index_column):
                continue
            if history_index_column == existed_index_column[0:len(history_index_column)]:
                history_indexes[schema_table].remove(column)
                history_invalid_indexes[schema_table] = history_invalid_indexes.get(
                    schema_table, list())
                history_invalid_indexes[schema_table].append(column)
                if not history_indexes[schema_table]:
                    del history_indexes[schema_table]


@lru_cache(maxsize=None)
def fetch_created_indexes(executor):
    created_indexes = []
    for schema in executor.get_schemas():
        sql = "select tablename from pg_catalog.pg_tables where schemaname = '%s'" % escape_single_quote(schema)
        res = executor.execute_sqls([sql])
        if not res:
            continue
        tables = parse_table_sql_results(res)
        if not tables:
            continue
        sql = get_existing_index_sql(schema, tables)
        res = executor.execute_sqls([sql])
        if not res:
            continue
        _created_indexes = parse_existing_indexes_results(res, schema)
        created_indexes.extend(_created_indexes)

    return created_indexes


def print_candidate_indexes(candidate_indexes, is_fqs_on=False):
    if is_fqs_on:
        print_header_boundary(" Generate candidate indexes(FQS GSI) ")
    else:
        print_header_boundary(" Generate candidate indexes")
    for index in candidate_indexes:
        table = index.get_quoted_table()
        columns = index.get_columns()
        index_type = index.get_index_type()
        containing = index.get_containing()
        if index_type:
            if containing:
                bar_print("table: ", table, "columns: ", columns, "containing: ", containing, "type: ", index_type)
            else:
                bar_print("table: ", table, "columns: ", columns, "type: ", index_type)
        else:
            bar_print("table: ", table, "columns: ", columns)
    if not candidate_indexes:
        bar_print("No candidate indexes generated!")


def index_sort_func(index):
    """ Sort indexes function. """
    if index.get_index_type() == 'global':
        return index.get_table(), 0, index.get_columns(), ''
    elif index.get_index_type() != 'gsi':
        return index.get_table(), 1, index.get_columns(), ''
    elif index.get_containing():
        return index.get_table(), 2, index.get_columns(), index.get_containing()
    else:
        return index.get_table(), 2, index.get_columns(), ''


def merge_gsi_with_redundant_columns(cur_index, next_index):
    # merge two gsi with redundant index columns,
    # the containing columns of two gsi will be merged.
    if not cur_index.get_containing():
        return next_index
    if not next_index.get_containing():
        next_containing = []
    else:
        next_containing = next_index.get_containing().split(COLUMN_DELIMITER)
    cur_containing = cur_index.get_containing().split(COLUMN_DELIMITER)
    next_columns = next_index.get_columns().split(COLUMN_DELIMITER)
    for containing in cur_containing:
        if containing not in next_containing and containing not in next_columns:
            next_containing.append(containing)
    merged_index = IndexItemFactory().get_index(next_index.get_table(), next_index.get_columns(),
                                                'gsi', COLUMN_DELIMITER.join(next_containing))
    return merged_index


def filter_redundant_indexes_with_same_type(indexes: List[AdvisedIndex]):
    """ Filter redundant indexes with same index_type. """
    candidate_indexes = []
    for table, table_group_indexes in groupby(sorted(indexes, key=lambda x: x.get_table()),
                                              key=lambda x: x.get_table()):
        for index_type, index_type_group_indexes in groupby(
                sorted(table_group_indexes, key=lambda x: x.get_index_type()), key=lambda x: x.get_index_type()):
            column_sorted_indexes = sorted(index_type_group_indexes, key=lambda x: x.get_columns())
            for i in range(len(column_sorted_indexes) - 1):
                if match_columns(column_sorted_indexes[i], column_sorted_indexes[i + 1]):
                    cur_index = column_sorted_indexes[i]
                    next_index = column_sorted_indexes[i + 1]
                    if (cur_index.get_index_type() == 'gsi' and next_index.get_index_type() == 'gsi'
                            and cur_index.get_containing() != next_index.get_containing()):
                        column_sorted_indexes[i + 1] = merge_gsi_with_redundant_columns(cur_index, next_index)
                    continue
                else:
                    index = column_sorted_indexes[i]
                    candidate_indexes.append(index)
            candidate_indexes.append(column_sorted_indexes[-1])
    candidate_indexes.sort(key=index_sort_func)

    return candidate_indexes


def add_query_indexes(indexes: List[AdvisedIndex], queries: List[QueryItem], pos):
    for table, index_group in groupby(indexes, key=lambda x: x.get_table()):
        _indexes = sorted(list(index_group), key=lambda x: -len(x.get_columns()))
        for _index in _indexes:
            if len(queries[pos].get_indexes()) >= FULL_ARRANGEMENT_THRESHOLD:
                break
            queries[pos].append_index(_index)


def generate_query_placeholder_indexes(query, executor: BaseExecutor, n_distinct=0.01, reltuples=10000,
                                       use_all_columns=False, advise_gsi=False, updated_dict={}):
    indexes = []
    gsis = []
    if not has_dollar_placeholder(query) and not use_all_columns:
        return []
    try:
        plan_dict = is_valid_statement(executor, query)[0]
        if not isinstance(plan_dict, dict):
            plan_dict = {'Plan': dict()}
        plan_tables, plan_columns = parse_plan(plan_dict['Plan'])
        tables = [table.lower() for table in get_query_tables(query)]
        tables = list(set(tables) | set(plan_tables))
        columns = get_potential_columns(query)
        columns = list(set(columns) | set(plan_columns))
        query_tables[query.lower()] = tables
        logging.info('parsing query: %s', query)
        logging.info('found tables: %s, columns: %s', " ".join(tables), " ".join(columns))
    except (ValueError, AttributeError, KeyError) as e:
        logging.warning('Found %s while parsing SQL statement.', e)
        return []
    multi_node = is_multi_node(executor)
    for table in tables:
        table_indexes = []
        table_columns = []
        table_gsis = []
        table_context = get_table_context(table, executor)
        if table_context:
            RELATED_TABLE_SET.add((table_context.schema, table_context.table))
        if not table_context or table_context.reltuples < reltuples:
            logging.info('filtered: table_context is %s and does not meet the requirements', table_context)
            continue
        for column in columns:
            column = column.lower()
            if table_context.has_column(column):
                table_columns.append(column)
            if table_context.has_column(column) and table_context.get_n_distinct(column) <= n_distinct:
                table_indexes.extend(generate_placeholder_indexes(table_context, column.split('.')[-1].lower()))
        if advise_gsi and multi_node:
            table_gsis = generate_placeholder_gsis(table_context, tuple(table_columns), n_distinct, multi_node)
        # filter invalid_gsi
        schema_table = f'{table_context.schema}.{table_context.table}'
        if schema_table in updated_dict:
            for gsi in table_gsis:
                if gsi.get_columns() in updated_dict[schema_table]:
                    table_gsis.remove(gsi)

        # top 20 for candidate indexes
        indexes.extend(sorted(table_indexes, key=lambda x: table_context.get_n_distinct(x.get_columns()))[:20])
        gsis.extend(sorted(table_gsis, key=lambda x: table_context.get_n_distinct(x.get_columns()))[:20])
    logging.info('related indexes: %s', indexes)
    logging.info('related gsis: %s', gsis)
    return indexes, gsis


def get_original_base_indexes(original_indexes: List[ExistingIndex]) -> List[AdvisedIndex]:
    original_base_indexes = []
    for index in original_indexes:
        table = f'{index.get_schema()}.{index.get_table()}'
        columns = index.get_columns().split(COLUMN_DELIMITER)
        index_type = index.get_index_type()
        columns_length = len(columns)
        for _len in range(1, columns_length):
            _columns = COLUMN_DELIMITER.join(columns[:_len])
            original_base_indexes.append(IndexItemFactory().get_index(table, _columns, index_type))
        all_columns_index = IndexItemFactory().get_index(table, index.get_columns(), index_type)
        original_base_indexes.append(all_columns_index)
        all_columns_index.set_source_index(index)
    return original_base_indexes


def multi_get_all_indexes(workload, total_pos_query, all_indexes, all_indexes_fqs, original_base_indexes, executor,
                          n_distinct, reltuples, use_all_columns, advise_gsi, number, updated_dict):
    with executor.session():
        for i in range(number, len(total_pos_query), MULTI_THREAD_NUM):
            pos = total_pos_query[i][0]
            query = total_pos_query[i][1]
            advised_indexes = []
            advised_gsis = []
            indexes, gsis = generate_query_placeholder_indexes(query.get_statement(), executor, n_distinct,
                                                               reltuples, use_all_columns, advise_gsi, updated_dict)
            for advised_index in indexes:
                if advised_index not in advised_indexes:
                    advised_indexes.append(advised_index)
            for advised_gsi in gsis:
                if advised_gsi not in advised_gsis:
                    advised_gsis.append(advised_gsi)

            valid_indexes = get_valid_indexes(advised_indexes, advised_gsis, original_base_indexes,
                                              query.get_statement(), executor)
            # If MAX_INDEX_STORAGE is set, We must consider the size of the index.
            # Compared with other index, GSI may hava slight performance advantage, but require much more storage.
            # In this case, we keep both GSI and other index as part of the candidate index,
            # let the MCTS decide which index is better.
            if MAX_INDEX_STORAGE and advised_gsis:
                extra_indexes = get_valid_indexes(advised_indexes, [], original_base_indexes,
                                                  query.get_statement(), executor)
                for index in extra_indexes:
                    if index not in valid_indexes:
                        valid_indexes.append(index)
            logging.info('get valid indexes: %s for the query %s', valid_indexes, query)
            add_query_indexes(valid_indexes, workload.get_queries(), pos)
            for index in valid_indexes:
                if index not in all_indexes:
                    all_indexes.append(index)
            valid_indexes_fqs = get_valid_indexes_fqs(advised_gsis, original_base_indexes,
                                                      query.get_statement(), executor, n_distinct)
            logging.info('get valid indexes for fqs: %s for the query %s', valid_indexes_fqs, query)
            if valid_indexes_fqs:
                for index in valid_indexes_fqs:
                    if index not in all_indexes_fqs:
                        all_indexes_fqs.append(index)
            GLOBAL_PROCESS_BAR.next_bar()


def generate_candidate_indexes(workload: WorkLoad, executor: BaseExecutor, n_distinct, reltuples, use_all_columns,
                               **kwargs):
    advise_gsi = kwargs.get('advise_gsi')
    all_indexes = []
    all_indexes_fqs = []
    # Resolve the bug that indexes extended on top of the original index will not be recommended
    # by building the base index related to the original index
    original_indexes = fetch_created_indexes(executor)
    original_base_indexes = get_original_base_indexes(original_indexes)
    updated_dict = {}
    if advise_gsi:
        for query in workload.get_queries():
            updated_table, updated_columns = get_updated_columns(
                query.get_statement())
            if updated_table:
                if '.' not in updated_table:
                    updated_table = executor.get_schema() + '.' + updated_table
                updated_dict.update({updated_table: updated_columns})
    total_pos_query = list(enumerate(workload.get_queries()))
    if MULTI_THREAD_NUM:
        thread_list = []
        GLOBAL_PROCESS_BAR.reset_bar(len(total_pos_query), 'Candidate indexes')
        for number in range(MULTI_THREAD_NUM):
            thread_list.append(threading.Thread(target=multi_get_all_indexes,
                                                args=(workload, total_pos_query, all_indexes, all_indexes_fqs,
                                                      original_base_indexes, executor, n_distinct, reltuples,
                                                      use_all_columns, advise_gsi, number, updated_dict)))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
    else:
        with executor.session():
            for pos, query in GLOBAL_PROCESS_BAR.process_bar(total_pos_query, 'Candidate indexes'):
                advised_indexes = []
                advised_gsis = []
                indexes, gsis = generate_query_placeholder_indexes(query.get_statement(), executor, n_distinct,
                                                                   reltuples, use_all_columns, advise_gsi, updated_dict)
                for advised_index in indexes:
                    if advised_index not in advised_indexes:
                        advised_indexes.append(advised_index)
                for advised_gsi in gsis:
                    if advised_gsi not in advised_gsis:
                        advised_gsis.append(advised_gsi)

                valid_indexes = get_valid_indexes(advised_indexes, advised_gsis, original_base_indexes,
                                                  query.get_statement(), executor)
                # If MAX_INDEX_STORAGE is set, We must consider the size of the index.
                # Compared with other index, GSI may hava slight performance advantage, but require much more storage.
                # In this case, we keep both GSI and other index as part of the candidate index,
                # let the MCTS decide which index is better.
                if MAX_INDEX_STORAGE and advised_gsis:
                    extra_indexes = get_valid_indexes(advised_indexes, [], original_base_indexes,
                                                      query.get_statement(), executor)
                    for index in extra_indexes:
                        if index not in valid_indexes:
                            valid_indexes.append(index)
                logging.info('get valid indexes: %s for the query %s', valid_indexes, query)
                add_query_indexes(valid_indexes, workload.get_queries(), pos)
                for index in valid_indexes:
                    if index not in all_indexes:
                        all_indexes.append(index)
                valid_indexes_fqs = get_valid_indexes_fqs(advised_gsis, original_base_indexes,
                                                          query.get_statement(), executor, n_distinct)
                logging.info('get valid indexes for fqs: %s for the query %s', valid_indexes_fqs, query)
                if valid_indexes_fqs:
                    for index in valid_indexes_fqs:
                        if index not in all_indexes_fqs:
                            all_indexes_fqs.append(index)
    # Filter redundant indexes.
    candidate_indexes = filter_redundant_indexes_with_same_type(all_indexes)
    determine_indexes_fqs = filter_redundant_indexes_with_same_type(all_indexes_fqs)
    for index in candidate_indexes:
        if index.get_index_type() != 'gsi':
            continue
        if index in determine_indexes_fqs:
            candidate_indexes.remove(index)
            continue
        for i in range(len(determine_indexes_fqs)):
            gsi_fqs = determine_indexes_fqs[i]
            if match_columns(index, gsi_fqs):
                determine_indexes_fqs[i] = merge_gsi_with_redundant_columns(index, gsi_fqs)
                candidate_indexes.remove(index)
            elif match_columns(gsi_fqs, index):
                determine_indexes_fqs[i] = merge_gsi_with_redundant_columns(gsi_fqs, index)
                candidate_indexes.remove(index)

    if len(candidate_indexes) == 0:
        estimate_workload_cost_file(executor, workload)
    return determine_indexes_fqs, candidate_indexes


def generate_sorted_atomic_config(queries: List[QueryItem],
                                  candidate_indexes: List[AdvisedIndex]) -> List[Tuple[AdvisedIndex, ...]]:
    atomic_config_total = []

    for query in queries:
        if len(query.get_indexes()) == 0:
            continue

        indexes = []
        for i, (table, group) in enumerate(groupby(query.get_sorted_indexes(), lambda x: x.get_table())):
            # The max number of table is 2.
            if i > 1:
                break
            # The max index number for each table is 2.
            indexes.extend(list(group)[:2])
        atomic_configs = powerset(indexes)
        for new_config in atomic_configs:
            if new_config not in atomic_config_total:
                atomic_config_total.append(new_config)
    # Make sure atomic_config_total contains candidate_indexes.
    for index in candidate_indexes:
        if (index,) not in atomic_config_total:
            atomic_config_total.append((index,))
    return atomic_config_total


def generate_atomic_config_containing_same_columns(candidate_indexes: List[AdvisedIndex]) \
        -> List[Tuple[AdvisedIndex, AdvisedIndex]]:
    atomic_configs = []
    for _, _indexes in groupby(sorted(candidate_indexes, key=lambda index: (index.get_table(), index.get_index_type())),
                               key=lambda index: (index.get_table(), index.get_index_type())):
        _indexes = list(_indexes)
        _indexes.sort(key=lambda index: len(index.get_columns().split(COLUMN_DELIMITER)))
        for short_index_idx in range(len(_indexes) - 1):
            short_columns = set(_indexes[short_index_idx].get_columns().split(COLUMN_DELIMITER))
            for long_index_idx in range(short_index_idx + 1, len(_indexes)):
                long_columns = set(_indexes[long_index_idx].get_columns().split(COLUMN_DELIMITER))
                if not (short_columns - long_columns):
                    atomic_configs.append((_indexes[short_index_idx], _indexes[long_index_idx]))

    return atomic_configs


def display_redundant_indexes(redundant_indexes: List[ExistingIndex]):
    if not redundant_indexes:
        bar_print("No redundant indexes!")
    # Display redundant indexes.
    for index in redundant_indexes:
        if index.get_is_unique() or index.is_primary_key():
            continue
        statement = "DROP INDEX %s.%s;(%s)" % (index.get_schema(), index.get_indexname(), index.get_indexdef())
        bar_print(statement)
        bar_print('Related indexes:')
        for _index in index.redundant_objs:
            _statement = "\t%s" % (_index.get_indexdef())
            bar_print(_statement)
        bar_print('')


def record_redundant_indexes(redundant_indexes: List[ExistingIndex], detail_info):
    for index in redundant_indexes:
        statement = "DROP INDEX %s.%s;" % (index.get_schema(), index.get_indexname())
        existing_index = [item.get_indexname() + ':' + item.get_columns()
                          for item in index.redundant_objs]
        redundant_index = {"schemaName": index.get_schema(), "tbName": index.get_table(),
                           "type": IndexType.REDUNDANT.value,
                           "columns": index.get_columns(), "statement": statement,
                           "existingIndex": existing_index}
        detail_info['uselessIndexes'].append(redundant_index)


def display_useless_redundant_indexes(created_indexes, workload_indexnames, detail_info):
    unused_indexes = [index for index in created_indexes if index.get_indexname() not in workload_indexnames]
    print_header_boundary(" Current workload useless indexes ")
    detail_info['uselessIndexes'] = []
    has_unused_index = False

    for cur_index in unused_indexes:
        if (not cur_index.get_is_unique()) and (not cur_index.is_primary_key()):
            has_unused_index = True
            statement = "DROP INDEX %s;" % cur_index.get_indexname()
            bar_print(statement)
            useless_index = {"schemaName": cur_index.get_schema(), "tbName": cur_index.get_table(),
                             "type": IndexType.INVALID.value,
                             "columns": cur_index.get_columns(), "statement": statement}
            detail_info['uselessIndexes'].append(useless_index)

    if not has_unused_index:
        bar_print("No useless indexes!")
    print_header_boundary(" Redundant indexes ")
    redundant_indexes = get_redundant_created_indexes(created_indexes, unused_indexes)
    display_redundant_indexes(redundant_indexes)
    record_redundant_indexes(redundant_indexes, detail_info)


def greedy_determine_opt_config(workload: WorkLoad, atomic_config_total: List[Tuple[AdvisedIndex]],
                                candidate_indexes: List[AdvisedIndex]):
    opt_config = []
    candidate_indexes_copy = candidate_indexes[:]
    for i in range(len(candidate_indexes_copy)):
        cur_max_benefit = 0
        cur_index = None
        for index in candidate_indexes_copy:
            cur_config = copy.copy(opt_config)
            cur_config.append(index)
            cur_estimated_benefit = infer_workload_benefit(workload, cur_config, atomic_config_total)
            if cur_estimated_benefit > cur_max_benefit:
                cur_max_benefit = cur_estimated_benefit
                cur_index = index
        if cur_index:
            if len(opt_config) == MAX_INDEX_NUM:
                break
            opt_config.append(cur_index)
            candidate_indexes_copy.remove(cur_index)
        else:
            break

    return opt_config


def get_last_indexes_result(input_path):
    last_indexes_result_file = os.path.join(os.path.realpath(
        os.path.dirname(input_path)), 'index_result.json')
    integrate_indexes = {'historyIndexes': {}}
    if os.path.exists(last_indexes_result_file):
        try:
            with open(last_indexes_result_file, 'r', errors='ignore') as file:
                integrate_indexes['historyIndexes'] = json.load(file)
        except json.JSONDecodeError:
            return integrate_indexes
    return integrate_indexes


def recalculate_cost_for_opt_indexes(workload: WorkLoad, indexes: Tuple[AdvisedIndex]):
    """After the recommended indexes are all built, calculate the gain of each index."""
    all_used_index_names = workload.get_workload_used_indexes(indexes)
    for query, used_index_names in zip(workload.get_queries(), all_used_index_names):
        cost = workload.get_indexes_cost_of_query(query, indexes)
        origin_cost = workload.get_indexes_cost_of_query(query, None)
        query_benefit = origin_cost - cost
        query.set_benefit(query_benefit)
        query.reset_opt_indexes()
        if not query_benefit > 0:
            continue
        for index in indexes:
            for index_name in used_index_names:
                if index.match_index_name(index_name):
                    index.append_positive_query(query)
                    query.append_index(index)


def filter_no_benefit_indexes(indexes):
    for index in indexes[:]:
        if not index.get_positive_queries():
            indexes.remove(index)
            logging.info('remove no benefit index %s', index)


def index_advisor_workload(history_advise_indexes, executor: BaseExecutor, workload_file_path,
                           multi_iter_mode: bool, show_detail: bool, n_distinct: float, reltuples: int,
                           use_all_columns: bool, **kwargs):
    queries = compress_workload(workload_file_path)
    if MULTI_THREAD_NUM:
        valid_queries = []
        thread_list = []
        GLOBAL_PROCESS_BAR.reset_bar(len(queries), 'Determine valid queries')
        for number in range(MULTI_THREAD_NUM):
            thread_list.append(threading.Thread(target=multi_is_valid_statement,
                                                args=(executor, queries, valid_queries, number)))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        queries = valid_queries
    else:
        queries = [query for query in queries if is_valid_statement(executor, query.get_statement())]
    workload = WorkLoad(queries)
    determine_indexes_fqs, candidate_indexes = generate_candidate_indexes(workload, executor, n_distinct, reltuples,
                                                                          use_all_columns, **kwargs)
    print_candidate_indexes(determine_indexes_fqs, is_fqs_on=True)
    print_candidate_indexes(candidate_indexes, is_fqs_on=False)
    index_advisor = IndexAdvisor(executor, workload, multi_iter_mode)
    index_advisor.determine_indexes_fqs = determine_indexes_fqs
    if determine_indexes_fqs or candidate_indexes:
        print_header_boundary(" Determine optimal indexes ")
    if candidate_indexes:
        with executor.session():
            if multi_iter_mode:
                opt_indexes = index_advisor.complex_index_advisor(candidate_indexes)
            else:
                opt_indexes = index_advisor.simple_index_advisor(candidate_indexes)
        if opt_indexes:
            index_advisor.filter_low_benefit_index(opt_indexes, kwargs.get('improved_rate', 0))
            if index_advisor.determine_indexes:
                estimate_workload_cost_file(executor, workload, tuple(index_advisor.determine_indexes))
                recalculate_cost_for_opt_indexes(workload, tuple(index_advisor.determine_indexes))
            determine_indexes = index_advisor.determine_indexes[:]
            filter_no_benefit_indexes(index_advisor.determine_indexes)
            index_advisor.determine_indexes.sort(key=lambda index: -sum(query.get_benefit()
                                                                        for query in index.get_positive_queries()))
            workload.replace_indexes(tuple(determine_indexes), tuple(index_advisor.determine_indexes))

    index_advisor.display_advise_indexes_info(show_detail)
    created_indexes = fetch_created_indexes(executor)
    if kwargs.get('show_benefits'):
        index_advisor.print_benefits(created_indexes)
    index_advisor.generate_incremental_index(history_advise_indexes)
    history_invalid_indexes = {}
    with executor.session():
        index_advisor.generate_redundant_useless_indexes(history_invalid_indexes)
    index_advisor.display_incremental_index(
        history_invalid_indexes, workload_file_path)
    if show_detail:
        print_header_boundary(" Display detail information ")
        sql_info = json.dumps(
            index_advisor.display_detail_info, indent=4, separators=(',', ':'))
        bar_print(sql_info)
    # Retain only related_index
    for idx, useless_index in enumerate(index_advisor.display_detail_info.get('uselessIndexes', [])[:]):
        if (useless_index['schemaName'], useless_index['tbName']) not in RELATED_TABLE_SET:
            index_advisor.display_detail_info.get('uselessIndexes').pop(
                index_advisor.display_detail_info.get('uselessIndexes').index(useless_index))
            continue
        index_name = useless_index['statement'].replace('DROP INDEX ', '').strip(';')
        if not index_name.startswith(useless_index['schemaName'] + '.'):
            index_name = useless_index['schemaName'] + '.' + index_name
        useless_index['statement'] = index_name
    RELATED_TABLE_SET.clear()
    return index_advisor.display_detail_info, index_advisor.index_benefits, index_advisor.redundant_indexes


def check_parameter(args):
    global MAX_INDEX_NUM, MAX_INDEX_STORAGE
    if args.max_index_num is not None and args.max_index_num <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_num)
    if args.max_index_columns <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_columns)
    if args.max_index_storage is not None and args.max_index_storage <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_storage)
    if args.max_n_distinct <= 0 or args.max_n_distinct > 1:
        raise argparse.ArgumentTypeError(
            '%s is an invalid max-n-distinct which ranges from 0 to 1' % args.max_n_distinct)
    if args.min_improved_rate < 0 or args.min_improved_rate >= 1:
        raise argparse.ArgumentTypeError(
            '%s is an invalid min-improved-rate which must be greater than '
            'or equal to 0 and less than 1' % args.min_improved_rate)
    if args.min_reltuples <= 0:
        raise argparse.ArgumentTypeError('%s is an invalid positive int value' % args.min_reltuples)
    if args.multi_thread_num is None:
        dbmind.components.index_advisor.workload.MULTI_THREAD_NUM = 0
    elif 1 <= args.multi_thread_num <= 64:
        dbmind.components.index_advisor.workload.MULTI_THREAD_NUM = args.multi_thread_num
    else:
        raise argparse.ArgumentTypeError('%s is an invalid multi_thread_num which ranges from 1 to 64'
                                         % args.multi_thread_num)
    dbmind.components.index_advisor.workload.JSON_TYPE = args.json
    MAX_INDEX_NUM = args.max_index_num
    MAX_INDEX_STORAGE = args.max_index_storage
    dbmind.components.index_advisor.workload.MAX_INDEX_COLUMN_NUM = args.max_index_columns
    # Check if the password contains illegal characters.
    is_legal = re.search(r'^[A-Za-z0-9~!@#$%^&*()-_=+\|\[{}\];:,<.>/?]+$', args.W)
    if not is_legal:
        raise ValueError("The password contains illegal characters.")


def check_gsi_params(executor, advise_gsi):
    if not is_multi_node(executor) and advise_gsi:
        raise argparse.ArgumentTypeError('GSI only support distributed scenarios.')
    for row in executor.execute_sqls(["select * from pg_catalog.pg_settings where name='enable_gsiscan';"]):
        if row == ('(0 row)',):
            return argparse.ArgumentTypeError('GSI is not supported in current database clusters.')


def main(argv):
    arg_parser = argparse.ArgumentParser(
        description='Generate index set for workload.')
    arg_parser.add_argument("db_port", help="Port of database", type=int)
    arg_parser.add_argument("database", help="Name of database", action=CheckWordValid)
    arg_parser.add_argument(
        "--db-host", "--h", help="Host for database", action=CheckWordValid)
    arg_parser.add_argument(
        "-U", "--db-user", help="Username for database log-in", action=CheckWordValid)
    arg_parser.add_argument(
        "file", type=path_type, help="File containing workload queries (One query per line)", action=CheckWordValid)
    arg_parser.add_argument("--schema", help="Schema name for the current business data",
                            required=True, action=CheckWordValid)
    arg_parser.add_argument(
        "--max-index-num", "--max_index_num", help="Maximum number of suggested indexes", type=int)
    arg_parser.add_argument("--max-index-storage", "--max_index_storage",
                            help="Maximum storage of suggested indexes/MB", type=int)
    arg_parser.add_argument("--multi-iter-mode", "--multi_iter_mode", action='store_true',
                            help="Whether to use multi-iteration algorithm", default=False)
    arg_parser.add_argument("--max-n-distinct", type=float,
                            help="Maximum n_distinct value (reciprocal of the distinct number)"
                                 " for the index column.",
                            default=0.01)
    arg_parser.add_argument("--min-improved-rate", type=float,
                            help="Minimum improved rate of the cost for the indexes",
                            default=0.1)
    arg_parser.add_argument('--max-index-columns', type=int,
                            help='Maximum number of columns in a joint index',
                            default=4)
    arg_parser.add_argument("--min-reltuples", type=int,
                            help="Minimum reltuples value for the index column.", default=10000)
    arg_parser.add_argument("--multi-node", "--multi_node", action='store_true',
                            help="Whether to support distributed scenarios", default=False)
    arg_parser.add_argument("--json", action='store_true',
                            help="Whether the workload file format is json", default=False)
    arg_parser.add_argument("--driver", action='store_true',
                            help="Whether to employ python-driver", default=False)
    arg_parser.add_argument("--show-detail", "--show_detail", action='store_true',
                            help="Whether to show detailed sql information", default=False)
    arg_parser.add_argument("--show-benefits", action='store_true',
                            help="Whether to show index benefits", default=False)
    arg_parser.add_argument("--advise_gsi", action='store_true',
                            help="Whether to advise gsis", default=False)
    arg_parser.add_argument("--multi_thread_num", type=int,
                            help="Use multithreading and determine threads number.")
    args = arg_parser.parse_args(argv)

    set_logger()
    args.W = get_password()
    check_parameter(args)
    # Initialize the connection.
    if args.driver:
        try:
            import psycopg2
            try:
                from .executors.driver_executor import DriverExecutor
            except ImportError:
                from executors.driver_executor import DriverExecutor
            executor = DriverExecutor(args.database, args.db_user, args.W, args.db_host, args.db_port, args.schema)
        except ImportError:
            logging.warning('Python driver import failed, '
                            'the gsql mode will be selected to connect to the database.')
            executor = GsqlExecutor(args.database, args.db_user, args.W, args.db_host, args.db_port, args.schema)
            args.driver = None
    else:
        executor = GsqlExecutor(args.database, args.db_user, args.W, args.db_host, args.db_port, args.schema)
    check_gsi_params(executor, args.advise_gsi)
    use_all_columns = True
    index_advisor_workload(get_last_indexes_result(args.file), executor, args.file,
                           args.multi_iter_mode, args.show_detail, args.max_n_distinct, args.min_reltuples,
                           use_all_columns, advise_gsi=args.advise_gsi, improved_rate=args.min_improved_rate,
                           max_candidate_columns=MAX_CANDIDATE_COLUMNS, show_benefits=args.show_benefits)


if __name__ == '__main__':
    main(sys.argv[1:])

