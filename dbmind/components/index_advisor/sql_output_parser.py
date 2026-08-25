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
from typing import List
import logging

from sqlparse.tokens import Punctuation, Keyword, Name

from .utils import get_tokens, QUERY_PLAN_SUFFIX, EXPLAIN_SUFFIX, ERROR_KEYWORD
from .models import UniqueList, ExistingIndex, AdvisedIndex, IndexItemFactory


def __get_columns_from_indexdef(indexdef):
    for content in get_tokens(indexdef):
        if content.ttype is Punctuation and content.normalized == '(':
            return content.parent.value.strip()[1:-1]


def __is_unique_from_indexdef(indexdef):
    for content in get_tokens(indexdef):
        if content.ttype is Keyword:
            return content.value.upper() == 'UNIQUE'


def __get_index_type_from_indexdef(indexdef):
    for content in get_tokens(indexdef):
        if content.ttype is Name:
            if content.value.upper() == 'LOCAL':
                return 'local'
            elif content.value.upper() == 'GLOBAL':
                return 'global'


def is_lp_plan(results):
    for cur_tuple in results:
        text = cur_tuple[0]
        if 'Data Node Scan' in text and ('__REMOTE_LIGHT_QUERY__' in text):
            return True
    return False
    

def parse_existing_indexes_results(results, schema) -> List[ExistingIndex]:
    indexes = list()
    indexdef_list = []
    table = index = pkey = None
    for cur_tuple in results:
        if len(cur_tuple) == 1:
            continue
        else:
            temptable, tempindex, indexdef, temppkey = cur_tuple
            if temptable and tempindex:
                table, index, pkey = temptable, tempindex, temppkey
            if indexdef.endswith('+'):
                if len(indexdef_list) >= 1:
                    if indexdef.startswith('SUBPARTITION'):
                        indexdef_list.append(' ' * 8 + indexdef.strip(' +'))
                    else:
                        indexdef_list.append(' ' * 4 + indexdef.strip(' +'))
                else:
                    indexdef_list.append(indexdef.strip(' +'))
                continue
            elif indexdef_list and indexdef.startswith(')'):
                indexdef_list.append(indexdef.strip().strip('+').strip())
                indexdef = '\n'.join(indexdef_list)
                indexdef_list = []
            cur_columns = __get_columns_from_indexdef(indexdef)
            is_unique = __is_unique_from_indexdef(indexdef)
            index_type = __get_index_type_from_indexdef(indexdef)
            cur_index = ExistingIndex(
                schema, table, index, cur_columns, indexdef)
            if pkey:
                cur_index.set_is_primary_key(True)
            if is_unique:
                cur_index.set_is_unique()
            if index_type:
                cur_index.set_index_type(index_type)
            indexes.append(cur_index)
    return indexes


def parse_table_sql_results(table_sql_results):
    tables = []
    for cur_tuple in table_sql_results:
        text = cur_tuple[0]
        if 'tablename' in text or re.match(r'-+', text) or re.match(r'\(\d+ rows?\)', text) \
                or text.strip().startswith('SELECT '):
            continue
        tables.append(text.strip())
    return tables


def parse_hypo_index(results):
    hypo_index_ids = []
    for cur_tuple in results:
        text = cur_tuple[0]
        if 'btree' in text:
            hypo_index_id = text.strip().strip('()').split(',')[0]
            hypo_index_ids.append(hypo_index_id)
    return hypo_index_ids


def parse_explain_plan(results, query_num):
    # record execution plan for each explain statement (the parameter results contain multiple explain results)
    plans = []
    plan = []
    index_names_list = []
    found_plan = False
    plan_start = False
    costs = []
    i = 0
    index_names = UniqueList()
    for cur_tuple in results:
        text = cur_tuple[0]
        # Save the results of the last index_names according to the EXPLAIN keyword.
        if QUERY_PLAN_SUFFIX in text or text == EXPLAIN_SUFFIX:
            index_names_list.append(index_names)
            index_names = UniqueList()
            plans.append(plan)
            plan = []
            found_plan = True
            plan_start = True
            continue
        if plan_start:
            plan.append(cur_tuple[0])
        # Consider execution errors and ensure that the cost value of an explain is counted only once.
        if ERROR_KEYWORD in text and 'prepared statement' not in text:
            if i >= query_num:
                logging.info('Cannot correct parse the explain results: %s', results)
                raise ValueError("The size of queries is not correct!")
            costs.append(0)
            index_names_list.append(index_names)
            index_names = UniqueList()
            i += 1
        if found_plan and '(cost=' in text:
            if i >= query_num:
                logging.info('Cannot correct parse the explain results: %s', results)
                raise ValueError("The size of queries is not correct!")
            query_cost = parse_plan_cost(text)
            costs.append(query_cost)
            found_plan = False
            i += 1
        if 'Index' in text and 'Scan' in text:
            try:
                ind1, ind2 = re.search(r'Index.*Scan.* using (.*) on (.*) \(cost',
                                       text.strip(), re.IGNORECASE).groups()
            except:
                ind1, ind2 = re.search(r'Index.*Scan(.*)on ([^\s]+)',
                                       text.strip(), re.IGNORECASE).groups()
                ind1 = ind1.strip().split(' ')[-1]
            if ind1.strip():
                # `Index (Only)? Scan (Backward)? using index1`
                if ind1 not in index_names:
                    index_names.append(ind1)
            else:
                index_names.append(ind2)
    index_names_list.append(index_names)
    index_names_list = index_names_list[1:]
    plans.append(plan)
    plans = plans[1:]

    # when a syntax error causes multiple explain queries to be run as one query
    while len(index_names_list) < query_num:
        index_names_list.append([])
    while len(plans) < query_num:
        plans.append([])
    while i < query_num:
        costs.append(0)
        i += 1
    return costs, index_names_list, plans


def parse_plan_cost(line):
    """ Parse the explain plan to get the estimated cost by database optimizer. """
    cost = -1
    # like "Limit  (cost=19932.04..19933.29 rows=100 width=17)"
    pattern = re.compile(r'\(cost=([^)]*)\)', re.S)
    matched_res = re.search(pattern, line)
    if matched_res and len(matched_res.group(1).split()) == 3:
        _cost, _rows, _width = matched_res.group(1).split()
        # like cost=19932.04..19933.29
        cost = float(_cost.split('..')[-1])
    return cost


def parse_single_advisor_results(results) -> List[AdvisedIndex]:
    indexes = []
    for cur_tuple in results:
        res = cur_tuple[0]
        schema_idx = 0
        table_idx = 1
        index_type_idx = -1
        columns_slice = slice(2, -1)
        # like '(1 row)' or (2 rows)
        if res.strip().endswith('rows)') or res.strip().endswith(' row)'):
            continue
        # like ' (public,date_dim,d_year,global)' or ' (public,store_sales,"ss_sold_date_sk,ss_item_sk","")'
        if len(res) > 2 and res.strip()[0:1] == '(':
            items = res.strip().split(',')
            table = items[schema_idx][1:] + '.' + items[table_idx]
            columns = ','.join(items[columns_slice]).strip('\"')
            if columns == '':
                continue
            if items[index_type_idx].strip(') ') not in ['global', 'local']:
                index_type = ''
            else:
                index_type = items[index_type_idx].strip(') ')
            indexes.append(IndexItemFactory().get_index(table, columns, index_type))
    return indexes


def get_checked_indexes(indexes, index_check_results) -> list:
    valid_indexes = []
    hypo_index_info_length = 4
    hypo_index_names = []
    hypo_index_name_idx = 0
    rows_with_index = []
    for cur_tuple in index_check_results:
        # like '(<134672>btree_local_customer_c_customer_sk,134672,customer,"(c_customer_sk)")'
        text = cur_tuple[0]
        if text.strip('("').startswith('<') and 'btree' in text:
            if len(text.split(',', 3)) == hypo_index_info_length:
                hypo_index_info = text.split(',', 3)
                hypo_index_name = hypo_index_info[hypo_index_name_idx].strip('(')
                if hypo_index_name.startswith('"'):
                    hypo_index_name = hypo_index_name[1:-1]
                hypo_index_names.append(hypo_index_name)

        if 'Index' in text and 'Scan' in text and 'btree' in text:
            rows_with_index.append(text)
        for row_with_index in rows_with_index:
            for hypo_index_name in hypo_index_names:
                if hypo_index_name in row_with_index:
                    used_index = indexes[hypo_index_names.index(hypo_index_name)]
                    if used_index not in valid_indexes:
                        valid_indexes.append(used_index)

    return valid_indexes
