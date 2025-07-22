# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
import re
import numpy as np
import pypred
from src.plan_encoding.meta_info import *
from src.parameters import *

from src.feature_extraction.predicate_operators import *
from src.plan_encoding.encoding_predicates import *


def remove_invalid_tokens(predicate):
    x = re.sub(r'\(\(([a-zA-Z_]+)\)::text ~~ \'(((?!::text).)*)\'::text\)',
               r"(\1 = '__LIKE__\2')", predicate)
    x = re.sub(r'\(\(([a-zA-Z_]+)\)::text !~~ \'(((?!::text).)*)\'::text\)',
               r"(\1 = '__NOTLIKE__\2')", x)
    x = re.sub(r'\(\(([a-zA-Z_]+)\)::text <> \'(((?!::text).)*)\'::text\)',
               r"(\1 = '__NOTEQUAL__\2')", x)
    x = re.sub(r'\(([a-zA-Z_]+) ~~ \'(((?!::text).)*)\'::text\)',
               r"(\1 = '__LIKE__\2')", x)
    x = re.sub(r'\(([a-zA-Z_]+) !~~ \'(((?!::text).)*)\'::text\)',
               r"(\1 = '__NOTLIKE__\2')", x)
    x = re.sub(r'\(([a-zA-Z_]+) <> \'(((?!::text).)*)\'::text\)',
               r"(\1 = '__NOTEQUAL__\2')", x)
    x = re.sub(r'(\'[^\']*\')::[a-z_]+', r'\1', x)
    x = re.sub(r'\(([^\(]+)\)::[a-z_]+', r'\1', x)
    x = re.sub(r'\(([a-z_0-9A-Z\-]+) = ANY \(\'(\{.+\})\'\[\]\)\)',
               r"(\1 = '__ANY__\2')", x)
    return x


def predicates2seq(pre_tree, alias2table, relation_name, index_name):
    current_level = -1
    current_line = 0
    sequence = []
    while current_line < len(pre_tree):
        operator_str = pre_tree[current_line]
        level = len(re.findall(r'\t', operator_str))
        operator_seq = operator_str.strip('\t').split(' ')
        operator_type = operator_seq[1]
        operator = operator_seq[0]
        if level <= current_level:
            for i in range(current_level - level + 1):
                pass
        current_level = level
        if operator_type == 'operator':
            sequence.append(Operator(operator))
            current_line += 1
        elif operator_type == 'comparison':
            operator = operator_seq[0]
            current_line += 1
            operator_str = pre_tree[current_line]
            operator_seq = operator_str.strip('\t').split(' ')
            left_type = operator_seq[0]
            left_value = operator_seq[1]
            current_line += 1
            operator_str = pre_tree[current_line]
            operator_seq = operator_str.strip('\t').split(' ')
            right_type = operator_seq[0]
            if right_type == 'Number':
                right_value = operator_seq[1]
            elif right_type == 'Literal':
                p = re.compile("Literal (.*) at line:")
                result = p.search(operator_str)
                right_value = result.group(1)
            elif right_type == 'Constant':
                p = re.compile("Constant (.*) at line:")
                result = p.search(operator_str)
                right_value = result.group(1)
            else:
                raise "Unsupport Value Type: " + right_type

            if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$',
                        left_value) is not None:
                left_relation = left_value.split('.')[0]
                left_column = left_value.split('.')[1]
                if left_relation in alias2table:
                    left_relation = alias2table[left_relation]
                left_value = left_relation + '.' + left_column
            else:
                if relation_name is None:
                    relation = index_name.replace(left_value + '_', '')
                else:
                    relation = relation_name
                left_value = relation + '.' + left_value

            if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$',
                        right_value) is not None:
                right_relation = right_value.split('.')[0]
                right_column = right_value.split('.')[1]
                if right_relation in alias2table:
                    right_relation = alias2table[right_relation]
                right_value = right_relation + '.' + right_column
            sequence.append(
                Comparison(operator, left_value, right_value.strip('\'')))
            current_line += 1
    return sequence


def pre2seq(predicates, alias2table, relation_name, index_name):

    pr = remove_invalid_tokens(predicates)
    pr = pr.replace("''", " ")
    p = pypred.Predicate(pr)
    try:
        pp = p.description().strip('\n').split('\n')
    except:
        return []
    try:
        predicates = predicates2seq(pp, alias2table, relation_name, index_name)
    except:
        raise
    return predicates


def get_value_reps_mean(conds, relation_name, index_name):
    sum = 0
    cnt = 0
    for cond in conds:
        cur = get_value_rep(cond, relation_name, index_name)
        if cur != None:
            sum += cur
            cnt += 1
    if cnt == 0:
        return None
    return sum / cnt


def get_value_rep(condition_op, relation_name, index_name):
    if condition_op is None:
        return None
    elif condition_op.op_type == 'Bool':
        return None
    else:
        left_value = condition_op.left_value
        right_value = condition_op.right_value

        if re.match(r'.+\..+', left_value) is None:
            if relation_name is None:
                relation_name = index_name.split(left_value)[1].strip('_')
            left_value = relation_name + '.' + left_value
        else:
            relation_name = left_value.split('.')[0]

        if relation_name not in parameters.tables_id:
            return None
        else:

            if re.match(r'^[a-z][a-zA-Z0-9_]*\.[a-z][a-zA-Z0-9_]*$', right_value) is not None \
                    and right_value.split('.')[0] in parameters.tables_id:
                return None

            elif re.match(r'^[a-z][a-zA-Z0-9_]*\.[a-z][a-zA-Z0-9_]*$', right_value) is not None \
                    and right_value.split('.')[0] not in parameters.tables_id:
                return None
            elif right_value == 'None':
                return None

            elif left_value in parameters.columnTypeisNum:
                try:
                    right_value = float(right_value)
                    right_value_idx = get_idx_box(right_value, left_value,
                                                  parameters)
                    return right_value_idx
                except:
                    return None

            elif re.match(r'^__LIKE__', right_value) is not None:
                right_value = right_value.strip('\'')[8:]
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                return right_value_idxs[0]
            elif re.match(r'^__NOTLIKE__', right_value) is not None:
                right_value = right_value.strip('\'')[11:]
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                return right_value_idxs[0]
            elif re.match(r'^__NOTEQUAL__', right_value) is not None:
                right_value = right_value.strip('\'')[12:]
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                return right_value_idxs[0]

            else:
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                return right_value_idxs[0]


def getParameters():
    column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, tables, columnTypeisNum, box_lines = prepare_dataset(
    )
    table_total_num = len(tables_id)
    column_total_num = len(columns_id)
    physic_op_total_num = len(physic_ops_id)
    compare_ops_total_num = len(compare_ops_id)
    bool_ops_total_num = len(bool_ops_id)
    box_num = 10
    condition_op_dim = bool_ops_total_num + \
        compare_ops_total_num + column_total_num + box_num

    parameters = Parameters(tables_id, columns_id, physic_ops_id,
                            column_total_num, table_total_num,
                            physic_op_total_num, condition_op_dim,
                            compare_ops_id, bool_ops_id, bool_ops_total_num,
                            compare_ops_total_num, box_num, columnTypeisNum,
                            box_lines)
    return parameters


parameters = getParameters()
