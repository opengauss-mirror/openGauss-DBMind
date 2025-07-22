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
import datetime
import decimal
from src.plan_encoding.meta_info import *


def get_representation(value, word_vectors):
    if value in word_vectors:
        embedded_result = np.array(list(word_vectors[value]))
    else:
        embedded_result = np.array([0.0 for _ in range(500)])
    hash_result = np.array([0.0 for _ in range(500)])
    for t in value:
        hash_result[hash(t) % 500] = 1.0
    return np.concatenate((embedded_result, hash_result), 0)


def get_str_representation(value, column, word_vectors):
    vec = np.array([])
    count = 0
    prefix = determine_prefix(column)
    for v in value.split('%'):
        if len(v) > 0:
            if len(vec) == 0:
                vec = get_representation(prefix + v, word_vectors)
                count = 1
            else:
                new_vec = get_representation(prefix + v, word_vectors)
                vec = vec + new_vec
                count += 1
    if count > 0:
        vec /= float(count)
    return vec


def get_str_representation_box(value, t_column, parameters):
    idxs = []
    for v in value.split('%'):
        if len(v) > 0:
            idxs.append(get_idx_box(v, t_column, parameters))
    return idxs


def get_idx_box(v, t_column, parameters):

    box_lines = parameters.box_lines[t_column.split('.')[0]][t_column.split(
        '.')[1]]
    if (isinstance(box_lines[0], datetime.date)):
        v = datetime.date(int(v.split('-')[0]), int(v.split('-')[1]),
                          int(v.split('-')[2]))
    elif (isinstance(box_lines[0], decimal.Decimal)):
        v = decimal.Decimal(v)
    elif (isinstance(box_lines[0], int)):
        v = int(float(v))
    else:
        if (type(v) != type(box_lines[0])):
            print()
    for i in range(len(box_lines)):
        if v < box_lines[i]:
            return i
    return len(box_lines)


def encode_condition_op(condition_op, relation_name, index_name, parameters):
    if condition_op is None:
        vec = [0 for _ in range(parameters.condition_op_dim)]
    elif condition_op.op_type == 'Bool':
        idx = parameters.bool_ops_id[condition_op.operator]
        vec = [0 for _ in range(parameters.bool_ops_total_num)]
        vec[idx - 1] = 1
    else:
        operator = condition_op.operator
        left_value = condition_op.left_value
        right_value = condition_op.right_value

        if re.match(r'.+\..+', left_value) is None:
            if relation_name is None:
                relation_name = index_name.split(left_value)[1].strip('_')
            left_value = relation_name + '.' + left_value
        else:
            relation_name = left_value.split('.')[0]

        if relation_name not in parameters.tables_id:
            left_value_vec = [0 for _ in range(parameters.column_total_num)]
            operator_idx = parameters.compare_ops_id[operator]
            operator_vec = [0 for _ in range(parameters.compare_ops_total_num)]
            operator_vec[operator_idx - 1] = 1
            right_value_vec = [0]
        else:
            left_value_vec = [0 for _ in range(parameters.column_total_num)]
            left_value_idx = parameters.columns_id[left_value]
            left_value_vec[left_value_idx - 1] = 1

            column_name = left_value.split('.')[1]

            if re.match(r'^[a-z][a-zA-Z0-9_]*\.[a-z][a-zA-Z0-9_]*$', right_value) is not None \
                    and right_value.split('.')[0] in parameters.tables_id:
                operator_idx = parameters.compare_ops_id[operator]
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                right_value_idx = parameters.columns_id[right_value]
                right_value_vec = [0]
                left_value_vec[right_value_idx - 1] = 1
            elif re.match(r'^[a-z][a-zA-Z0-9_]*\.[a-z][a-zA-Z0-9_]*$', right_value) is not None \
                    and right_value.split('.')[0] not in parameters.tables_id:
                operator_idx = parameters.compare_ops_id[operator]
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                right_value_vec = [0]
            elif right_value == 'None':
                operator_idx = parameters.compare_ops_id['!Null']
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                if operator == 'IS':
                    right_value_vec = [1]
                elif operator == '!=':
                    right_value_vec = [0]
                else:
                    print(operator)
                    raise
            elif left_value in parameters.columnTypeisNum:
                try:
                    right_value = float(right_value)
                    right_value_idx = get_idx_box(right_value, left_value,
                                                  parameters)
                    right_value_vec = [0 for _ in range(parameters.box_num)]
                    right_value_vec[right_value_idx] = 1
                except:
                    right_value_vec = [0]

                operator_idx = parameters.compare_ops_id[operator]
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
            elif re.match(r'^__LIKE__', right_value) is not None:
                operator_idx = parameters.compare_ops_id['~~']
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                right_value = right_value.strip('\'')[8:]
                right_value_vec = [0 for _ in range(parameters.box_num)]
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                for idx in right_value_idxs:
                    right_value_vec[idx] = 1
            elif re.match(r'^__NOTLIKE__', right_value) is not None:
                operator_idx = parameters.compare_ops_id['!~~']
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                right_value = right_value.strip('\'')[11:]
                right_value_vec = [0 for _ in range(parameters.box_num)]
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                for idx in right_value_idxs:
                    right_value_vec[idx] = 1
            elif re.match(r'^__NOTEQUAL__', right_value) is not None:
                operator_idx = parameters.compare_ops_id['!=']
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                right_value = right_value.strip('\'')[12:]
                right_value_vec = [0 for _ in range(parameters.box_num)]
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                for idx in right_value_idxs:
                    right_value_vec[idx] = 1
            elif re.match(r'^__ANY__', right_value) is not None:
                operator_idx = parameters.compare_ops_id['=']
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                right_value_vec = [0 for _ in range(parameters.box_num)]

                right_value = right_value.strip('\'')[7:].strip('{}')

                for v in right_value.split(','):
                    v = v.strip('"').strip('\'')
                    if len(v) > 0:
                        right_value_idxs = get_str_representation_box(
                            v, left_value, parameters)
                        for idx in right_value_idxs:
                            right_value_vec[idx] = 1

            else:
                operator_idx = parameters.compare_ops_id[operator]
                operator_vec = [
                    0 for _ in range(parameters.compare_ops_total_num)
                ]
                operator_vec[operator_idx - 1] = 1
                right_value_vec = [0 for _ in range(parameters.box_num)]
                right_value_idxs = get_str_representation_box(
                    right_value, left_value, parameters)
                for idx in right_value_idxs:
                    right_value_vec[idx] = 1

        vec = [0 for _ in range(parameters.bool_ops_total_num)]
        vec = vec + left_value_vec + operator_vec + right_value_vec

    num_pad = parameters.condition_op_dim - len(vec)
    result = np.pad(vec, (0, num_pad), 'constant')

    return result


def encode_condition(condition, relation_name, index_name, parameters):
    if len(condition) == 0:
        vecs = [[0 for _ in range(parameters.condition_op_dim)]]
    else:
        vecs = [
            encode_condition_op(condition_op, relation_name, index_name,
                                parameters) for condition_op in condition
        ]

    return vecs
