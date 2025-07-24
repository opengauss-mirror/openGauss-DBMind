# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
from src.plan_encoding.encoding_predicates import *


def encode_sample(sample):
    return np.array([int(i) for i in sample])


def bitand(sample1, sample2):
    return np.minimum(sample1, sample2)


def encode_node_job(node, parameters):

    extra_info_num = parameters.column_total_num
    operator_vec = np.array([0 for _ in range(parameters.physic_op_total_num)])
    extra_info_vec = np.array([0 for _ in range(extra_info_num)])
    condition_vec = [np.array([0 for _ in range(parameters.condition_op_dim)])]

    has_condition = 0

    if node != None:
        operator = node.node_type
        operator_idx = parameters.physic_ops_id[operator]
        operator_vec[operator_idx - 1] = 1

        if operator == 'Materialize' or operator == 'BitmapAnd' or operator == 'BitmapOr' or operator == 'Hash' or operator == 'Result' or operator == 'Memoize':
            pass
        elif operator == 'Sort' or operator == 'Merge Append':
            for key in node.sort_keys:
                if key in parameters.columns_id:
                    extra_info_inx = parameters.columns_id[key]
                    extra_info_vec[extra_info_inx - 1] = 1
        elif operator == 'Hash Join' or operator == 'Merge Join' or operator == 'Nested Loop':
            condition_vec = encode_condition(node.condition, None, None,
                                             parameters)
        elif operator == 'Aggregate' or operator == 'Group':
            for key in node.group_keys:
                if key in parameters.columns_id:
                    extra_info_inx = parameters.columns_id[key]
                    extra_info_vec[extra_info_inx - 1] = 1
        elif operator == 'Seq Scan' or operator == 'Bitmap Heap Scan' or operator == 'Index Scan'\
                or operator == 'Bitmap Index Scan' or operator == 'Index Only Scan' or operator == 'CTE Scan':
            relation_name = node.relation_name
            index_name = node.index_name
            if relation_name is not None and relation_name in parameters.tables_id:
                extra_info_inx = parameters.tables_id[relation_name]
                extra_info_vec[extra_info_inx - 1] = 1
            if len(node.condition_filter) != 0:
                condition_vec = encode_condition(node.condition_filter,
                                                 relation_name, index_name,
                                                 parameters)
            if len(node.condition_index) != 0:
                condition2_vec = encode_condition(node.condition_index,
                                                  relation_name, index_name,
                                                  parameters)
                condition_vec += condition2_vec

            if hasattr(node, 'bitmap'):
                has_condition = 1
            if hasattr(node, 'bitmap_filter'):
                has_condition = 1
            if hasattr(node, 'bitmap_index'):
                has_condition = 1

    return operator_vec, extra_info_vec, condition_vec, has_condition
