# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
class Parameters():

    def __init__(self, tables_id, columns_id, physic_ops_id, column_total_num,
                 table_total_num, physic_op_total_num, condition_op_dim,
                 compare_ops_id, bool_ops_id, bool_ops_total_num,
                 compare_ops_total_num, box_num, columnTypeisNum, box_lines):
        self.tables_id = tables_id
        self.columns_id = columns_id
        self.physic_ops_id = physic_ops_id
        self.column_total_num = column_total_num
        self.table_total_num = table_total_num
        self.physic_op_total_num = physic_op_total_num
        self.condition_op_dim = condition_op_dim
        self.compare_ops_id = compare_ops_id
        self.bool_ops_id = bool_ops_id
        self.bool_ops_total_num = bool_ops_total_num
        self.compare_ops_total_num = compare_ops_total_num
        self.box_num = box_num
        self.columnTypeisNum = columnTypeisNum
        self.box_lines = box_lines
