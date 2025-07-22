# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
class BanditArm:
    def __init__(self, index_cols, table_name, memory, table_row_count, include_cols=()):
        self.schema_name = 'dbo'
        self.table_name = table_name
        self.index_cols = index_cols
        self.include_cols = include_cols
        if self.include_cols:
            # include_col_hash = hashlib.sha1('_'.join(include_cols).lower().encode()).hexdigest()
            include_col_names = '_'.join(tuple(map(lambda x: x[0:4], include_cols))).lower()
            self.index_name = 'IXN_' + table_name + '_' + '_'.join(index_cols).lower() + '_' + include_col_names
        else:
            self.index_name = 'IX_' + table_name + '_' + '_'.join(index_cols).lower()
        self.index_name = self.index_name[:127]
        self.index_oid = None
        self.memory = memory
        self.table_row_count = table_row_count
        self.name_encoded_context = []
        self.index_usage_last_batch = 0
        self.cluster = None
        self.query_id = None
        self.query_ids = set()
        self.query_ids_backup = set()
        self.is_include = 0
        self.arm_value = {}
        self.clustered_index_time = 0
        self.execute_time = 0

    def __eq__(self, other):
        return self.index_name == other.index_name

    def __hash__(self):
        return hash(self.index_name)

    def __le__(self, other):
        if len(self.index_cols) > len(other.index_cols):
            return False
        else:
            for i in range(len(self.index_cols)):
                if self.index_cols[i] != other.index_cols[i]:
                    return False
            return True

    def __str__(self):
        return self.index_name

    @staticmethod
    def get_arm_id(index_cols, table_name, include_cols=()):
        if include_cols:
            include_col_names = '_'.join(tuple(map(lambda x: x[0:4], include_cols))).lower()
            arm_id = 'IXN_' + table_name + '_' + '_'.join(index_cols).lower() + '_' + include_col_names
        else:
            arm_id = 'IX_' + table_name + '_' + '_'.join(index_cols).lower()
        arm_id = arm_id[:127]
        return arm_id
