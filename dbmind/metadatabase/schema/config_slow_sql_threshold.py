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
from .. import DynamicConfig


class SlowSQLThreshold(DynamicConfig):
    __tablename__ = "slow_sql_threshold"

    __default__ = {
        'tuple_number_threshold': 5,
        'table_total_size_threshold': 3,
        'fetch_tuples_threshold': 1000,
        'returned_rows_threshold': 1000,
        'updated_tuples_threshold': 1000,
        'deleted_tuples_threshold': 1000,
        'inserted_tuples_threshold': 1000,
        'hit_rate_threshold': 0.95,
        'dead_rate_threshold': 0.02,
        'index_number_threshold': 3,
        'column_number_threshold': 2,
        'analyze_operation_probable_time_interval': 6,  # unit is second
        'max_elapsed_time': 60,
        'analyze_threshold': 3,  # unit is second
        'nestloop_rows_threshold': 10000,
        'large_join_threshold': 10000,
        'groupagg_rows_threshold': 5000,
        'cost_rate_threshold': 0.02,
        'plan_height_threshold': 10,
        'complex_operator_threshold': 2,
        'large_in_list_threshold': 10,
        'tuples_diff_threshold': 1000
    }
