# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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

from dbmind.metadatabase import DynamicConfig
from dbmind.cmd.configs.config_constants import IV_TABLE


class DynamicParams(DynamicConfig):
    __tablename__ = "dynamic_params"

    detection_params = {'detection_params': [
        ('high_ac_threshold', 0.1, ''),
        ('min_seasonal_freq', 2, ''),
        ('disk_usage_threshold', 0.7, ''),
        ('mem_usage_threshold', 0.6, ''),
        ('cpu_usage_threshold', 0.6, ''),
        ('cpu_high_usage_percent', 0.8, ''),
        ('handler_occupation_threshold', 0.7, ''),
        ('disk_ioutils_threshold', 0.7, ''),
        ('connection_usage_threshold', 0.9, ''),
        ('package_drop_rate_threshold', 0.01, ''),
        ('package_error_rate_threshold', 0.01, ''),
        ('thread_pool_usage_threshold', 0.95, ''),
        ('idle_session_occupy_rate_threshold', 0.3, ''),
        ('data_file_wait_threshold', 100000, ''),
        ('network_bandwidth_usage_threshold', 0.8, 'bandwidth usage'),
        ('db_memory_rate_threshold', 0.8, "The threshold of memory occupy in gs_total_memory_detail."),
        ('other_used_memory_threshold', 5120, "The threshold of other_used_memory. Default is 5GB.")]
    }

    iv_table = {IV_TABLE: [
        ('cipher_s1', '', ''),
        ('cipher_s2', '', '')
    ]}

    slow_sql_threshold = {'slow_sql_threshold': [
        ('tuple_number_threshold', 1000, 'The number of tuples, using to judge large table.'),
        ('table_total_size_threshold', 50, 'Unit is MB. the size of table, using to judge large table.'),  # unit is MB
        ('fetch_tuples_threshold', 1000, 'The scan tuples of operator, using to judge heavy cost scan operator.'),
        ('returned_rows_threshold', 1000, 'The returned tuples of plan, using to judge whether it is an plan '
                                          'that returns a large amount of data.'),
        ('updated_tuples_threshold', 1000, 'The number of rows updated by the statement, '
                                           'using to determine whether it is a batch update statement.'),
        ('deleted_tuples_threshold', 1000, 'The number of rows deleted by the statement, '
                                           'using to determine whether it is a batch delete statement.'),
        ('inserted_tuples_threshold', 1000, 'The number of rows updated by the statement, '
                                            'using to determine whether it is a batch update statement.'),
        ('dead_rate_threshold', 0.02, 'The rate of dead tuple, '
                                      'using to judge whether there are too many dead tuples in the table.'),
        ('index_number_threshold', 3, 'The number of indexes in the table, '
                                      'using to judge whether there are too many indexes in the table.'),
        ('analyze_operation_probable_time_interval', 6, 'Unit is second. '
                                                        'The estimated length of time impact for analyze and vacuum.'),
        ('nestloop_rows_threshold', 10000, 'The row number threshold suitable for nestloop operator.'),
        ('large_join_threshold', 50000, 'The row number threshold suitable for hashjoin operator.'),
        ('cost_rate_threshold', 0.02, 'The operator cost ratio threshold, which exceeds the threshold, '
                                      'is considered to be an operator with a heavy cost.'),
        ('plan_height_threshold', 10, 'The height of  plan tree, '
                                      'plan tree exceeding this height are considered more complex.'),
        ('complex_operator_threshold', 2, 'The number of join and agg operator, '
                                          'plans containing many such operators are considered complex.'),
        ('large_in_list_threshold', 50, 'The number of elements in the in-clause.'),
        ('tuples_diff_threshold', 1000, 'The difference between the statistical value of the number of '
                                        'table tuples and the real value, '
                                        'using to judge whether the statistical information gap is too large.'),
        ('plan_time_rate_threshold', 0.6, 'rate of SQL execution plan generation time'),
        ('sort_rate_threshold', 0.7, 'threshold of disk-spill rate in SQL execution history')]
    }

    self_monitoring = {'self_monitoring': [
        ('detection_interval', 600,
         'Unit is second. The interval for performing health examination on the openGauss through monitoring metrics.'),
        ('last_detection_time', 600, 'Unit is second. The time for last detection.'),
        ('forecasting_future_time', 3600,
         'Unit is second. How long the KPI in the future for forecasting. '
         'Meanwhile, this is the period for the forecast.'),
        ('result_storage_retention', 604800,
         'Unit is second. How long should the results retain? '
         'If retention is more than the threshold, DBMind will delete them.'),
        ('golden_kpi', 'os_cpu_usage, os_mem_usage, os_disk_usage, gaussdb_qps_by_instance',
         'DBMind only measures and detects the golden metrics in the anomaly detection processing.')
    ]}

    self_optimization = {'self_optimization': [
        ('min_improved_rate', 0.3, 'Minimum improved rate of the cost for advised indexes'),
        ('max_index_num', 10, 'Maximum number of advised indexes'),
        ('max_index_storage', 1000, 'Maximum index storage (Mb)'),
        ('max_template_num', 5000, 'Maximum number of templates'),
        ('max_reserved_period', 100, 'Maximum retention time (day)'),
        ('optimization_interval', 86400, 'The interval for index recommendation (second)'),
        ('max_elapsed_time', 60, 'The default elapsed time of a slow query to be killed (second)'),
        ('expansion_coefficient', 1.2, 'The relationship between interval of some timed-task and the fetch-interval '
                                       'in each task during execution (second)')
    ]}

    __default__ = dict()
    __default__.update(detection_params)
    __default__.update(iv_table)
    __default__.update(slow_sql_threshold)
    __default__.update(self_optimization)
    __default__.update(self_monitoring)
