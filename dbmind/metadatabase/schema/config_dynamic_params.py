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


class DynamicParams(DynamicConfig):
    __tablename__ = "dynamic_params"

    detection_params = {'detection_params': [
        ('high_ac_threshold', 0.1, ''),
        ('min_seasonal_freq', 2, ''),
        ('disk_usage_threshold', 0.7, ''),
        ('mem_usage_threshold', 0.6, ''),
        ('cpu_usage_threshold', 0.6, ''),
        ('cpu_high_usage_percent', 0.8, ''),
        ('tps_threshold', 1000, ''),
        ('handler_occupation_threshold', 0.7, ''),
        ('disk_ioutils_threshold', 0.7, ''),
        ('connection_usage_threshold', 0.9, ''),
        ('package_drop_rate_threshold', 0.01, ''),
        ('package_error_rate_threshold', 0.01, ''),
        ('thread_occupy_rate_threshold', 0.95, ''),
        ('idle_session_occupy_rate_threshold', 0.3, ''),
        ('data_file_wait_threshold', 100000, '')]
    }

    iv_table = {'iv_table': [
        ('cipher_s1', '', ''),
        ('cipher_s2', '', '')
    ]}

    slow_sql_threshold = {'slow_sql_threshold': [
        ('tuple_number_threshold', 1000, ''),
        ('table_total_size_threshold', 50, ''),  # unit is MB
        ('fetch_tuples_threshold', 1000, ''),
        ('returned_rows_threshold', 1000, ''),
        ('updated_tuples_threshold', 1000, ''),
        ('deleted_tuples_threshold', 1000, ''),
        ('inserted_tuples_threshold', 1000, ''),
        ('dead_rate_threshold', 0.02, ''),
        ('index_number_threshold', 3, ''),
        ('analyze_operation_probable_time_interval', 6, ''),  # unit is second
        ('nestloop_rows_threshold', 10000, ''),
        ('large_join_threshold', 10000, ''),
        ('cost_rate_threshold', 0.02, ''),
        ('plan_height_threshold', 10, ''),
        ('complex_operator_threshold', 2, ''),
        ('large_in_list_threshold', 10, ''),
        ('tuples_diff_threshold', 1000, ''), ]
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
        ('max_elapsed_time', 0, 'The default elapsed time of a slow query to be killed (second)')
    ]}

    __default__ = dict()
    __default__.update(detection_params)
    __default__.update(iv_table)
    __default__.update(slow_sql_threshold)
    __default__.update(self_optimization)
    __default__.update(self_monitoring)
