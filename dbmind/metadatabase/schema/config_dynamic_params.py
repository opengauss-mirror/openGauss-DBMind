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
        ('disk_usage_threshold', 0.1, ''),
        ('disk_usage_max_coef', 2.5e-08, ''),  # window, 5 minutes
        ('mem_usage_threshold', 0.2, ''),
        ('mem_usage_max_coef', 8e-08, ''),  # window, 5 minutes
        ('cpu_usage_threshold', 0.05, ''),
        ('cpu_high_usage_percent', 0.8, ''),
        ('tps_threshold', 2, ''),
        ('qps_max_coef', 8e-03, ''),  # window, 5 minutes
        ('connection_max_coef', 4e-04, ''),  # window, 5 minutes
        ('p80_threshold', 260, ''),
        ('io_capacity_threshold', 25, ''),
        ('io_delay_threshold', 50, ''),
        ('io_wait_threshold', 0.1, ''),
        ('load_average_threshold', 0.6, ''),
        ('iops_threshold', 2000, ''),
        ('handler_occupation_threshold', 0.7, ''),
        ('disk_ioutils_threshold', 0.7, ''),
        ('connection_rate_threshold', 0.1, ''),
        ('connection_usage_threshold', 0.9, ''),
        ('package_drop_rate_threshold', 0.01, ''),
        ('package_error_rate_threshold', 0.01, ''),
        ('bgwriter_rate_threshold', 0.1, ''),
        ('replication_write_diff_threshold', 100000, ''),
        ('replication_sent_diff_threshold', 100000, ''),
        ('replication_replay_diff_threshold', 1000000, ''),
        ('thread_occupy_rate_threshold', 0.95, ''),
        ('idle_session_occupy_rate_threshold', 0.3, ''),
        ('double_write_file_wait_threshold', 100, ''),
        ('data_file_wait_threshold', 100000, ''),
        ('os_cpu_usage_low', 0, ''),
        ('os_cpu_usage_high', 0.8, ''),
        ('os_cpu_usage_percent', 0.8, ''),
        ('os_mem_usage_low', 0, ''),
        ('os_mem_usage_high', 0.8, ''),
        ('os_mem_usage_percent', 0.8, ''),
        ('os_disk_usage_low', 0, ''),
        ('os_disk_usage_high', 0.8, ''),
        ('os_disk_usage_percent', 0, ''),
        ('io_write_bytes_low', 0, ''),
        ('io_write_bytes_high', 2, ''),
        ('io_write_bytes_percent', 0, ''),
        ('pg_replication_replay_diff_low', 0, ''),
        ('pg_replication_replay_diff_high', 70, ''),
        ('pg_replication_replay_diff_percent', 0, ''),
        ('gaussdb_qps_by_instance_low', 0, ''),
        ('gaussdb_qps_by_instance_high', 100, ''),
        ('gaussdb_qps_by_instance_percent', 0, '')]
    }

    iv_table = {'iv_table': [
        ('cipher_s1', '', ''),
        ('cipher_s2', '', '')
    ]}

    slow_sql_threshold = {'slow_sql_threshold': [
        ('tuple_number_threshold', 5, ''),
        ('table_total_size_threshold', 3, ''),
        ('fetch_tuples_threshold', 1000, ''),
        ('returned_rows_threshold', 1000, ''),
        ('updated_tuples_threshold', 1000, ''),
        ('deleted_tuples_threshold', 1000, ''),
        ('inserted_tuples_threshold', 1000, ''),
        ('hit_rate_threshold', 0.95, ''),
        ('dead_rate_threshold', 0.02, ''),
        ('index_number_threshold', 3, ''),
        ('column_number_threshold', 2, ''),
        ('analyze_operation_probable_time_interval', 6, ''),  # unit is second
        ('max_elapsed_time', 60, ''),
        ('analyze_threshold', 3, ''),  # unit is second
        ('nestloop_rows_threshold', 10000, ''),
        ('large_join_threshold', 10000, ''),
        ('groupagg_rows_threshold', 5000, ''),
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
        ('kill_slow_query', 0, 'Whether to actively check and kill slow query. '
                               'The default elapsed time of a slow query to be killed is 1 minute.')
    ]}

    __default__ = dict()
    __default__.update(detection_params)
    __default__.update(iv_table)
    __default__.update(slow_sql_threshold)
    __default__.update(self_optimization)
    __default__.update(self_monitoring)
