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
        ('esd_test_alpha', 0.05, 'The Significance level.'),
        ('gradient_side', "positive", 'The side of anomaly for detector to warn.'),
        ('gradient_max_coef', 1, 'The warning level of the gradient.'),
        ('increasing_side', 'positive', 'The side of anomaly for detector to warn.'),
        ('increasing_alpha', 0.05, 'The Significance level'),  # to be determined
        ('iqr_outliers_1', 3, 'The lower limit for the times of iqr below q1.'),
        ('iqr_outliers_2', 3, 'The upper limit for the times of iqr above q3.'),
        ('level_shift_outliers_1', 'None', 'The lower limit for the times of iqr below q1.'),
        ('level_shift_outliers_2', 6, 'The upper limit for the times of iqr above q3.'),
        ('level_shift_side', 'both', 'The side of anomaly for detetcor to warn.'),
        ('level_shift_window', 5, 'The length of sliding window for aggregation method.'),
        ('level_shift_agg', 'median', 'The aggregation method for the detector.'),
        ('mad_threshold', 3, 'The MAD threshold to decide a anomaly data.'),
        ('mad_scale_factor', 1.4826, 'The relationship between std and mad.'),
        ('quantile_high', 1, 'The upper limit of quantile for quantile detector.'),
        ('quantile_low', 0, 'The lower limit of quantile for quantile detector.'),
        ('seasonal_outliers_1', 'None', 'The lower limit for the times of iqr below q1.'),
        ('seasonal_outliers_2', 3, 'The upper limit for the times of iqr above q3.'),
        ('seasonal_side', 'positive', 'The side of anomaly for detetcor to warn.'),
        ('seasonal_window', 10, 'The length of sliding window for aggregation method.'),
        ('seasonal_period', 'None', 'The given period to skip the period calculation.'),
        ('seasonal_high_ac_threshold', 0.1, 'The parameter to calculate the period.'),
        ('seasonal_min_seasonal_freq', 2, 'The parameter to calculate the period.'),
        ('spike_outliers_1', 'None', 'The lower limit for the times of iqr below q1.'),
        ('spike_outliers_2', 3, 'The upper limit for the times of iqr above q3.'),
        ('spike_side', 'both', 'The side of anomaly for detetcor to warn.'),
        ('spike_window', 1, 'The length of sliding window for aggregation method.'),
        ('spike_agg', 'median', 'The aggregation method for the detector.'),
        ('threshold_high', 'inf', 'The lower threshold.'),
        ('threshold_low', '-inf', 'The upper threshold.'),
        ('threshold_percentage', 'None', 'The warning percentage for outliers.'),
        ('volatility_shift_outliers_1', 'None', 'The lower limit for the times of iqr below q1.'),
        ('volatility_shift_outliers_2', 6, 'The upper limit for the times of iqr above q3.'),
        ('volatility_shift_side', 'both', 'The side of anomaly for detetcor to warn.'),
        ('volatility_shift_window', 10, 'The length of sliding window for aggregation method.'),
        ('volatility_shift_agg', 'std', 'The aggregation method for the detector.'),
    ]}

    detection_threshold = {'detection_threshold': [
        ('connection_usage_threshold', 0.9, ''),
        ('cpu_usage_threshold', 0.6, ''),
        ('cpu_high_usage_percent', 0.8, ''),
        ('data_file_wait_threshold', 100000, ''),
        ('db_memory_rate_threshold', 0.8, "The threshold of memory occupy in gs_total_memory_detail."),
        ('disk_ioutils_threshold', 0.7, ''),
        ('disk_usage_threshold', 0.7, ''),
        ('handler_occupation_threshold', 0.7, ''),
        ('idle_session_occupy_rate_threshold', 0.3, ''),
        ('mem_usage_threshold', 0.6, ''),
        ('network_bandwidth_usage_threshold', 0.8, 'bandwidth usage'),
        ('other_used_memory_threshold', 5120, "The threshold of other_used_memory. Default is 5GB."),
        ('package_drop_rate_threshold', 0.01, ''),
        ('package_error_rate_threshold', 0.01, ''),
        ('thread_pool_usage_threshold', 0.95, ''),
    ]}

    iv_table = {IV_TABLE: [
        ('cipher_s1', '', ''),
        ('cipher_s2', '', '')
    ]}

    slow_sql_threshold = {'slow_sql_threshold': [
        ('analyze_operation_probable_time_interval', 6,
         'The estimated impact time for analyze and vacuum. Unit: second.'),
        ('tuple_number_threshold', 1000,
         'The number of tuples threshold to identify large tables.'),
        ('table_total_size_threshold', 50,
         'The size of table threshold to identify large tables. Unit: MB.'),
        ('fetch_tuples_threshold', 1000,
         'The number of scanned tuples threshold to identify operators with heavy cost.'),
        ('returned_rows_threshold', 1000,
         'The number of returned tuples threshold to identify plans that returns large amount of data.'),
        ('updated_tuples_threshold', 1000,
         'The number of updated tuples threshold to identify batch update statements.'),
        ('deleted_tuples_threshold', 1000,
         'The number of deleted tuples threshold to identify batch delete statements.'),
        ('inserted_tuples_threshold', 1000,
         'The number of inserted tuples threshold to identify batch insert statements.'),
        ('dead_rate_threshold', 0.02,
         'The rate of dead tuples threshold to determine whether a table has too many dead tuples.'),
        ('index_number_threshold', 3,
         'The number of indexes threshold to determine whether a table has too many indexes.'),
        ('nestloop_rows_threshold', 10000,
         'The number of tuples threshold for nestloop operator.'),
        ('large_join_threshold', 50000,
         'The number of tuples threshold for hashjoin operator.'),
        ('cost_rate_threshold', 0.02,
         'The operator cost ratio threshold to identify operators with heavy cost.'),
        ('plan_height_threshold', 10,
         'The height of plan trees threshold to identify complex plan trees.'),
        ('complex_operator_threshold', 2,
         'The number of join and agg operators threshold to identify complex plans.'),
        ('large_in_list_threshold', 50,
         'The number of elements threshold in the in-clause.'),
        ('tuples_diff_threshold', 1000,
         'The difference in number of tuples between statistics and reality threshold '
         'to identify large gap in statistical information.'),
        ('plan_time_rate_threshold', 0.6,
         'The rate of SQL execution plan generation time threshold.'),
        ('sort_rate_threshold', 0.7,
         'The rate of disk-spill rate in SQL execution history threshold')
    ]}

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
    __default__.update(detection_threshold)
    __default__.update(iv_table)
    __default__.update(slow_sql_threshold)
    __default__.update(self_optimization)
    __default__.update(self_monitoring)
