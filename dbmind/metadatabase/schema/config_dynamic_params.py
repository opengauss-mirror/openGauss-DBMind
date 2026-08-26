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

from dbmind.metadatabase import DynamicConfigDbBase

# IV table name
IV_TABLE = 'iv_table'


class DynamicParams(DynamicConfigDbBase):
    __tablename__ = "dynamic_params"

    detection_params = {'detection_params': [
        ('esd_test_alpha', 0.05, 'float', 'The Significance level.'),
        ('gradient_side', "positive", 'str', 'The side of anomaly for detector to warn.'),
        ('gradient_max_coef', 1, 'int', 'The warning level of the gradient.'),
        ('increasing_side', 'positive', 'str', 'The side of anomaly for detector to warn.'),
        ('increasing_alpha', 0.05, 'float', 'The Significance level'),  # to be determined
        ('iqr_outliers_1', 3, 'int', 'The lower limit for the times of iqr below q1.'),
        ('iqr_outliers_2', 3, 'int', 'The upper limit for the times of iqr above q3.'),
        ('level_shift_outliers_1', None, 'int', 'The lower limit for the times of iqr below q1.'),
        ('level_shift_outliers_2', 3, 'int', 'The upper limit for the times of iqr above q3.'),
        ('level_shift_side', 'both', 'str', 'The side of anomaly for detector to warn.'),
        ('level_shift_window', 20, 'int', 'The length of sliding window for aggregation method.'),
        ('level_shift_agg', 'median', 'str', 'The aggregation method for the detector.'),
        ('mad_threshold', 3, 'int', 'The MAD threshold to decide a anomaly data.'),
        ('mad_scale_factor', 1.4826, 'float', 'The relationship between std and mad.'),
        ('quantile_high', 1, 'int', 'The upper limit of quantile for quantile detector.'),
        ('quantile_low', 0, 'int', 'The lower limit of quantile for quantile detector.'),
        ('seasonal_outliers_1', None, 'int', 'The lower limit for the times of iqr below q1.'),
        ('seasonal_outliers_2', 3, 'int', 'The upper limit for the times of iqr above q3.'),
        ('seasonal_side', 'positive', 'str', 'The side of anomaly for detector to warn.'),
        ('seasonal_window', 10, 'int', 'The length of sliding window for aggregation method.'),
        ('seasonal_period', None, 'int', 'The given period to skip the period calculation.'),
        ('seasonal_high_ac_threshold', 0.1, 'float', 'The parameter to calculate the period.'),
        ('seasonal_min_seasonal_freq', 2, 'int', 'The parameter to calculate the period.'),
        ('spike_outliers_1', None, 'int', 'The lower limit for the times of iqr below q1.'),
        ('spike_outliers_2', 3, 'int', 'The upper limit for the times of iqr above q3.'),
        ('spike_side', 'both', 'str', 'The side of anomaly for detector to warn.'),
        ('spike_window', 1, 'int', 'The length of sliding window for aggregation method.'),
        ('spike_agg', 'median', 'str', 'The aggregation method for the detector.'),
        ('threshold_high', 'inf', 'float', 'The lower threshold.'),
        ('threshold_low', '-inf', 'float', 'The upper threshold.'),
        ('threshold_percentage', None, 'float', 'The warning percentage for outliers.'),
        ('volatility_shift_outliers_1', None, 'int', 'The lower limit for the times of iqr below q1.'),
        ('volatility_shift_outliers_2', 6, 'int', 'The upper limit for the times of iqr above q3.'),
        ('volatility_shift_side', 'both', 'str', 'The side of anomaly for detector to warn.'),
        ('volatility_shift_window', 10, 'int', 'The length of sliding window for aggregation method.'),
        ('volatility_shift_agg', 'std', 'str', 'The aggregation method for the detector.'),
    ]}

    detection_threshold = {'detection_threshold': [
        ('connection_usage_threshold', 0.9, 'float', 'The alarm threshold for the number of '
                                            'connections and the maximum number of allowed connections.'),
        ('cpu_usage_threshold', 0.8, 'float', 'The alarm threshold of CPU(User) usage.'),
        ('cpu_high_usage_percent', 0.8, 'float', 'The proportion of abnormal CPU usage in the time window.'),
        ('db_memory_rate_threshold', 0.8, 'float', "The threshold of memory occupy in gs_total_memory_detail."),
        ('disk_ioutils_threshold', 0.99, 'float', 'The alarm threshold of IO-Utils.'),
        ('disk_usage_threshold', 0.8, 'float', 'The alarm threshold of disk usage.'),
        ('handler_occupation_threshold', 0.9, 'float', 'The alarm threshold of fds usage.'),
        ('mem_usage_threshold', 0.8, 'float', 'The alarm threshold of memory usage.'),
        ('mem_high_usage_percent', 0.8, 'float', 'The proportion of abnormal memory usage in the time window.'),
        ('network_bandwidth_usage_threshold', 0.8, 'float', 'The alarm threshold of bandwidth usage.'),
        ('other_used_memory_threshold', 5120, 'int', "The alarm threshold of other_used_memory. Default is 5GB."),
        ('package_drop_rate_threshold', 0.01, 'float', 'The alarm threshold of network drop rate.'),
        ('package_error_rate_threshold', 0.01, 'float', 'The alarm threshold of network error rate.'),
        ('thread_pool_usage_threshold', 0.95, 'float', 'The alarm threshold of thread pool usage.'),
        ('disk_await_threshold', 30.0, 'float', 'The alarm threshold of disk await. Default is 30 ms.'),
        ('leaked_fds_threshold', 5, 'int', 'The threshold of leaked fds. Default is 5.'),
        ('connection_rate_threshold', 0.95, 'float', 'The threshold of connection rate. Default is 0.95.'),
        ('ping_lag_threshold', 50.0, 'float', 'The threshold of ping lag. Default is 50 ms.'),
        ('ping_packet_rate_threshold', 0.9, 'float', 'The threshold of ping packet rate. Default is 0.9.'),
        ('significance_threshold', 0.05, 'float', 'The threshold of statistic significance. Default is 0.05.'),
        ('long_transaction_threshold', 3600, 'int', 'The threshold of long transaction thresholde. Default is 3600 seconds.')
    ]}

    # the same as `dbmind.cmd.configs.config_constants`
    iv_table = {IV_TABLE: [
        ('cipher_s1', '', 'str', ''),
        ('cipher_s2', '', 'str', '')
    ]}

    slow_query_threshold = {'slow_query_threshold': [
        ('analyze_operation_probable_time_interval', 6, 'int',
         'The estimated impact time for analyze and vacuum. Unit: second.'),
        ('tuple_number_threshold', 1000, 'int',
         'The number of tuples threshold to identify large tables.'),
        ('table_total_size_threshold', 50, 'int',
         'The size of table threshold to identify large tables. Unit: MB.'),
        ('fetch_tuples_threshold', 1000, 'int',
         'The number of scanned tuples threshold to identify operators with heavy cost.'),
        ('returned_rows_threshold', 1000, 'int',
         'The number of returned tuples threshold to identify plans that returns large amount of data.'),
        ('updated_tuples_threshold', 1000, 'int',
         'The number of updated tuples threshold to identify batch update statements.'),
        ('deleted_tuples_threshold', 1000, 'int',
         'The number of deleted tuples threshold to identify batch delete statements.'),
        ('inserted_tuples_threshold', 1000, 'int',
         'The number of inserted tuples threshold to identify batch insert statements.'),
        ('dead_rate_threshold', 0.02, 'float',
         'The rate of dead tuples threshold to determine whether a table has too many dead tuples.'),
        ('index_number_threshold', 6, 'int',
         'The number of indexes threshold to determine whether a table has too many indexes.'),
        ('nestloop_rows_threshold', 10000, 'int',
         'The number of tuples threshold for nestloop operator.'),
        ('large_join_threshold', 50000, 'int',
         'The number of tuples threshold for hashjoin operator.'),
        ('cost_rate_threshold', 0.1, 'float',
         'The operator cost ratio threshold to identify operators with heavy cost.'),
        ('plan_height_threshold', 25, 'int',
         'The height of plan trees threshold to identify complex plan trees.'),
        ('complex_operator_threshold', 3, 'int',
         'The number of join and agg operators threshold to identify complex plans.'),
        ('large_in_list_threshold', 50, 'int',
         'The number of elements threshold in the in-clause.'),
        ('tuples_diff_threshold', 1000, 'int',
         'The difference in number of tuples between statistics and reality threshold '
         'to identify large gap in statistical information.'),
        ('plan_time_rate_threshold', 0.6, 'float',
         'The rate of SQL execution plan generation time threshold.'),
        ('sort_rate_threshold', 0.7, 'float',
         'The rate of disk-spill rate in SQL execution history threshold'),
        ('large_broadcast_rows_threshold', 10000, 'int',
         'The number of rows threshold to identify large table broadcast in streaming.')
    ]}

    self_monitoring = {'self_monitoring': [
        ('detection_interval_seconds', 600, 'int',
         'The interval for performing health examination on '
         'the database through monitoring metrics.'),
        ('detection_window_seconds', 600, 'int', 'The time for last detection. Unit is second.'),
        ('result_retention_seconds', 604800, 'int',
         'Storage time of metadata database data. If retention is more than the threshold, DBMind will delete them.'),
        ('golden_kpi', 'os_cpu_user_usage, os_mem_usage, os_disk_usage, opengauss_qps_by_instance', 'str',
         'DBMind only measures and detects the golden metrics in the anomaly detection processing.')
    ]}

    self_optimization = {'self_optimization': [
        ('min_improved_rate', 0.3, 'float', 'Minimum improved rate of the cost for advised indexes'),
        ('max_index_num', 10, 'int', 'Maximum number of advised indexes'),
        ('max_index_storage', 1000000, 'int', 'Maximum index storage (Mb)'),
        ('max_template_num', 5000, 'int', 'Maximum number of templates'),
        ('max_reserved_period', 100, 'int', 'Maximum retention time (day)'),
        ('optimization_interval', 86400, 'int', 'The interval for index recommendation (second)'),
        ('max_elapsed_time', 60, 'int', 'The default elapsed time of a slow query to be killed (second)'),
        ('expansion_coefficient', 1.2, 'float', 'The relationship between interval of some timed-task and '
                                                'the fetch-interval in each task during execution (second)')
    ]}

    metadatabase_params = {'metadatabase_params': [
        ('real_time_inspection_retention', 31, 'int', 'Maximum storage duration of real-time inspection results'),
        ('daily_inspection_retention', 400, 'int', 'Maximum storage duration of daily inspection results'),
        ('weekly_inspection_retention', 720, 'int', 'Maximum storage duration of weekly inspection results'),
        ('monthly_inspection_retention', 720, 'int', 'Maximum storage duration of monthly inspection results')
    ]}

    security_metrics = {'security_metrics': [
        ('ratio_conf_factor', 3.5, 'float', 'Confidence factor for ratio comparison'),
        ('z_score_conf_factor', 3.5, 'float', 'Confidence factor for z-score comparison'),
        ('on', 1, 'int', '0 – the feature is off, 1 – the feature is on'),
        ('calibration_training_in_minute', 10080, 'int', 'Number of minutes to use when learning the calibration'),
        ('calibration_forecasting_in_minutes', 1440, 'int',
         'Number of minutes to forecast when learning the calibration'),
        ('re_calibrate_period', 10080, 'int',
         'The time period, in minutes, that calibration need to be re-done'),
        ('detection_training_in_minutes', 1440, 'int',
         'The time period, in minutes, that calibration need to be re-done'),
        ('detection_forecasting_in_minutes', 30, 'int', 'detection forecasting period in minutes'),
        ('model_min_forecast_length_in_minute', 15, 'int',
         'during calibration and detection the time series is being forecast in chunks this size (sliding window), '
         'the same value is used on calibration and detection'),
        ('save_model_outputs', 2, 'int', '0/1/2 flag if to save model forecasting values in a csv file. '
                                  '0 - do not save, 1 - save calibration and anomalies only, 2- save everything'),
        ('scenario_high_alert', 0.8, 'float', 'the default thresholds for critical alert'),
        ('scenario_medium_alert', 0.6, 'float', 'the default thresholds for medium alert'),
        ('scenario_low_alert', 0.2, 'float', 'the default thresholds for low alert'),
        ('opengauss_invalid_logins_rate_lower_bound', 2.0, 'float', ''),
        ('opengauss_log_errors_rate_lower_bound', 2.0, 'float', ''),
        ('opengauss_user_violation_rate_lower_bound', 2.0, 'float', ''),
        ('opengauss_user_locked_rate_lower_bound', 2.0, 'float', ''),
        ('save_model_outputs_disk_size_in_kb', 10000, 'int',
         'Maximum disk space consumption for security metrics troubleshooting csv files.'),
    ]}

    __default__ = dict()
    __default__.update(detection_params)
    __default__.update(detection_threshold)
    __default__.update(iv_table)
    __default__.update(slow_query_threshold)
    __default__.update(self_optimization)
    __default__.update(self_monitoring)
    __default__.update(metadatabase_params)
    __default__.update(security_metrics)
