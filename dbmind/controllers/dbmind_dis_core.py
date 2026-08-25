# Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
"""The DBMind v2 interface, only available when DBMind is started as a microservice.
"""

from typing import Dict, Union, List, Optional
from pydantic import BaseModel

from dbmind.common.http import request_mapping, standardized_api_output
from dbmind.common.utils.checking import ParameterChecker
from dbmind.service.web import data_transformer

latest_version = 'v2'
api_prefix = '/%s/api' % latest_version


class ConnectionInfo(BaseModel):
    host: str
    port: str
    user: str
    pwd: str
    sqls: list = None


@request_mapping(api_prefix + '/app/slow-sql-rca', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    query={"type": ParameterChecker.STRING, "optional": True},
    db_name={"type": ParameterChecker.NAME, "optional": False},
    advise={"type": ParameterChecker.STRING, "optional": True},
    schema_name={"type": ParameterChecker.STRING, "optional": True},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    finish_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    template_id={"type": ParameterChecker.DIGIT, "optional": True},
    debug_query_id={"type": ParameterChecker.DIGIT, "optional": True},
    n_soft_parse={"type": ParameterChecker.UINT2, "optional": True},
    n_hard_parse={"type": ParameterChecker.UINT2, "optional": True},
    query_plan={"type": ParameterChecker.STRING, "optional": True},
    n_returned_rows={"type": ParameterChecker.UINT2, "optional": True},
    n_tuples_fetched={"type": ParameterChecker.UINT2, "optional": True},
    n_tuples_returned={"type": ParameterChecker.UINT2, "optional": True},
    n_tuples_inserted={"type": ParameterChecker.UINT2, "optional": True},
    n_tuples_updated={"type": ParameterChecker.UINT2, "optional": True},
    n_tuples_deleted={"type": ParameterChecker.UINT2, "optional": True},
    n_blocks_fetched={"type": ParameterChecker.UINT2, "optional": True},
    n_blocks_hit={"type": ParameterChecker.UINT2, "optional": True},
    db_time={"type": ParameterChecker.UINT2, "optional": True},
    cpu_time={"type": ParameterChecker.UINT2, "optional": True},
    parse_time={"type": ParameterChecker.UINT2, "optional": True},
    plan_time={"type": ParameterChecker.UINT2, "optional": True},
    data_io_time={"type": ParameterChecker.UINT2, "optional": True},
    hash_spill_count={"type": ParameterChecker.UINT2, "optional": True},
    sort_spill_count={"type": ParameterChecker.UINT2, "optional": True},
    n_calls={"type": ParameterChecker.PINT32, "optional": False},
    lock_wait_time={"type": ParameterChecker.UINT2, "optional": True},
    lwlock_wait_time={"type": ParameterChecker.UINT2, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True}
)
@standardized_api_output
def execute_slow_sql_rca(
        connection_info: ConnectionInfo = None,
        query: str = None,
        advise: str = None,
        db_name: str = None,
        schema_name: str = 'public',
        start_time: int = None,
        finish_time: int = None,
        template_id: str = None,
        debug_query_id: str = None,
        n_soft_parse: int = 0,
        n_hard_parse: int = 0,
        query_plan: str = None,
        n_returned_rows: int = 0,
        n_tuples_fetched: int = 0,
        n_tuples_returned: int = 0,
        n_tuples_inserted: int = 0,
        n_tuples_updated: int = 0,
        n_tuples_deleted: int = 0,
        n_blocks_fetched: int = 0,
        n_blocks_hit: int = 0,
        db_time: int = 0,
        cpu_time: int = 0,
        parse_time: int = 0,
        plan_time: int = 0,
        data_io_time: int = 0,
        hash_spill_count: int = 0,
        sort_spill_count: int = 0,
        n_calls: int = 1,
        lock_wait_time: int = 0,
        lwlock_wait_time: int = 0,
        tz: str = None
):
    """
    To obtain the root cause analysis result of slow SQL statement
    :param connection_info: The DB instance connection info.
    :param query: The text of slow SQL statement.
    :param advise: the 'advise' from DFX analysis
    :param db_name: The database of slow SQL statement.
    :param schema_name: The schema of slow SQL statement.
    :param start_time: The start time of slow SQL statement.
    :param finish_time: The finish time of slow SQL statement.
    :param template_id: The unique SQL id of slow SQL statement.
    :param debug_query_id: The debug SQL id of slow SQL statement.
    :param n_soft_parse: the soft parse number of slow SQL statement
    :param n_hard_parse: the hard parse number of slow SQL statement
    :param query_plan: The query plan of slow SQL statement.
    :param n_returned_rows: the returned tuples of slow SQL statement
    :param n_tuples_fetched: the 'randomly scan' tuples of slow SQL statement
    :param n_tuples_returned: the 'sequentially scan' tuples of slow SQL statement
    :param n_tuples_inserted: the inserted tuples of slow SQL statement
    :param n_tuples_updated: the updated tuples of slow SQL statement
    :param n_tuples_deleted: the deleted tuples of slow SQL statement
    :param n_blocks_fetched: the number of block accesses of the buffers
    :param n_blocks_hit: the number of buffers block hits
    :param db_time: the db_time of slow SQL statement
    :param cpu_time: the cpu_time of slow SQL statement
    :param parse_time: the parse_time of slow SQL statement
    :param plan_time: the plan_time of slow SQL statement
    :param data_io_time: the data_io_time of slow SQL statement
    :param hash_spill_count: the hash_spill_count of slow SQL statement
    :param sort_spill_count: the sort_spill_count of slow SQL statement
    :param n_calls: the call number of slow SQL statement
    :param lock_wait_time: the lock_wait_time of slow SQL statement
    :param lwlock_wait_time: the lwlock_wait_time of slow SQL statement
    :param tz: The slow SQL related time zones, only support 'UTC'.
    :return: The RCA result of the specified slow SQL statement
        e.g. {"data":["Seq Scan on t1  (cost=0.00..1868.00 rows=100 width=70)\n",
              [[["1. HEAVY_SCAN_OPERATOR: (1.00) Existing expensive seq scans. Detail:..."]]...]],"success":true}
    """
    params = {
        'query': query,
        'advise': advise,
        'db_name': db_name,
        'schema_name': schema_name.split(',')[-1] if ',' in schema_name else schema_name,
        'start_time': start_time,
        'finish_time': finish_time,
        'template_id': template_id,
        'debug_query_id': debug_query_id,
        'n_soft_parse': n_soft_parse,
        'n_hard_parse': n_hard_parse,
        'query_plan': query_plan,
        'n_returned_rows': n_returned_rows,
        'n_tuples_fetched': n_tuples_fetched,
        'n_tuples_returned': n_tuples_returned,
        'n_tuples_inserted': n_tuples_inserted,
        'n_tuples_updated': n_tuples_updated,
        'n_tuples_deleted': n_tuples_deleted,
        'n_blocks_fetched': n_blocks_fetched,
        'n_blocks_hit': n_blocks_hit,
        'db_time': db_time,
        'cpu_time': cpu_time,
        'parse_time': parse_time,
        'plan_time': plan_time,
        'data_io_time': data_io_time,
        'hash_spill_count': hash_spill_count,
        'sort_spill_count': sort_spill_count,
        'n_calls': n_calls,
        'lock_wait_time': lock_wait_time,
        'lwlock_wait_time': lwlock_wait_time,
        'tz': tz
    }
    return data_transformer.toolkit_slow_query_rca_dis(connection_info, **params)


@request_mapping(api_prefix + '/app/correlation', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    metric_name={"type": ParameterChecker.NAME, "optional": False},
    instance={"type": ParameterChecker.INSTANCE, "optional": False},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": False},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": False},
    metric_filter={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def execute_correlation_analysis(
        metric_name: str = None, instance: str = None, start_time: int = None,
        end_time: int = None, metric_filter: str = None
):
    """
    To obtain the correlation analysis results of abnormal metric.
    :param metric_name: The name of the metric to be analyzed.
    :param instance: The instance where abnormal metric from.
    :param start_time: The abnormal start time of metric.
    :param end_time: The abnormal end time of metric.
    :param metric_filter: The filter to indicate the metric to be analyzed.
        e.g. {\"from_instance\":\"127.0.0.1\",\"from_job\":\"node_exporter\",\"instance\":\"127.0.0.1:8181\",
              \"job\":\"reprocessing_exporter\"}
    :return: The correlation analysis result.
        e.g. {"data":{"os_mem_usage from ip":[["os_mem_usage from ip",1.0,0,[0.32950820234487...]]...]},"success":true}
    """
    return data_transformer.get_correlation_result(metric_name, instance, start_time, end_time,
                                                   metric_filter=metric_filter)


@request_mapping(api_prefix + '/app/risk-analysis/{metric}', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    metric={"type": ParameterChecker.NAME, "optional": False},
    instance={"type": ParameterChecker.INSTANCE, "optional": False},
    warning_hours={"type": ParameterChecker.PINT32, "optional": False},
    labels={"type": ParameterChecker.STRING, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True},
    upper={"type": ParameterChecker.FLOAT, "optional": True},
    lower={"type": ParameterChecker.FLOAT, "optional": True}
)
@standardized_api_output
def execute_risk_analysis(
        metric: str, instance: str = None, warning_hours: int = 1, upper: float = None,
        lower: float = None, labels: str = None, tz: str = None
):
    """
    To obtain the risk analysis result of the specified metric.
    :param metric: The metric name to be implemented risk analysis.
    :param instance: The instance where abnormal metric from.
    :param warning_hours: The length of risk analysis, unit is hours.
    :param upper: The upper limit of the metric.
    :param lower: The lower limit of the metric.
    :param labels: The labels for metric, used to precisely find metric.
    :param tz: The timezone information, example: UTC-8, UTC+8.
    :return: The risk analysis result.
        e.g. {"data":{},"success":true}
    """
    return data_transformer.risk_analysis(metric, instance, warning_hours, upper=upper, lower=lower,
                                          labels=labels, tz=tz)


@request_mapping(api_prefix + '/app/workload-collection', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    data_source={"type": ParameterChecker.STRING, "optional": True},
    databases={"type": ParameterChecker.STRING, "optional": True},
    schemas={"type": ParameterChecker.STRING, "optional": True},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    db_users={"type": ParameterChecker.STRING, "optional": True},
    sql_types={"type": ParameterChecker.STRING, "optional": True},
    template_id={"type": ParameterChecker.DIGIT, "optional": True},
    duration={"type": ParameterChecker.INT32, "optional": True}
)
@standardized_api_output
def execute_workload_collection(
        connection_info: ConnectionInfo = None, data_source: str = None, databases: str = None,
        schemas: str = None, start_time: int = None, end_time: int = None, db_users: str = None,
        sql_types: str = None, template_id: str = None, duration: int = 60
):
    """
    To obtain workloads from different data source.
    :param connection_info: The DB instance connection info.
    :param data_source: The data source of workloads, choice=('asp', 'statement_history', 'pg_stat_activity').
    :param databases: The database lists of the workloads.
    :param schemas: The schema list of the workloads.
    :param start_time: The start timestamp of the collection interval.
    :param end_time: The end timestamp of the collection interval.
    :param db_users: The user list of the workloads.
    :param sql_types: The sql type list of workloads, choice=('SELECT', 'UPDATE', 'DELETE', 'INSERT').
    :param template_id: The unique_sql_id of SQL template.
    :param duration: The SQL execution time of workloads.
    :return: The collection result of the workload data
        e.g. {"data":{"header":["user_name","db_name","schema_name","application_name","unique_query_id","start_time",
              "finish_time","duration","n_returned_rows","n_tuples_fetched","n_tuples_returned","n_tuples_inserted",
              "n_tuples_updated","n_tuples_deleted","n_blocks_fetched","n_blocks_hit","n_soft_parse","n_hard_parse",
              "db_time","cpu_time","parse_time","plan_time","data_io_time","lock_wait_time","lwlock_wait_time","query"],
              "rows":[["user1","db1","public"...]...]},"success":true}
    """
    return data_transformer.collect_workloads_dis(connection_info, data_source, databases,
                                                  schemas, start_time, end_time, db_users, sql_types,
                                                  template_id, duration=duration)


@request_mapping(api_prefix + '/summary/alarms', methods=['GET'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": True},
    current={"type": ParameterChecker.UINT2, "optional": True},
    instance={"type": ParameterChecker.INSTANCE, "optional": False},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    metric_name={"type": ParameterChecker.NAME, "optional": True},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    anomaly_type={"type": ParameterChecker.NAME, "optional": True},
    group={"type": ParameterChecker.BOOL, "optional": True}
)
@standardized_api_output
def query_alarms(
        pagesize: int = 20, current: int = 0, instance: str = None, alarm_type: str = None,
        alarm_level: str = None, metric_name: str = None, start_at: int = None, end_at: int = None,
        anomaly_type: str = None, group: bool = False
):
    """
    To obtain the historical alarms list.
    :param pagesize: The number of records per page, which is a positive integer.
    :param current: Current page number, which is a non-negative integer.
    :param instance: The instance of alarms belongs to.
    :param alarm_type: The type of the alarm, choice=('SYSTEM', 'SLOW_QUERY', 'ALARM_LOG', 'ALARM', 'SECURITY',
     'PERFORMANCE').
    :param alarm_level: The level of the alarm, choice=('CRITICAL', 'FATAL', 'ERROR', 'WARNING', 'WARN', 'INFO',
     'NOTICE', 'DEBUG', 'NOTSET').
    :param metric_name: The name of the metric.
    :param start_at: The start timestamp when obtaining alarms.
    :param end_at: The end timestamp when obtaining alarms.
    :param anomaly_type: The type of the anomaly detection.
    :param group: Whether to group alarms by alarm type and content.
    :return: The list of historical alarms
        e.g. {"data":{"header":["history_alarm_id","instance","metric_name","metric_filter","alarm_type","alarm_level",
              "start_at","end_at","alarm_content","extra_info","anomaly_type"],
              "rows":[[65,"ip","os_mem_usage",null,"SYSTEM",30,1684762097001,1684762547001,"mem_usage_spike_detector:
              Find obvious spikes in memory usage.",null,"Spike"]]},"success":true}
    """
    return data_transformer.get_history_alarms(pagesize, current, instance, alarm_type, alarm_level, metric_name,
                                               start_at, end_at, anomaly_type, group)


@request_mapping(api_prefix + '/summary/alarms/count', methods=['GET'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": False},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    metric_name={"type": ParameterChecker.NAME, "optional": True},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    anomaly_type={"type": ParameterChecker.NAME, "optional": True},
    group={"type": ParameterChecker.BOOL, "optional": True}
)
@standardized_api_output
def query_alarms_count(
        instance: str = None, alarm_type: str = None, alarm_level: str = None, metric_name: str = None,
        start_at: int = None, end_at: int = None, anomaly_type: str = None, group: bool = False
):
    """
    To obtain the number of historical alarms.
    :param instance: The instance of alarms belongs to.
    :param alarm_type: The type of the alarm, choice=('SYSTEM', 'SLOW_QUERY', 'ALARM_LOG', 'ALARM', 'SECURITY',
     'PERFORMANCE').
    :param alarm_level: The level of the alarm, choice=('CRITICAL', 'FATAL', 'ERROR', 'WARNING', 'WARN', 'INFO',
     'NOTICE', 'DEBUG', 'NOTSET').
    :param metric_name: The name of the metric.
    :param start_at: The start timestamp when obtaining alarms.
    :param end_at: The end timestamp when obtaining alarms.
    :param anomaly_type: The type of the anomaly detection.
    :param group: Whether to group alarms by alarm type and content.
    :return: The number of historical alarms
        e.g. {"data":30,"success":true}
    """
    return data_transformer.get_history_alarms_count(instance, alarm_type, alarm_level, metric_name,
                                                     start_at, end_at, anomaly_type, group)


@request_mapping(api_prefix + '/summary/cluster-diagnosis', methods=['GET'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": True},
    current={"type": ParameterChecker.UINT2, "optional": True},
    instance={"type": ParameterChecker.INSTANCE, "optional": False},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    cluster_role={"type": ParameterChecker.NAME, "optional": True},
    diagnosis_method={"type": ParameterChecker.NAME, "optional": True},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    is_normal={"type": ParameterChecker.BOOL, "optional": True}
)
@standardized_api_output
def query_cluster_diagnosis(
        pagesize: int = 20, current: int = 0, instance: str = None, start_at: int = None,
        end_at: int = None, cluster_role: str = None, diagnosis_method: str = None,
        alarm_type=None, alarm_level=None, is_normal: bool = True
):
    """
    To obtain the historical cluster diagnosis list.
    :param pagesize: The number of records per page, which is a positive integer.
    :param current: Current page number, which is a non-negative integer.
    :param instance: The instance of the cluster diagnosis record belongs to.
    :param start_at: The start timestamp of the query interval.
    :param end_at: The end timestamp of the query interval.
    :param cluster_role: The role of instance for diagnosis, choice=('cn', 'dn').
    :param diagnosis_method: The model cluster diagnosis applies, choice=('logical', 'tree').
    :param alarm_type: The type of alarm, choice=('SYSTEM', 'SLOW_QUERY', 'ALARM_LOG', 'ALARM', 'SECURITY',
     'PERFORMANCE').
    :param alarm_level: The level of alarm, choice=('CRITICAL', 'FATAL', 'ERROR', 'WARNING', 'WARN', 'INFO',
     'NOTICE', 'DEBUG', 'NOTSET').
    :param is_normal: Whether contains normal data.
    :return: The list of historical cluster diagnosis record
        e.g. {"data":{"header":["diagnosis_id","instance","timestamp","cluster_role","diagnosis_method",
              "cluster_feature","diagnosis_result", "status_code", "alarm_type", "alarm_level"],
              "rows":[[65,"ip",1684762547001,"dn","logical",{"bind_ip_failed":0,"cms_phonydead_restart":0,
              "cms_restart_pending":0,"dn_disk_damage":0,"dn_manual_stop":0,"dn_nic_down":0,"dn_ping_standby":0,
              "dn_port_conflict":0,"dn_read_only":0,"dn_status":0,"dn_writable":0,"ffic_updated":0,"ping":1},
              "DN down/disconnection"]]},"success":true}
    """
    return data_transformer.get_history_cluster_diagnosis(
        pagesize=pagesize,
        current=current,
        instance=instance,
        start_at=start_at,
        end_at=end_at,
        cluster_role=cluster_role,
        diagnosis_method=diagnosis_method,
        alarm_type=alarm_type,
        alarm_level=alarm_level,
        is_normal=is_normal
    )


@request_mapping(api_prefix + '/summary/cluster-diagnosis/count', methods=['GET'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": False},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    cluster_role={"type": ParameterChecker.NAME, "optional": True},
    diagnosis_method={"type": ParameterChecker.NAME, "optional": True},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    is_normal={"type": ParameterChecker.BOOL, "optional": True}
)
@standardized_api_output
def query_cluster_diagnosis_count(
        instance: str = None, start_at: int = None, end_at: int = None, cluster_role: str = None,
        diagnosis_method: str = None, alarm_type=None, alarm_level=None, is_normal: bool = True
):
    """
    To obtain the number of historical cluster diagnosis records.
    :param instance: The instance of the cluster diagnosis record belongs to.
    :param start_at: The start timestamp of the query interval.
    :param end_at: The end timestamp of the query interval.
    :param cluster_role: The role of instance for diagnosis, choice=('cn', 'dn').
    :param diagnosis_method: The model cluster diagnosis applies, choice=('logical', 'tree').
    :param alarm_type: The type of alarm, choice=('SYSTEM', 'SLOW_QUERY', 'ALARM_LOG', 'ALARM', 'SECURITY',
     'PERFORMANCE').
    :param alarm_level: The level of alarm, choice=('CRITICAL', 'FATAL', 'ERROR', 'WARNING', 'WARN', 'INFO',
     'NOTICE', 'DEBUG', 'NOTSET').
    :param is_normal: Whether contains normal data.
    :return: The number of historical cluster diagnosis records
        e.g. {"data":30,"success":true}
    """
    return data_transformer.get_history_cluster_diagnosis_count(
        instance=instance,
        start_at=start_at,
        end_at=end_at,
        cluster_role=cluster_role,
        diagnosis_method=diagnosis_method,
        alarm_type=alarm_type,
        alarm_level=alarm_level,
        is_normal=is_normal
    )


@request_mapping(api_prefix + '/app/index-recommendation', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    max_index_num={"type": ParameterChecker.PINT32, "optional": True},
    max_index_storage={"type": ParameterChecker.PINT32, "optional": True},
    database={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def execute_index_recommendation(
        database: str, max_index_num: int, max_index_storage: int, connection_info: ConnectionInfo = None
):
    """
    To obtain the list of recommended indexes.
    :param database: The database which index belong to.
    :param max_index_num: Max number of index advised. If this value is not a valid positive integer, it will be
    replaced with the default value.
    :param max_index_storage: Max storage usage of index advised. If this value is not a valid positive integer,
    it will be replaced with the default value.
    :param sqls: The text of sql.
    :param connection_info: The DB instance connection info.
    :return: The list of recommended indexes
        e.g. {"data":[{"advise_indexes":[],"redundant_indexes":[],"total":0,"useless_indexes":
              [{"columns":"c1","schemaName":"public","statement":"DROP INDEX t1_c1_idx;","tbName":"t1","type":3},
              {"columns":"c2","schemaName":"public","statement":"DROP INDEX t2_c2_idx;","tbName":"t2","type":3}]},{}],
              "success":true}
    """
    return data_transformer.toolkit_index_advise_dis(connection_info, database, max_index_num, max_index_storage)


@request_mapping(api_prefix + '/summary/database-list', methods=['GET'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": False}
)
@standardized_api_output
def query_db_list(instance: str):
    """
    To obtain the list of databases under the given instance.
    :param instance: The given instance to be queried.
    :return: The list of the database
        e.g. {"data":["db1","db2","db3","db4","db5"...],"success":true}
    """
    return data_transformer.get_database_list_dis(instance)


@request_mapping(api_prefix + '/app/metric-diagnosis', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    metric_name={"type": ParameterChecker.NAME, "optional": False},
    metric_filter={"type": ParameterChecker.STRING, "optional": True},
    alarm_cause={"type": ParameterChecker.STRING, "optional": True},
    start={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end={"type": ParameterChecker.TIMESTAMP, "optional": True},
    rca_params={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def execute_metric_diagnosis(metric_name: str, metric_filter: str, alarm_cause: str, start: int, end: int,
                             rca_params=None):
    """
    To obtain the metric diagnosis result.
    :param metric_name: The specified metric to be diagnosed.
    :param metric_filter: The filter to indicate the very metric to be diagnosed.
        e.g. {\"from_instance\":\"127.0.0.1\",\"from_job\":\"node_exporter\",\"instance\":\"127.0.0.1:8181\",
              \"job\":\"reprocessing_exporter\"}
    :param alarm_cause:  The pre-set root cause for the anomaly
        e.g. [\"high_cpu_usage\"]
    :param start: The start time for diagnosis in timestamp(ms).
    :param end: The end time for diagnosis in timestamp(ms).
    :param rca_params: The pre-set params for rca
        e.g. '{\"slow_sql\": [{\"params\": {\"low\": -Infinity, \"high\": 100}}]}'
    :return: The metric diagnosis result.
        e.g. {"data":{[{'reason1': 0.0, 'reason2': 1.0}, 'conclusion', 'advice']},"success":true}
    """
    return data_transformer.get_root_cause_analysis(metric_name, metric_filter, start, end, alarm_cause=alarm_cause,
                                                    rca_params=rca_params)[:3]


@request_mapping(api_prefix + '/summary/sql-trace', methods=['GET'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    instance_id={"type": ParameterChecker.STRING, "optional": True},
    labels={"type": ParameterChecker.STRING, "optional": True},
    is_online={"type": ParameterChecker.BOOL, "optional": False},
    from_timestamp={"type": ParameterChecker.TIMESTAMP, "optional": True},
    to_timestamp={"type": ParameterChecker.TIMESTAMP, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True}
)
@standardized_api_output
def query_sql_trace(
        instance_id: str, labels: str, is_online: bool, from_timestamp: int = None,
        to_timestamp: int = None, tz: str = None
):
    """
    To obtain the sql trace result.
    :param instance_id: The specified instance_id.
    :param labels: The parameter that represents a specific label to filter the sequence.
    :param is_online: The specified data source.
    :param from_timestamp: The query start timestamp.
    :param to_timestamp: The query end timestamp.
    :param tz: The query related time zones, only support 'UTC'.
    :return: The sql trace query result.
        e.g. {"data": [{"all_time": 3477357, "application_name": "xxx", "client_addr": "", "client_port"": 0,
              "component_id": "xxx", "db_name": "xxx", "execution_time_details": {"kernel_time":
              {"all_time"": 3477357, "kernel_time_details": {"execution_time": 3466386, "other_time": 4958,
              "parse_time": 453, "plan_time": 5244, "rewrite_time": 316}}, "resource_time": {"all_time": 3477357,
              "resource_time_details": {"cpu_time": 98756, "data_io_time": 0, "other_time": 3378601}},
              "wait_event_time": {"code_wait_event_time": {"all_time": 3477357, "code_wait_event_time_details": {
              "events": [{"event_name": "xxx", "event_time": 16810}, {"event_name": "xxx", "event_time": 5330},
              {"event_name": "xxx", "event_time": 5195}, {"event_name": "xxx", "event_time": 197}], "left_time": 0,
              "other_time": 3449825}}, "resource_wait_event_time": {"all_time": 3477357, "other_time": 3477357,
              "resource_wait_event_time_details": {"data_io_time": {"all_time": 0, "data_io_time_details": {
              "events": [{"event_name": "xxx", "event_time": 742}], "left_time": 0, "other_time": -27532}},
              "lock_time": {"all_time": 0, "lock_time_details": {"events": [], "left_time": 0, "other_time": 0}},
              "lwlock_time": {"all_time": 0, "lwlock_time_details": {"events": [], "left_time": 0, "other_time": 0}}}}}
              }, "finish_time": "2023-08-01 17:50:04.923701+08", "node_id": "0", "schema_name": "user,public",
              "session_id": 1658590, "sql_exec_id": 72339069024193250, "sql_id": 1951225884, "start_time":
              "2023-08-01 17:50:01.446446+08", "trace_id": "0", "transaction_id": "0", "user_name": "user"}],
              "success": true}
    """
    return data_transformer.get_sql_trace(instance_id, is_online, labels, from_timestamp, to_timestamp, tz)


@request_mapping(api_prefix + '/summary/metric-unit', methods=['GET'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    metric={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def query_metric_unit(metric: str):
    """
    To obtain the unit of the given metric.
    :param metric: The given metric to be queried.
    :return: The unit of the given metric.
        e.g. {'data':{'en':'s','cn':'秒'},'success':true}
    """
    return data_transformer.get_metric_unit(metric)


@request_mapping(api_prefix + '/app/metric-diagnosis-insight', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    metric_name={"type": ParameterChecker.NAME, "optional": False},
    metric_filter={"type": ParameterChecker.STRING, "optional": True},
    alarm_cause={"type": ParameterChecker.STRING, "optional": True},
    start={"type": ParameterChecker.TIMESTAMP, "optional": False},
    end={"type": ParameterChecker.TIMESTAMP, "optional": False}
)
@standardized_api_output
def execute_metric_diagnosis_insight(
        metric_name: str, metric_filter: str, alarm_cause: str, start: int, end: int,
        connection_info: ConnectionInfo = None
):
    """
    To obtain addition information of metric-diagnosis.
    :param metric_name: The specified metric to be diagnosed.
    :param metric_filter: The filter to indicate the very metric to be diagnosed.
        e.g. {\"from_instance\":\"127.0.0.1\",\"from_job\":\"node_exporter\",\"instance\":\"127.0.0.1:8181\",
              \"job\":\"reprocessing_exporter\"}
    :param alarm_cause: The pre-set root cause for the anomaly.
        e.g. [\"high_cpu_usage\"]
    :param start: The start time for diagnosis in timestamp(ms).
    :param end: The end time for diagnosis in timestamp(ms).
    :param connection_info: The DB instance connection info.
    :return: The metric diagnosis result
        e.g. {"data":{[{'reason1': 0.0, 'reason2': 1.0}, 'conclusion', 'advice']},"success":true}
    """
    return data_transformer.get_insight_dis(connection_info, metric_name, metric_filter, alarm_cause, start, end)


class DetectorInfo(BaseModel):
    metric_name: str
    detector_name: str
    metric_filter: Optional[Dict[str, Optional[str]]]
    detector_kwargs: Dict[
        str,
        Optional[
            Union[
                float,
                List[Optional[float]],
                str
            ]
        ]
    ]


class AlarmInfo(BaseModel):
    alarm_type: str
    alarm_level: str
    alarm_content: Optional[str]
    alarm_cause: Optional[str]
    extra: Optional[str]


class TimeTaskConfig(BaseModel):
    detector_name: str = None
    duration: int = 600
    alarm_info: AlarmInfo = None
    detector_info: List[DetectorInfo] = None
    cn: List[str] = None
    dn: List[str] = None


@request_mapping(api_prefix + '/app/timed-tasks', methods=['POST'], api=True)
@data_transformer.mode_check
@ParameterChecker.define_rules(
    end={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def execute_time_task(task_name: str, time_task_config: TimeTaskConfig = None):
    """
    To execute a single time task for given instance.
    :param task_name: The name of the task to be launched.
    :param time_task_config: The corresponding configs of the given task.
    :return: The execution result.
        e.g. {'data':{'msg': 'The task xxx has been successfully executed for 127.0.0.1'}, 'success':true}
    """
    return data_transformer.dispatch_time_task_dis(task_name, time_task_config)
