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
import logging
import time
from collections import defaultdict
from datetime import timedelta, datetime

from dbmind import global_vars, constants
from dbmind.app.diagnosis.entry import diagnose_query
from dbmind.app.diagnosis.query.slow_sql.query_info_source import QueryContextFromTSDBAndRPC
from dbmind.app.monitoring import MUST_BE_DETECTED_METRICS
from dbmind.app.monitoring import detect_history, group_sequences_together, regular_inspection
from dbmind.app.optimization import (need_recommend_index,
                                     do_index_recomm,
                                     recommend_knobs,
                                     TemplateArgs,
                                     get_database_schemas)
from dbmind.common.dispatcher import customized_timer
from dbmind.common.utils import cast_to_int_or_float, NaiveQueue
from dbmind.service import dai

global_vars.self_driving_records = NaiveQueue(20)

index_template_args = TemplateArgs(
    global_vars.configs.getint(
        'SELF-OPTIMIZATION', 'max_reserved_period', fallback=100
    ),
    global_vars.configs.getint(
        'SELF-OPTIMIZATION', 'max_template_num', fallback=5000
    )
)
# get interval of TIMED-TASK
slow_sql_diagnosis_interval = global_vars.configs.getint('TIMED_TASK', 'slow_sql_diagnosis_interval',
                                                         fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
index_recommend_interval = global_vars.configs.getint('TIMED_TASK', 'index_recommend_interval',
                                                      fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
knob_recommend_interval = global_vars.configs.getint('TIMED_TASK', 'knob_recommend_interval',
                                                     fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
self_monitoring_interval = global_vars.configs.getint('TIMED_TASK', 'self_monitoring_interval',
                                                      fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
slow_query_killer_interval = global_vars.configs.getint('TIMED_TASK', 'slow_query_killer_interval',
                                                        fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
discard_expired_results_interval = global_vars.configs.getint('TIMED_TASK', 'discard_expired_results_interval',
                                                              fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
one_day = 24 * 60 * 60  # unit is second
one_week = 7 * one_day  # unit is second
one_month = 30 * one_day  # unit is second

last_detection_minutes = global_vars.dynamic_configs.get_int_or_float(
    'self_monitoring', 'last_detection_time', fallback=600) / 60
how_long_to_forecast_minutes = global_vars.dynamic_configs.get_int_or_float(
    'self_monitoring', 'forecasting_future_time', fallback=3600
) / 60
result_storage_retention = global_vars.dynamic_configs.get_int_or_float(
    'self_monitoring', 'result_storage_retention', fallback=604800
)
optimization_interval = global_vars.dynamic_configs.get_int_or_float(
    'self_monitoring', 'optimization_interval', fallback=86400
)
templates = defaultdict(dict)

"""The Four Golden Signals:
https://sre.google/sre-book/monitoring-distributed-systems/#xref_monitoring_golden-signals
"""
golden_kpi = {'os_cpu_usage', 'os_mem_usage', 'os_disk_usage', 'gaussdb_qps_by_instance'}
golden_kpi |= MUST_BE_DETECTED_METRICS.BUILTIN_GOLDEN_KPI

wrapped_golden_kpi = set((kpi,) for kpi in golden_kpi)
to_be_detected_metrics_for_history = wrapped_golden_kpi | MUST_BE_DETECTED_METRICS.HISTORY
to_be_detected_metrics_for_future = wrapped_golden_kpi | MUST_BE_DETECTED_METRICS.future()


@customized_timer(self_monitoring_interval)
def self_monitoring():
    history_alarms = list()
    expansion_coefficient = 1.5
    # transfer 'second' to 'minute'
    fetch_interval = int(expansion_coefficient * self_monitoring_interval / 60)
    for metrics in to_be_detected_metrics_for_history:
        sequences_list = []
        for metric in metrics:
            latest_sequences = dai.get_latest_metric_sequence(metric, fetch_interval).fetchall()
            logging.debug('The length of latest_sequences is %d and metric name is %s.',
                          len(latest_sequences), metric)

            sequences_list.append(latest_sequences)

        group_list = group_sequences_together(sequences_list, metrics)

        alarms = global_vars.worker.parallel_execute(
            detect_history, ((sequences,) for sequences in group_list)
        ) or []

        logging.debug('The length of detected alarms is %d.', len(alarms))
        history_alarms.extend(alarms)
    # save history alarms
    for alarms in history_alarms:
        if not alarms:
            continue
        dai.save_history_alarms(alarms)
    global_vars.self_driving_records.put(
        {
            'catalog': 'monitoring',
            'msg': 'Completed anomaly detection for KPIs and found %d anomalies.' % len(history_alarms),
            'time': int(time.time() * 1000)
        }
    )


@customized_timer(slow_sql_diagnosis_interval)
def slow_sql_diagnosis():
    # in order to avoid losing slow SQL data, the real 'fetch_interval' is equal to
    # the 'slow_sql_diagnosis_interval * expansion coefficient'
    expansion_coefficient = 1.5
    # transfer 'second' to 'minute'
    fetch_interval = int(expansion_coefficient * slow_sql_diagnosis_interval / 60)
    slow_query_collection = dai.get_all_slow_queries(fetch_interval)
    logging.debug('The length of slow_query_collection is %d.', len(slow_query_collection))
    global_vars.self_driving_records.put(
        {
            'catalog': 'monitoring',
            'msg': 'Completed detection for slow queries and diagnosed %d slow queries.'
                   % len(slow_query_collection),
            'time': int(time.time() * 1000)
        }
    )
    query_contexts = []
    for slow_query in slow_query_collection:
        try:
            with global_vars.agent_proxy.context(slow_query.instance):
                query_contexts.append(
                    (QueryContextFromTSDBAndRPC(slow_query),)
                )
        except global_vars.agent_proxy.RPCAddressError as e:
            logging.warning(
                'Cannot diagnose slow queries because %s.', e
            )
    slow_queries = global_vars.worker.parallel_execute(
        diagnose_query, query_contexts
    ) or []
    dai.save_slow_queries(slow_queries)


@customized_timer(index_recommend_interval)
def index_recommend():
    if not need_recommend_index():
        return
    database_schemas = get_database_schemas()

    args_collection = []
    for address in database_schemas:
        for db_name in database_schemas[address]:
            schema_names = database_schemas[address][db_name]
            args_collection.append(
                (index_template_args, address, db_name,
                 schema_names, templates[db_name], optimization_interval)
            )
    results = global_vars.worker.parallel_execute(
        do_index_recomm, args_collection
    )
    index_infos = []
    for result in results:
        if result is None:
            continue
        index_info, database_templates = result
        if index_info and database_templates:
            index_infos.append(index_info)
            templates.update(database_templates)
    dai.save_index_recomm(index_infos)
    global_vars.self_driving_records.put(
        {
            'catalog': 'optimization',
            'msg': 'Completed index recommendation and generated report.',
            'time': int(time.time() * 1000)
        }
    )


@customized_timer(knob_recommend_interval)
def knob_recommend():
    recommend_knobs_result = recommend_knobs()
    dai.save_knob_recomm(recommend_knobs_result)
    global_vars.self_driving_records.put(
        {
            'catalog': 'optimization',
            'msg': 'Completed knob recommendation.',
            'time': int(time.time() * 1000)
        }
    )


@customized_timer(seconds=slow_query_killer_interval)
def slow_query_killer():
    max_elapsed_time = cast_to_int_or_float(
        global_vars.dynamic_configs.get('self_optimization', 'max_elapsed_time')
    )
    if max_elapsed_time is None or max_elapsed_time < 0:
        logging.warning("Can not actively kill slow SQL, because the "
                        "configuration value 'max_elapsed_time' is invalid.")
        return
    stmt = """
    SELECT datname AS db_name,
           query,
           pg_cancel_backend(pid) AS killed,
           usename AS username,
           extract(epoch
                   FROM now() - xact_start) AS elapsed_time,
           (extract(epoch from now()) * 1000)::bigint AS killed_time
    FROM pg_stat_activity
    WHERE query_id > 0
      AND query IS NOT NULL
      AND length(trim(query)) > 0
      AND elapsed_time >= {};
    """.format(max_elapsed_time)
    for instance_addr, rpc in global_vars.agent_proxy:
        results = rpc.call('query_in_postgres', stmt)
        if len(results) > 0:
            dai.save_killed_slow_queries(instance_addr, results)
            global_vars.self_driving_records.put(
                {
                    'catalog': 'optimization',
                    'msg': 'Automatically killed %d slow queries.' % len(results),
                    'time': int(time.time() * 1000)
                }
            )


@customized_timer(max(discard_expired_results_interval, 60))
def discard_expired_results():
    """Periodic cleanup of not useful diagnostics or predictions"""
    logging.info('Starting to clean up older diagnostics and predictions.')
    try:
        dai.delete_older_result(int(time.time()), result_storage_retention)
        global_vars.self_driving_records.put(
            {
                'catalog': 'vacuum',
                'msg': 'Automatically clean up discarded diagnosis results.',
                'time': int(time.time() * 1000)
            }
        )
    except Exception as e:
        logging.exception(e)
        global_vars.self_driving_records.put(
            {
                'catalog': 'vacuum',
                'msg': 'Failed to clean up discarded diagnosis results due to %s.' % e,
                'time': int(time.time() * 1000)
            }
        )


@customized_timer(seconds=one_day)
def daily_inspection():
    results = []
    end = datetime.now()
    start = end - timedelta(seconds=one_day)
    for instance, rpc in global_vars.agent_proxy:
        inspector = regular_inspection.DailyInspection(instance, start, end)
        report = inspector()
        results.append({'instance': instance,
                        'inspection_type': 'daily_check',
                        'start': int(start.timestamp() * 1000),
                        'end': int(end.timestamp()) * 1000,
                        'report': report,
                        'conclusion': ''})
    dai.save_regular_inspection_results(results)
    global_vars.self_driving_records.put(
        {
            'catalog': 'diagnosis',
            'msg': 'Updated daily inspection report.',
            'time': int(time.time() * 1000)
        }
    )


@customized_timer(seconds=one_week)
def weekly_inspection():
    results = []
    end = datetime.now()
    start = end - timedelta(seconds=one_week)
    for instance, rpc in global_vars.agent_proxy:
        inspector = regular_inspection.MultipleDaysInspection(instance, start, end, history_inspection_limit=7)
        report = inspector()
        results.append({'instance': instance,
                        'inspection_type': 'weekly_check',
                        'start': int(start.timestamp() * 1000),
                        'end': int(end.timestamp()) * 1000,
                        'report': report,
                        'conclusion': ''})
    dai.save_regular_inspection_results(results)


@customized_timer(seconds=one_month)
def monthly_inspection():
    results = []
    end = datetime.now()
    start = end - timedelta(seconds=one_month)
    for instance, rpc in global_vars.agent_proxy:
        inspector = regular_inspection.MultipleDaysInspection(instance, start, end, history_inspection_limit=30)
        report = inspector()
        results.append({'instance': instance,
                        'inspection_type': 'monthly_check',
                        'start': int(start.timestamp() * 1000),
                        'end': int(end.timestamp()) * 1000,
                        'report': report,
                        'conclusion': ''})
    dai.save_regular_inspection_results(results)

