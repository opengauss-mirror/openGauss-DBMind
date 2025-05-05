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

import json
import logging
import time
from datetime import datetime, timedelta

import dbmind.app.diagnosis.security.security_diagnosis as security_diagnosis_module
from dbmind import global_vars, constants
from dbmind.app.diagnosis.query.entry import diagnose_query
from dbmind.app.diagnosis.security.security_metrics_settings import get_security_metrics_settings
from dbmind.app.monitoring import ad_pool_manager
from dbmind.app.monitoring.monitoring_constants import LONG_TERM_METRIC_STATS
from dbmind.app.optimization import recommend_knobs, TemplateArgs
from dbmind.app.timed_task_utils import detect_anomaly, diagnose_cluster_state
from dbmind.cmd.edbmind import init_anomaly_detection_pool
from dbmind.common.dispatcher import customized_timer
from dbmind.common.utils import cast_to_int_or_float, NaiveQueue
from dbmind.components.cluster_diagnosis import cluster_diagnosis, utils
from dbmind.service import dai
from dbmind.service.cluster_info import get_all_cn_dn_ip_set

# get interval of TIMED-TASK
anomaly_detection_interval = global_vars.configs.getint('TIMED_TASK', 'anomaly_detection_interval',
                                                        fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
slow_query_diagnosis_interval = global_vars.configs.getint('TIMED_TASK', 'slow_query_diagnosis_interval',
                                                           fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
knob_recommend_interval = global_vars.configs.getint('TIMED_TASK', 'knob_recommend_interval',
                                                     fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
slow_query_killer_interval = global_vars.configs.getint('TIMED_TASK', 'slow_query_killer_interval',
                                                        fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
CLUSTER_DIAGNOSE_INTERVAL = global_vars.configs.getint('TIMED_TASK', 'cluster_diagnose_interval',
                                                       fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
discard_expired_results_interval = global_vars.configs.getint('TIMED_TASK', 'discard_expired_results_interval',
                                                              fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
AGENT_UPDATE_DETECT_INTERVAL = global_vars.configs.getint('TIMED_TASK', 'agent_update_detect_interval',
                                                          fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
CHECK_SECURITY_METRICS_INTERVAL = global_vars.configs.getint('TIMED_TASK', 'check_security_metrics_interval',
                                                             fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
CALIBRATE_SECURITY_METRICS_INTERVAL = global_vars.configs.getint('TIMED_TASK', 'calibrate_security_metrics_interval',
                                                                 fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
UPDATE_STATISTICS_INTERVAL = global_vars.configs.getint('TIMED_TASK', 'update_statistic_interval',
                                                        fallback=constants.TIMED_TASK_DEFAULT_INTERVAL)
result_retention_seconds = global_vars.dynamic_configs.get_int_or_float(
    'self_monitoring', 'result_retention_seconds', fallback=604800
)

optimization_interval = global_vars.dynamic_configs.get_int_or_float(
    'self_optimization', 'optimization_interval', fallback=86400
)
index_template_args = TemplateArgs(
    global_vars.configs.getint(
        'self_optimization', 'max_reserved_period', fallback=100
    ),
    global_vars.configs.getint(
        'self_optimization', 'max_template_num', fallback=5000
    )
)

# there is a relationship between the interval of some timed-task and
# the fetch-interval in each task during execution, in order to avoid problems
# caused by inconsistencies, we set 'expansion coefficient' to associate them
expansion_coefficient = global_vars.dynamic_configs.get_int_or_float(
    'self_optimization', 'expansion_coefficient', fallback=1.5
)

_self_driving_records = NaiveQueue(20)
_SELF_AGENT_RECORDS = set()
#  Start of self secured configuration
MAX_DATA_POINTS = 10000
MIN_STEP = 15000  # 15 seconds is the lower resolution we use
last_security_scenarios_file_ts = 0
security_scenarios_list = []

_is_calibration_on_now = False  # flag that indicates that metric calibration in in progress


@customized_timer(CALIBRATE_SECURITY_METRICS_INTERVAL)
def calibrate_security_metrics():
    security_metrics_on = get_security_metrics_settings('on')
    if security_metrics_on != 1:
        logging.warning("The self_security setting is off, can not start calibration timer.")
        return

    minutes_back = (
        security_diagnosis_module.calibration_training_in_minutes +
        security_diagnosis_module.calibration_forecasting_in_minutes
    )
    logging.info("Starting calibrating security metrics. The calibration process need %s data", minutes_back)
    global _is_calibration_on_now
    if _is_calibration_on_now:
        metric_hosts_tuple = security_diagnosis_module.get_metrics_that_need_calibration(minutes_back)
        if len(metric_hosts_tuple) == 0:
            _is_calibration_on_now = False
            logging.info("Security metrics calibration is done")
            return
        else:
            logging.info("Security metrics calibrating is in progress, cannot move on")
            return

    metric_hosts_tuple = security_diagnosis_module.get_metrics_that_need_calibration(minutes_back)
    if len(metric_hosts_tuple) == 0:
        logging.info("No calibration is required")
    else:
        _is_calibration_on_now = True
        security_diagnosis_module.calibrate_security_metrics_serial(metric_hosts_tuple)
        _is_calibration_on_now = False
        logging.info("_calibrate_all_security_metrics is done")


@customized_timer(CHECK_SECURITY_METRICS_INTERVAL)
def check_security_metrics():
    """
    Runs the anomaly detection for the security metrics
    """
    security_metrics_on = get_security_metrics_settings('on')
    if security_metrics_on != 1:
        return
    logging.info("In check_security_metrics")
    security_diagnosis_module.load_security_scenarios_list()
    try:
        security_diagnosis_module.find_anomalies_in_security_metrics()
    except Exception as error:
        logging.error("check_security_metrics failed due to exception")
        logging.exception(error)


def get_timed_app_records():
    alerts = list(_self_driving_records)
    alerts.reverse()  # Bring the latest events to the front
    return alerts


@customized_timer(anomaly_detection_interval)
def anomaly_detection():
    long_term_metrics = dai.get_meta_metric_sequence(None, {}, {})  # all
    short_term_detectors = list()
    long_term_detectors = list()
    for detection in ad_pool_manager.get_anomaly_detectors():
        running = detection.get(ad_pool_manager.DetectorParam.RUNNING)
        detector = detection.get(ad_pool_manager.DetectorParam.DETECTOR)
        if running and detector:
            if detector.duration != 0:
                short_term_detectors.append((detector, None))
            else:
                long_term_detectors.append((detector, long_term_metrics))

    short_term_alarms = global_vars.worker.parallel_execute(
        detect_anomaly, short_term_detectors
    ) or []
    dai.save_history_alarms(short_term_alarms, detection_interval=anomaly_detection_interval)

    long_term_alarms = global_vars.worker.parallel_execute(
        detect_anomaly, long_term_detectors
    ) or []
    dai.save_history_alarms(long_term_alarms, detection_interval=anomaly_detection_interval)

    num_of_alarms = sum([len(l) for l in short_term_alarms + long_term_alarms])
    logging.debug('The length of detected alarms is %d.', num_of_alarms)
    _self_driving_records.put(
        {
            'catalog': 'anomaly_detection',
            'msg': 'Completed anomaly detection and found %d anomalies.' % num_of_alarms,
            'time': int(time.time() * 1000)
        }
    )


@customized_timer(slow_query_diagnosis_interval)
def slow_query_diagnosis():
    # in order to avoid losing slow SQL data, the real 'fetch_interval' is equal to
    # the 'slow_query_diagnosis_interval * expansion coefficient'
    fetch_interval = int(expansion_coefficient * slow_query_diagnosis_interval / 60)
    slow_query_collection = dai.get_all_slow_queries(fetch_interval)
    logging.debug('The length of slow_query_collection is %d.', len(slow_query_collection))
    _self_driving_records.put(
        {
            'catalog': 'monitoring',
            'msg': 'Completed detection for slow queries and diagnosed %d slow queries.'
                   % len(slow_query_collection),
            'time': int(time.time() * 1000)
        }
    )
    slow_queries = []
    for slow_query in slow_query_collection:
        slow_queries.append((slow_query,))
    diagnosed_slow_queries = global_vars.worker.parallel_execute(
        diagnose_query, slow_queries
    ) or []
    dai.save_slow_queries(diagnosed_slow_queries)


@customized_timer(knob_recommend_interval)
def knob_recommend():
    recommend_knobs_result = recommend_knobs()
    dai.save_knob_recomm(recommend_knobs_result)
    _self_driving_records.put(
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
           pg_catalog.pg_cancel_backend(pid) AS killed,
           usename AS username,
           extract(epoch
                   FROM pg_catalog.now() - xact_start) AS elapsed_time,
           (extract(epoch from pg_catalog.now()) * 1000)::bigint AS killed_time
    FROM pg_catalog.pg_stat_activity
    WHERE query_id > 0
      AND query IS NOT NULL
      AND pg_catalog.length(trim(query)) > 0
      AND elapsed_time >= {};
    """.format(max_elapsed_time)
    for instance_addr, rpc in global_vars.agent_proxy:
        results = rpc.call('query_in_postgres', stmt)
        if len(results) > 0:
            dai.save_killed_slow_queries(instance_addr, results)
            _self_driving_records.put(
                {
                    'catalog': 'optimization',
                    'msg': 'Automatically killed %d slow queries.' % len(results),
                    'time': int(time.time() * 1000)
                }
            )


@customized_timer(CLUSTER_DIAGNOSE_INTERVAL)
def cluster_diagnose():
    """cluster diagnose"""
    end_datetime = datetime.now()
    start_datetime = end_datetime - timedelta(minutes=cluster_diagnosis.WINDOW_IN_MINUTES)

    cn_dn_ip_set = get_all_cn_dn_ip_set()

    cluster_diagnose_params_set = set()
    for role in utils.ANSWER_ORDERS:
        for instance in cn_dn_ip_set[role]:
            for method in utils.METHOD:
                cluster_diagnose_params_set.add((instance, role, start_datetime, end_datetime, method,))

    diagnosis_record = global_vars.worker.parallel_execute(
        diagnose_cluster_state, cluster_diagnose_params_set
    ) or []

    logging.debug('The length of cluster diagnosis record is %d.', len(diagnosis_record))
    if diagnosis_record:
        dai.save_history_cluster_diagnosis(diagnosis_record)
        _self_driving_records.put(
            {
                'catalog': 'cluster_diagnosis',
                'msg': 'Completed cluster diagnosis and found %d diagnosis results.' % len(diagnosis_record),
                'time': int(time.time() * 1000)
            }
        )


@customized_timer(max(discard_expired_results_interval, 60))
def discard_expired_results():
    """Periodic cleanup of not useful diagnostics or predictions"""
    logging.info('Starting to clean up older diagnostics and predictions.')
    try:
        dai.delete_older_result(int(time.time()), result_retention_seconds)
        _self_driving_records.put(
            {
                'catalog': 'vacuum',
                'msg': 'Automatically clean up discarded diagnosis results.',
                'time': int(time.time() * 1000)
            }
        )
    except Exception as e:
        logging.exception(e)
        _self_driving_records.put(
            {
                'catalog': 'vacuum',
                'msg': 'Failed to clean up discarded diagnosis results due to %s.' % e,
                'time': int(time.time() * 1000)
            }
        )


@customized_timer(AGENT_UPDATE_DETECT_INTERVAL)
def agent_update_detect():
    """To detect whether agents list is updated. If so, init anomaly detectors pool."""
    agents = global_vars.agent_proxy.agent_get_all()
    acquired_clusters = set()
    for primary, node_list in agents.items():
        nodes = node_list.copy()
        if primary not in nodes:
            nodes.append(primary)

        acquired_clusters.add(json.dumps(sorted(nodes)))

    if acquired_clusters != _SELF_AGENT_RECORDS:
        logging.info('Agents update detected, automatically updating anomaly detectors.')
        init_anomaly_detection_pool()

        _SELF_AGENT_RECORDS.clear()
        _SELF_AGENT_RECORDS.update(acquired_clusters)

        _self_driving_records.put(
            {
                'catalog': 'agents_update',
                'msg': 'Agents update detected, anomaly detectors are updated',
                'time': int(time.time() * 1000)
            }
        )


@customized_timer(UPDATE_STATISTICS_INTERVAL)
def update_statistics():
    """To update the metric statistics"""
    now = int(time.time())
    for metric_name, metric_stat in LONG_TERM_METRIC_STATS.items():
        length = metric_stat["length"]
        step = metric_stat["step"]
        dai.update_metric_stats(now, metric_name, length, step)
    logging.info('Metric statistics have updated.')
