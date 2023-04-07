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
"""This is only a template file that helps
 users implement the web interfaces for DBMind.
 And some implementations are only demonstrations.
"""
import time
from typing import Dict, Union, List, Optional

from pydantic import BaseModel

from dbmind import global_vars
from dbmind.common.http import request_mapping, OAuth2
from dbmind.common.http import standardized_api_output
from dbmind.service.web import context_manager
from dbmind.service.web import data_transformer

latest_version = 'v1'
api_prefix = '/%s/api' % latest_version


class DBMindOauth2(OAuth2):
    token_url = '/api/token'

    def __init__(self):
        super().__init__(pwd_checker=self._password_checker)

        # add the controller rule
        request_mapping(
            DBMindOauth2.token_url, methods=['POST'], api=True
        )(self.login_handler)

    @staticmethod
    def get_dbmind_oauth_instance():
        oauth = OAuth2.get_instance(
            token_url=DBMindOauth2.token_url,
            pwd_checker=DBMindOauth2._password_checker,
        )
        oauth.__instance__ = DBMindOauth2()

        return oauth.__instance__

    @staticmethod
    def _password_checker(username, password, scopes):
        # the parameter scopes here indicate the corresponding
        # instance address if there are more than one clusters
        # are recorded into the TSDB.
        return data_transformer.check_credential(username, password, scopes)

    def before_hook(self, *args, **kwargs):
        super().before_hook(*args, **kwargs)
        if self.scopes:
            scope = self.scopes[0]
            global_vars.agent_proxy.switch_context(scope)
        else:
            # If not specified and there is only one RPC, use it.
            agent_list = global_vars.agent_proxy.agent_get_all()
            if len(agent_list) != 1:
                return
            global_vars.agent_proxy.switch_context(list(agent_list.keys())[0])
        instances = global_vars.agent_proxy.current_cluster_instances()
        ip_list = [i.split(':')[0] for i in instances]
        params = {
            context_manager.ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: instances,
            context_manager.ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ip_list,
            context_manager.ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT:
                global_vars.agent_proxy.current_agent_addr(),
            context_manager.ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX:
                '|'.join(map(lambda s: s + ':?.*', ip_list))
        }
        context_manager.set_access_context(
            **params
        )

    def after_hook(self, *args, **kwargs):
        super().after_hook(*args, **kwargs)
        global_vars.agent_proxy.switch_context(None)  # clear the RPC state
        params = {
            context_manager.ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: None,
            context_manager.ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: None,
            context_manager.ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT: None,
            context_manager.ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX: None
        }
        context_manager.set_access_context(
            **params
        )


oauth2 = DBMindOauth2.get_dbmind_oauth_instance()


@request_mapping('/api/list/agent', methods=['GET'], api=True)
@request_mapping(api_prefix + '/agent/list', methods=['GET'], api=True)
@standardized_api_output
def get_all_agents():
    return data_transformer.get_all_agents()


@request_mapping(api_prefix + '/agent/update', methods=['GET'], api=True)
@standardized_api_output
def update_agents():
    return data_transformer.update_agent_list(force=False)


@request_mapping(api_prefix + '/agent/update/force', methods=['GET'], api=True)
@standardized_api_output
def update_agents_force():
    return data_transformer.update_agent_list(force=True)


@request_mapping('/api/status/running', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_running_status():
    return data_transformer.get_running_status()


@request_mapping('/api/status/transaction', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_xact_status():
    return data_transformer.get_xact_status()


@request_mapping('/api/status/alert', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_alert():
    return data_transformer.get_latest_alert()


@request_mapping('/api/status/node', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_node_status():
    return data_transformer.get_cluster_node_status()


@request_mapping('/api/status/instance', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_instance_status():
    return data_transformer.get_instance_status()


@request_mapping('/api/summary/cluster', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_cluster_summary():
    return data_transformer.get_cluster_summary()


@request_mapping('/api/list/metric', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_all_metrics():
    return data_transformer.get_all_metrics()


@request_mapping('/api/sequence/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_metric_sequence(name: str, start: int = None, end: int = None, step: int = None):
    return data_transformer.get_metric_sequence(name, start, end, step)


@request_mapping('/api/latest-sequence/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_latest_metric_sequence(name: str, instance: str = None, latest_minutes: int = None,
                               step: int = None, fetch_all: bool = False,
                               regrex: bool = False, labels: str = None,
                               regrex_labels: str = None):
    return data_transformer.get_latest_metric_sequence(name, instance, latest_minutes,
                                                       step=step, fetch_all=fetch_all,
                                                       regrex=regrex, labels=labels,
                                                       regrex_labels=regrex_labels)


@request_mapping('/api/alarm/history', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_history_alarms(pagesize: int = 20, current: int = 0,
                       instance: str = None, alarm_type: str = None, alarm_level: str = None, group: bool = False):
    return data_transformer.get_history_alarms(pagesize, current, instance, alarm_type, alarm_level, group)


@request_mapping('/api/alarm/history_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_history_alarms_count(instance: str = None, alarm_type: str = None, alarm_level: str = None,
                             group: bool = False):
    return data_transformer.get_history_alarms_count(instance, alarm_type, alarm_level, group)


@request_mapping('/api/workload_forecasting/sequence/forecast/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def workload_forecasting_forecast(name: str, start: int = None, end: int = None, step: int = None):
    return data_transformer.get_metric_forecast_sequence(name, start, end, step)


@request_mapping('/api/alarm/future', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_future_alarms(pagesize: int = 20, current: int = 0,
                      instance: str = None, metric_name: str = None, start: int = None, group: bool = False):
    return data_transformer.get_future_alarms(pagesize, current, instance, metric_name, start, group)


@request_mapping('/api/alarm/future_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_future_alarms_count(instance: str = None, metric_name: str = None, start: int = None, group: bool = False):
    return data_transformer.get_future_alarms_count(instance, metric_name, start, group)


@request_mapping('/api/alarm/healing', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_healing_info_for_alarms(pagesize: int = 20, current: int = 0, instance: str = None,
                                action: str = None, success: bool = None, min_occurrence: int = None):
    return data_transformer.get_healing_info(pagesize, current, instance, action, success, min_occurrence)


@request_mapping('/api/alarm/healing_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_healing_info_count_for_alarms(instance: str = None, action: str = None, success: bool = None,
                                      min_occurrence: int = None):
    return data_transformer.get_healing_info_count(instance, action, success, min_occurrence)


@request_mapping('/api/query/slow/recent', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_recent_slow_queries(pagesize: int = 20, current: int = 0, instance: str = None, query: str = None,
                            start: int = None, end: int = None, group: bool = False):
    return data_transformer.get_slow_queries(pagesize, current, instance, query, start, end, group)


@request_mapping('/api/query/slow/killed', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_killed_slow_queries(pagesize: int = 20, current: int = 0, instance: str = None, query: str = None,
                            start: int = None, end: int = None):
    return data_transformer.get_killed_slow_queries(pagesize, current, instance, query, start, end)


@request_mapping('/api/query/slow/killed_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_killed_slow_queries_count(instance: str = None, query: str = None, start: int = None, end: int = None):
    return data_transformer.get_killed_slow_queries_count(instance, query, start, end)


@request_mapping('/api/query/top', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_top_queries():
    username, password = oauth2.credential
    return data_transformer.get_top_queries(username, password)


@request_mapping('/api/query/active', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_active_queries():
    username, password = oauth2.credential
    return data_transformer.get_active_query(username, password)


@request_mapping('/api/query/locking', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_locking_queries():
    username, password = oauth2.credential
    return data_transformer.get_holding_lock_query(username, password)


@request_mapping('/api/list/database', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_db_list():
    return data_transformer.get_database_list()


@request_mapping('/api/summary/index_advisor', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_index_advisor_summary(positive_pagesize: int = 20, positive_current: int = 20,
                              existing_pagesize: int = 20, existing_current: int = 20):
    return data_transformer.get_index_advisor_summary(positive_pagesize, positive_current,
                                                      existing_pagesize, existing_current)


@request_mapping('/api/summary/get_existing_indexes', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_existing_indexes(pagesize: int = 20, current: int = 0):
    return data_transformer.get_existing_indexes(pagesize, current)


@request_mapping('/api/summary/existing_indexes_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_existing_indexes_count():
    return data_transformer.get_existing_indexes_count()


@request_mapping('/api/summary/get_advised_indexes', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_advised_indexes():
    return data_transformer.get_advised_index()


@request_mapping('/api/summary/get_positive_sql', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_positive_sql(pagesize: int = 20, current: int = 0):
    return data_transformer.get_positive_sql(pagesize, current)


@request_mapping('/api/summary/positive_sql_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_positive_sql_count():
    return data_transformer.get_positive_sql_count()


@request_mapping('/api/summary/knob_tuning', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_tuning_summary(metricpagesize: int = 20, metriccurrent: int = 0,
                            warningpagesize: int = 20, warningcurrent: int = 0,
                            knobpagesize: int = 20, knobcurrent: int = 0):
    return data_transformer.toolkit_recommend_knobs_by_metrics(metricpagesize, metriccurrent,
                                                               warningpagesize, warningcurrent,
                                                               knobpagesize, knobcurrent)


@request_mapping('/api/summary/get_knob_recommendation_snapshot', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_snapshot(pagesize: int = 20, current: int = 0):
    return data_transformer.get_knob_recommendation_snapshot(pagesize, current)


@request_mapping('/api/summary/knob_recommendation_snapshot_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_snapshot_count():
    return data_transformer.get_knob_recommendation_snapshot_count()


@request_mapping('/api/summary/get_knob_recommendation_warnings', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_warnings(pagesize: int = 20, current: int = 0):
    return data_transformer.get_knob_recommendation_warnings(pagesize, current)


@request_mapping('/api/summary/knob_recommendation_warnings_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_warnings_count():
    return data_transformer.get_knob_recommendation_warnings_count()


@request_mapping('/api/summary/get_knob_recommendation', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation(pagesize: int = 20, current: int = 0):
    return data_transformer.get_knob_recommendation(pagesize, current)


@request_mapping('/api/summary/knob_recommendation_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_count():
    return data_transformer.get_knob_recommendation_count()


@request_mapping('/api/summary/slow_query', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_slow_query_summary(pagesize: int = 20, current: int = 0):
    return data_transformer.get_slow_query_summary(pagesize, current)


@request_mapping('/api/query/slow/recent_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_slow_query_count(instance: str = None, distinct: bool = False, query: str = None,
                         start_time: str = None, end_time: str = None, group: bool = False):
    return data_transformer.get_slow_queries_count(instance, distinct, query, start_time, end_time, group)


@request_mapping('/api/summary/slow_query/projection', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_security_summary():
    return {
        'dbname': {
            'x': [],
            'y': []
        }
    }


@request_mapping('/api/summary/security', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_security_summary(pagesize: int = 20, current: int = 0, instance: str = None):
    return data_transformer.get_security_alarms(pagesize, current, instance)


@request_mapping('/api/summary/log', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_log_information():
    import datetime

    text = """\
    testr postgres [local] 140714833868544 0[0:0#0]  0 [BACKEND] FATAL:  Invalid username/password,login denied.
    sectest postgres [local] 140714865325824 0[0:0#0]  2251799813687293 [BACKEND] ERROR:  SQL_INJECTION: TAUTOLOGY
    """
    lines = text.splitlines()
    timestamp = int(time.time()) - len(lines) * 10
    for i, line in enumerate(lines):
        lines[i] = datetime.datetime.fromtimestamp(timestamp + i * 10).strftime('%Y-%m-%d %H:%M:%S.%f') + \
                   ' ' + line.strip()
    return '\n'.join(lines)


@request_mapping('/api/toolkit/advise/index', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def advise_indexes(pagesize: int, current: int, instance: str, database: str,
                   max_index_num: int, max_index_storage: int, sqls: list):
    return data_transformer.toolkit_index_advise(current, pagesize, instance, database, sqls, max_index_num, max_index_storage)


@request_mapping('/api/toolkit/advise/default/value', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def advise_indexes():
    return data_transformer.toolkit_index_advise_default_value()


@request_mapping('/api/toolkit/advise/query', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def tune_query(database: str, sql: str,
               instance: str,
               use_rewrite: bool = True,
               use_hinter: bool = True,
               use_materialized: bool = True):
    return data_transformer.toolkit_rewrite_sql(instance, database, sql)


@request_mapping('/api/toolkit/predict/query', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def sqldiag(database: str, sql: str):
    return {
        'cost_time': 1.1,
        'coordinate': [1.1, 2.1]
    }


@request_mapping('/api/setting/set', methods=['POST', 'GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def set_setting(config: str, name: str, value: str, dynamic: bool = True):
    if dynamic:
        if '' in (config.strip(), name.strip(), value.strip()):
            raise Exception('You should input correct setting.')
        global_vars.dynamic_configs.set(config, name, value)
        return 'success'
    else:
        raise Exception('Currently, DBMind cannot modify the static configurations. '
                        'You should modify the configuration by using the command line or'
                        ' text editor.')


@request_mapping('/api/setting/get', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_setting(config: str, name: str, dynamic: bool = True):
    if dynamic:
        return global_vars.dynamic_configs.get(config, name)
    else:
        raise Exception("Currently, DBMind doesn't support showing the static "
                        "configurations due to security. Instead, you should "
                        "log in to the deployment machine and "
                        "see the configuration file.")


@request_mapping('/api/setting/list', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def list_setting():
    return {'dynamic': global_vars.dynamic_configs.list()}


class SlowSQLItem(BaseModel):
    sql: str
    database: str
    schemaname: str
    start_time: str
    end_time: str
    wdr: str


@request_mapping('/api/toolkit/slow_sql_rca', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def diagnosis_slow_sql(item: SlowSQLItem):
    sql = item.sql if len(item.sql) else None
    database = item.database if len(item.database) else None
    schema = item.schemaname if len(item.schemaname) else 'public'
    start_time = item.start_time if len(item.start_time) else None
    end_time = item.end_time if len(item.end_time) else None
    return data_transformer.toolkit_slow_sql_rca(sql,
                                                 dbname=database,
                                                 schema=schema,
                                                 start_time=start_time,
                                                 end_time=end_time)


@request_mapping('/api/summary/regular_inspections', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_regular_inspections(inspection_type: str = None):
    return data_transformer.get_regular_inspections(inspection_type)


@request_mapping('/api/summary/regular_inspections_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_regular_inspections_count(inspection_type: str = None):
    return data_transformer.get_regular_inspections_count(inspection_type)


@request_mapping('/api/summary/correlation_result', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_correlation_result(metric_name: str = None, instance: str = None,
                           start_time: str = None, end_time: str = None,
                           metric_filter: str = None):
    return data_transformer.get_correlation_result(metric_name, instance,
                                                   start_time, end_time,
                                                   metric_filter=metric_filter)


@request_mapping('/api/toolkit/memory_check', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def memory_check(start_time: int = 0, end_time: int = 0):
    return data_transformer.check_memory_context(start_time, end_time)


@request_mapping('/api/app/timed-task/status', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_timed_task_status():
    return data_transformer.get_timed_task_status()


@request_mapping('/api/toolkit/risk-analysis/{metric}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def risk_analysis(metric: str, instance: str = None, warning_hours: int = 1, upper: str = None,
                  lower: str = None, labels: str = None):
    return data_transformer.risk_analysis(metric, instance, warning_hours, upper=upper, lower=lower, labels=labels)


@request_mapping('/api/collection/status', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_collection_system_status():
    return data_transformer.get_collection_system_status()


@request_mapping('/api/anomaly_detection/defaults', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_detector_init_defaults():
    return data_transformer.get_detector_init_defaults()


class AlarmInfo(BaseModel):
    alarm_type: str
    alarm_level: str
    alarm_content: Optional[str]
    alarm_cause: Optional[str]
    extra: Optional[str]


class DetectorInfo(BaseModel):
    metric_name: str
    detector_name: str
    metric_filter: Dict[str, str]
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


class JsonDict(BaseModel):
    running: int
    duration: int
    forecasting_seconds: int
    alarm_info: AlarmInfo
    detector_info: List[DetectorInfo]


@request_mapping('/api/anomaly_detection/detectors/{name}/addition', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def add_detector(name, json_dict: JsonDict):
    return data_transformer.add_detector(name, json_dict.dict())


@request_mapping('/api/anomaly_detection/detectors/{name}/deletion', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def del_detector(name):
    return data_transformer.delete_detector(name)


@request_mapping('/api/anomaly_detection/detectors/{name}/pause', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def pause_detector(name):
    return data_transformer.pause_detector(name)


@request_mapping('/api/anomaly_detection/detectors/{name}/resumption', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def resume_detector(name):
    return data_transformer.resume_detector(name)


@request_mapping('/api/anomaly_detection/detectors/{name}/view', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def view_detector(name):
    return data_transformer.view_detector(name)


@request_mapping('/api/anomaly_detection/detectors/reconstruction', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def rebuild_detector():
    return data_transformer.rebuild_detector()


@request_mapping('/api/anomaly_detection/detectors/clearance', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def clear_detector():
    return data_transformer.clear_detector()


@request_mapping('/api/data-directory/status', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_database_data_directory_status(instance: str, latest_minutes: int = 5):
    return data_transformer.get_database_data_directory_status(instance, latest_minutes)


@request_mapping('/api/overview', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_front_oveview(latest_minutes: int = 3):
    return data_transformer.get_front_overview(latest_minutes=latest_minutes)


@request_mapping('/api/instance/status', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_current_instance_status():
    return data_transformer.get_current_instance_status()


@request_mapping('/api/agent/status', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_agent_status():
    return data_transformer.get_agent_status()


@request_mapping('/api/workloads/collect', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def collect_workloads(data_source: str = None, databases: str = None, schemas: str = None, start_time: int = None, end_time: int = None, db_users: str = None, sql_types: str = None, duration: int = 60):
    username, password = oauth2.credential
    return data_transformer.collect_workloads(username, password, data_source, 
                                              databases, schemas, start_time, end_time, 
                                              db_users, sql_types, duration=duration)
