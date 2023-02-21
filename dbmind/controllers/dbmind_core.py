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

from pydantic import BaseModel

from dbmind.common.http import request_mapping, OAuth2
from dbmind.common.http import standardized_api_output
from dbmind.service import web
from dbmind import global_vars

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
        return web.check_credential(username, password, scopes)

    def before_hook(self, *args, **kwargs):
        super().before_hook(*args, **kwargs)
        if self.scopes:
            scope = self.scopes[0]
            global_vars.agent_proxy.switch_context(scope)
        else:
            # If not specified and there is only one RPC, use it.
            agent_list = global_vars.agent_proxy.get_all_agents()
            if len(agent_list) != 1:
                return
            global_vars.agent_proxy.switch_context(list(agent_list.keys())[0])
        instances = global_vars.agent_proxy.current_cluster_instances()
        ip_list = [i.split(':')[0] for i in instances]
        params = {
            web.ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: instances,
            web.ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ip_list,
            web.ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT:
                global_vars.agent_proxy.current_agent_addr(),
            web.ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX:
                '|'.join(map(lambda s: s + ':?.*', ip_list))
        }
        web.set_access_context(
            **params
        )

    def after_hook(self, *args, **kwargs):
        super().after_hook(*args, **kwargs)
        global_vars.agent_proxy.switch_context(None)  # clear the RPC state
        params = {
            web.ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: None,
            web.ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: None,
            web.ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT: None,
            web.ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX: None
        }
        web.set_access_context(
            **params
        )


oauth2 = DBMindOauth2.get_dbmind_oauth_instance()


@request_mapping('/api/list/agent', methods=['GET'], api=True)
@standardized_api_output
def get_all_agents():
    return web.get_all_agents()


@request_mapping('/api/status/running', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_running_status():
    return web.get_running_status()


@request_mapping('/api/status/transaction', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_xact_status():
    return web.get_xact_status()


@request_mapping('/api/status/alert', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_alert():
    return web.get_latest_alert()


@request_mapping('/api/status/node', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_node_status():
    return web.get_cluster_node_status()


@request_mapping('/api/status/instance', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_instance_status():
    return web.get_instance_status()


@request_mapping('/api/summary/cluster', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_cluster_summary():
    return web.get_cluster_summary()


@request_mapping('/api/list/metric', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_all_metrics():
    return web.get_all_metrics()


@request_mapping('/api/sequence/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_metric_sequence(name, start: int = None, end: int = None, step: int = None):
    return web.get_metric_sequence(name, start, end, step)


@request_mapping('/api/summary/workload_forecasting', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def workload_forecasting_summary():
    return web.get_forecast_sequence_info(metric_name=None)


@request_mapping('/api/workload_forecasting/sequence/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def workload_forecasting_get_metric_sequence(name, start: int = None, end: int = None, step: int = None):
    return web.get_metric_sequence(name, start, end, step)


@request_mapping('/api/workload_forecasting/sequence/forecast/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def workload_forecasting_forecast(name: str, start: int = None, end: int = None, step: int = None):
    return web.get_metric_forecast_sequence(name, start, end, step)


@request_mapping('/api/alarm/history', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_history_alarms(pagesize: int = None, current: int = None,
                       instance: str = None, alarm_type: str = None, alarm_level: str = None, group: bool = False):
    return web.get_history_alarms(pagesize, current, instance, alarm_type, alarm_level, group)


@request_mapping('/api/alarm/history_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_history_alarms_count(instance: str = None, alarm_type: str = None, alarm_level: str = None, group: bool = False):
    return web.get_history_alarms_count(instance, alarm_type, alarm_level, group)


@request_mapping('/api/alarm/future', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_future_alarms(pagesize: int = None, current: int = None,
                      instance: str = None, metric_name: str = None, start: int = None, group: bool = False):
    return web.get_future_alarms(pagesize, current, instance, metric_name, start, group)


@request_mapping('/api/alarm/future_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_future_alarms_count(instance: str = None, metric_name: str = None, start: int = None, group: bool = False):
    return web.get_future_alarms_count(instance, metric_name, start, group)


@request_mapping('/api/alarm/healing', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_healing_info_for_alarms(pagesize: int = None, current: int = None, instance: str = None, 
                                action: str = None, success: bool = None, min_occurrence: int = None):
    return web.get_healing_info(pagesize, current, instance, action, success, min_occurrence)


@request_mapping('/api/alarm/healing_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_healing_info_count_for_alarms(instance: str = None, action: str = None, success: bool = None, min_occurrence: int = None):
    return web.get_healing_info_count(instance, action, success, min_occurrence)


@request_mapping('/api/query/slow/recent', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_recent_slow_queries(pagesize: int = None, current: int = None, instance: str = None, query: str = None,
                            start: int = None, end: int = None, group: bool = False):
    return web.get_slow_queries(pagesize, current, instance, query, start, end, group)


@request_mapping('/api/query/slow/killed', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_killed_slow_queries(pagesize: int = None, current: int = None, instance: str = None, query: str = None,
                            start: int = None, end: int = None):
    return web.get_killed_slow_queries(pagesize, current, instance, query, start, end)


@request_mapping('/api/query/slow/killed_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_killed_slow_queries_count(instance: str = None, query: str = None, start: int = None, end: int = None):
    return web.get_killed_slow_queries_count(instance, query, start, end)


@request_mapping('/api/query/top', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_top_queries():
    username, password = oauth2.credential
    return web.get_top_queries(username, password)


@request_mapping('/api/query/active', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_active_queries():
    username, password = oauth2.credential
    return web.get_active_query(username, password)


@request_mapping('/api/query/locking', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_locking_queries():
    username, password = oauth2.credential
    return web.get_holding_lock_query(username, password)


@request_mapping('/api/list/database', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_db_list():
    return web.get_database_list()


@request_mapping('/api/summary/index_advisor', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_index_advisor_summary(positive_pagesize: int = None, positive_current: int = None, 
                              existing_pagesize: int = None, existing_current: int = None):
    return web.get_index_advisor_summary(positive_pagesize, positive_current, 
                                         existing_pagesize, existing_current)


@request_mapping('/api/summary/get_existing_indexes', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_existing_indexes(pagesize: int = None, current: int = None):
    return web.get_existing_indexes(pagesize, current)


@request_mapping('/api/summary/existing_indexes_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_existing_indexes_count():
    return web.get_existing_indexes_count()


@request_mapping('/api/summary/get_advised_indexes', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_advised_indexes():
    return web.get_advised_index()


@request_mapping('/api/summary/get_positive_sql', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_positive_sql(pagesize: int = None, current: int = None):
    return web.get_positive_sql(pagesize, current)


@request_mapping('/api/summary/positive_sql_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_positive_sql_count():
    return web.get_positive_sql_count()


@request_mapping('/api/summary/knob_tuning', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_tuning_summary(metricpagesize: int = None, metriccurrent: int = None,
                            warningpagesize: int = None, warningcurrent: int = None,
                            knobpagesize: int = None, knobcurrent: int = None):
    return web.toolkit_recommend_knobs_by_metrics(metricpagesize, metriccurrent,
                                                  warningpagesize, warningcurrent,
                                                  knobpagesize, knobcurrent)


@request_mapping('/api/summary/get_knob_recommendation_snapshot', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_snapshot(pagesize: int = None, current: int = None):
    return web.get_knob_recommendation_snapshot(pagesize, current)


@request_mapping('/api/summary/knob_recommendation_snapshot_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_snapshot_count():
    return web.get_knob_recommendation_snapshot_count()


@request_mapping('/api/summary/get_knob_recommendation_warnings', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_warnings(pagesize: int = None, current: int = None):
    return web.get_knob_recommendation_warnings(pagesize, current)


@request_mapping('/api/summary/knob_recommendation_warnings_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_warnings_count():
    return web.get_knob_recommendation_warnings_count()


@request_mapping('/api/summary/get_knob_recommendation', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation(pagesize: int = None, current: int = None):
    return web.get_knob_recommendation(pagesize, current)


@request_mapping('/api/summary/knob_recommendation_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_count():
    return web.get_knob_recommendation_count()


@request_mapping('/api/summary/slow_query', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_slow_query_summary(pagesize: int = None, current: int = None):
    return web.get_slow_query_summary(pagesize, current)


@request_mapping('/api/query/slow/recent_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_slow_query_count(instance: str = None, distinct: bool = False, query: str = None, 
                         start_time: str = None, end_time: str = None, group: bool = False):
    return web.get_slow_queries_count(instance, distinct, query, start_time, end_time, group)


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
def get_security_summary(pagesize: int = None, current: int = None, instance: str = None):
    return web.get_security_alarms(pagesize, current, instance)


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
def advise_indexes(database: str, sqls: list):
    return web.toolkit_index_advise(database, sqls)


@request_mapping('/api/toolkit/advise/query', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def tune_query(database: str, sql: str,
               use_rewrite: bool = True,
               use_hinter: bool = True,
               use_materialized: bool = True):
    return web.toolkit_rewrite_sql(database, sql)


@request_mapping('/api/toolkit/predict/query', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def sqldiag(database: str, sql: str):
    return {
        'cost_time': 1.1,
        'coordinate': [1.1, 2.1]
    }


@request_mapping('/api/toolkit/forecast/sequence/{name}', methods=['POST', 'GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def forecast(name: str, start: int = None, end: int = None, step: int = None):
    return web.get_metric_forecast_sequence(name, start, end, step)


@request_mapping('/api/setting/set', methods=['POST', 'GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def set_setting(config: str, name: str, value: str, dynamic: bool = True):
    if dynamic:
        if '' in (config.strip(), name.strip(), value.strip()):
            raise Exception('You should input correct setting.')
        web.global_vars.dynamic_configs.set(config, name, value)
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
        return web.global_vars.dynamic_configs.get(config, name)
    else:
        raise Exception("Currently, DBMind doesn't support showing the static "
                        "configurations due to security. Instead, you should "
                        "log in to the deployment machine and "
                        "see the configuration file.")


@request_mapping('/api/setting/list', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def list_setting():
    return {'dynamic': web.global_vars.dynamic_configs.list()}


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
    return web.toolkit_slow_sql_rca(sql,
                                    dbname=database,
                                    schema=schema,
                                    start_time=start_time,
                                    end_time=end_time)


@request_mapping('/api/summary/metric_statistic', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_metric_statistic(pagesize: int = None, current: int = None, instance: str = None, metric_name: str = None):
    return web.get_metric_statistic(pagesize, current, instance, metric_name)


@request_mapping('/api/summary/metric_statistic_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_metric_statistic_count(instance: str = None, metric_name: str = None):
    return web.get_metric_statistic_count(instance, metric_name)


@request_mapping('/api/summary/regular_inspections', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_regular_inspections(inspection_type: str = None):
    return web.get_regular_inspections(inspection_type)


@request_mapping('/api/summary/regular_inspections_count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_regular_inspections_count(inspection_type: str = None):
    return web.get_regular_inspections_count(inspection_type)


@request_mapping('/api/summary/correlation_result', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_correlation_result(metric_name: str = None, host: str = None, start_time: str = None, end_time: str = None):
    return web.get_correlation_result(metric_name, host, start_time, end_time)


@request_mapping('/api/toolkit/memory_check', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def memory_check(start_time: int = 0, end_time: int = 0):
    return web.check_memory_context(start_time, end_time)


@request_mapping('/api/app/start_timed_task', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def start_timed_app(funcname: str = None):
    return web.start_timed_task(funcname)


@request_mapping('/api/app/stop_timed_task', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def start_timed_app(funcname: str = None):
    return web.stop_timed_task(funcname)


@request_mapping('/api/app/reset_interval', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def reset_interval(funcname: str = None, seconds: int = 24 * 60 * 60):
    return web.reset_timed_task_interval(funcname, seconds)


@request_mapping('/api/app/get_timed_task_status', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_timed_task_status():
    return web.get_timed_task_status()
