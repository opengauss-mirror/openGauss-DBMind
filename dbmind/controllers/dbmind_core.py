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
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Dict, Union, List, Optional
from pydantic import BaseModel

from dbmind import global_vars
from dbmind.common.exceptions import ModeError
from dbmind.common.http import request_mapping, OAuth2, Request
from dbmind.common.http import standardized_api_output
from dbmind.common.types import ALARM_TYPES, ALARM_LEVEL
from dbmind.common.utils.checking import ParameterChecker, prepare_ip, split_ip_port
from dbmind.constants import PORT_SUFFIX
from dbmind.service.web import context_manager
from dbmind.service.web import data_transformer

latest_version = 'v1'
api_prefix = '/%s/api' % latest_version

io_thread_pool = ThreadPoolExecutor(max_workers=20)


class DBMindOauth2(OAuth2):
    token_url = api_prefix + '/token'

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
        ip_list = [split_ip_port(i)[0] for i in instances]
        params = {
            context_manager.ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: instances,
            context_manager.ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ip_list,
            context_manager.ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT:
                global_vars.agent_proxy.current_agent_addr(),
            context_manager.ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX:
                '|'.join([f"{prepare_ip(ip)}{PORT_SUFFIX}|{ip}" for ip in ip_list])
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


@request_mapping(api_prefix + '/agents', methods=['GET', 'PUT'], api=True)
@standardized_api_output
async def handle_agents(request: Request):
    """
    method GET: Obtain the agent list of DBMind monitoring instance

    - return: the agent list of DBMind monitoring instance
         e.g. {"data":{"ip1":["ip1","ip2","ip3"]}

    method PUT: Update the agent list of DBMind monitoring instance

    - param force: update agents forcely, only userd for PUT. choice=(true, false)
    - return: True or False, True means the update is successful and False means failure
         e.g. {"data":true,"success":true}
    """
    if global_vars.is_distribute_mode:
        raise ModeError("V1 API is not allowed in distribute mode.")
    try:
        if request.method == 'GET':
            return data_transformer.get_all_agents()
        elif request.method == 'PUT':
            data = await request.json()
            if not isinstance(data, dict):
                raise Exception('value {} is not jsonable.'.format(data))

            force = data.get('force', False)
            wait = data.get('wait', True)
            if wait:
                await asyncio.sleep(15)

            return data_transformer.update_agent_list(force=force)

    except json.decoder.JSONDecodeError as json_error:
        return "input is not jsonable, because: {}".format(json_error)


@request_mapping(api_prefix + '/summary/metrics', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_all_metrics():
    """
    Obtain the list of all metrics

    - return: the list of all metrics name from TSDB
         e.g. {"data":["opengauss_blks_hit_ratio","opengauss_blks_read_rate"...],"success":true}
    """
    return data_transformer.get_all_metrics()


@request_mapping(api_prefix + '/summary/metrics/{name}', methods=['GET', 'DELETE'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    name={"type": ParameterChecker.NAME, "optional": False},
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    latest_minutes={"type": ParameterChecker.UINT2, "optional": True},
    step={"type": ParameterChecker.UINT2, "optional": True},
    fetch_all={"type": ParameterChecker.BOOL, "optional": False},
    regex={"type": ParameterChecker.BOOL, "optional": False},
    from_timestamp={"type": ParameterChecker.TIMESTAMP, "optional": True},
    to_timestamp={"type": ParameterChecker.TIMESTAMP, "optional": True},
    labels={"type": ParameterChecker.STRING, "optional": True},
    regex_labels={"type": ParameterChecker.STRING, "optional": True},
    min_value={"type": ParameterChecker.FLOAT, "optional": True},
    max_value={"type": ParameterChecker.FLOAT, "optional": True},
    limit={"type": ParameterChecker.PINT32, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True}
)
@standardized_api_output
def manage_metric_sequence(request: Request, name: str, instance: str = None, latest_minutes: int = None,
                           from_timestamp: int = None, to_timestamp: int = None, step: int = None,
                           fetch_all: bool = False, regex: bool = False, labels: str = None,
                           regex_labels: str = None, min_value: float = None, max_value: float = None,
                           flush: bool = False, limit: int = None, tz: str = None):
    """
    method GET: Obtain the sequence of metrics data based on range or latest time

    - param name: the name of target metric
    - param instance: the instance which metric belongs to
    - param latest_minutes: the length of time to get the most recent metrics
    - param from_timestamp: start timestamp for range query, it works when latest_minutes is None
    - param to_timestamp: end timestamp for range query， it works when latest_minutes is None
    - param step: the time step for the retrieved data
    - param fetch_all: whether to retrieve all available sequences or just one
    - param regex: the parameter that represents whether to use regex to filter instance,
    it works when instance is not None
    - param labels: the parameter that represents a specific label to filter the sequence
    - param regex_labels: the parameter that represents a specific label to filter the sequence by regex
    - param min_value: filter the sequence whose value is greater than min_value
    - param max_value: filter the sequence whose value is less than max_value
    - param limit: limit the number of results returned

    - return: The list of sequences of the specified metric
         e.g. {"data":[{"labels":{"device":"device","from_instance":"ip","from_job":"exporter","fstype":"ext4",
              "instance":"ip:port","job":"exporter","mountpoint":"mounpoint"},"name":"os_disk_usage",
              "timestamps":[1684900929939,1684900944939,1684900959939,1684900974939],
              "values":[0.7115459280083043,0.7115460512887748,0.7115461745692453,0.7115462978497158]}],"success":true}

    method DELETE: Manually clean up the sequence in TSDB

    - param name: the name of target metric
    - param instance: the instance which metric belongs to
    - param latest_minutes: the length of time to get the most recent metrics
    - param from_timestamp: start timestamp for range query, it works when latest_minutes is None
    - param to_timestamp: end timestamp for range query， it works when latest_minutes is None
    - param regex: the parameter that represents whether to use regex to filter instance,
    it works when instance is not None
    - param labels: the parameter that represents a specific label to filter the sequence
    - param regex_labels: the parameter that represents a specific label to filter the sequence by regex
    - param flush: The data will still be stored on the disk for a period of time after the data is deleted,
    this parameter controls whether to flash the disk immediately

    - return: the result
        e.g. {"data":null,"success":true}
    """
    full_sql_offline_metric_name = 'full_sql_offline_'
    full_sql_online_metric_name = 'full_sql_online_'

    if request.method == 'GET':
        if latest_minutes is not None:
            return data_transformer.get_latest_metric_sequence(name, instance, latest_minutes,
                                                               step=step, fetch_all=fetch_all,
                                                               regex=regex, labels=labels,
                                                               regex_labels=regex_labels,
                                                               min_value=min_value, max_value=max_value, tz=tz)

        else:
            if name.startswith(full_sql_online_metric_name) \
                    or name.startswith(full_sql_offline_metric_name):
                return data_transformer.get_full_sql_statement(name, from_timestamp, to_timestamp, step=step,
                                                               fetch_all=True, labels=labels, regex_labels=regex_labels,
                                                               min_value=min_value, max_value=max_value, limit=limit,
                                                               tz=tz)
            return data_transformer.get_metric_sequence(name, instance, from_timestamp, to_timestamp, step=step,
                                                        fetch_all=fetch_all, regex=regex, labels=labels,
                                                        regex_labels=regex_labels,
                                                        min_value=min_value, max_value=max_value, tz=tz)
    elif request.method == 'DELETE':
        return data_transformer.delete_metric_sequence(name, instance, from_timestamp,
                                                       to_timestamp, regex, labels, regex_labels, flush, tz)


@request_mapping(api_prefix + '/summary/alarms', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": False},
    current={"type": ParameterChecker.UINT2, "optional": True},
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    group={"type": ParameterChecker.BOOL, "optional": False},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    metric_name={"type": ParameterChecker.NAME, "optional": True},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    anomaly_type={"type": ParameterChecker.NAME, "optional": True}
)
@standardized_api_output
def get_history_alarms(pagesize: int = 20, current: int = 0,
                       instance: str = None, alarm_type: str = None, alarm_level: str = None,
                       metric_name: str = None, start_at: int = None, end_at: int = None,
                       anomaly_type: str = None, group: bool = False):
    """
    Obtain the historical alarms list

    - param pagesize: the number of records per page, which is a positive integer
    - param current: current page number, which is a non-negative integer
    - param instance: the instance of alarms belongs to
    - param alarm_type: the type of alarm
    - param alarm_level: the level of alarm
    - param metric_name: the name of the metric
    - param start_at: the start timestamp when obtaining alarms
    - param end_at: the end timestamp when obtaining alarms
    - param anomaly_type: the type of the anomaly detection
    - param group: whether to group alarms by alarm type and content
    - return: The list of historical alarms
         e.g. {"data":{"header":["history_alarm_id","instance","metric_name","metric_filter","alarm_type","alarm_level",
            "start_at","end_at","alarm_content","extra_info","anomaly_type"],
            "rows":[[65,"ip","os_mem_usage",null,"SYSTEM",30,1684762097001,1684762547001,"mem_usage_spike_detector:
            Find obvious spikes in memory usage.",null,"Spike"]]},"success":true}
    """
    return data_transformer.get_history_alarms(
        pagesize,
        current,
        instance,
        alarm_type,
        alarm_level,
        metric_name,
        start_at,
        end_at,
        anomaly_type,
        group
    )


@request_mapping(api_prefix + '/summary/alarms/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    group={"type": ParameterChecker.BOOL, "optional": False},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    metric_name={"type": ParameterChecker.NAME, "optional": True},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    anomaly_type={"type": ParameterChecker.NAME, "optional": True}
)
@standardized_api_output
def get_history_alarms_count(instance: str = None, alarm_type: str = None, alarm_level: str = None,
                             metric_name: str = None, start_at: int = None, end_at: int = None,
                             anomaly_type: str = None, group: bool = False):
    """
    Obtain the number of historical alarms

    - param instance: the instance of alarms belongs to
    - param alarm_type: the type of alarm
    - param alarm_level: the level of alarm
    - param metric_name: the name of the metric
    - param start_at: the start timestamp when obtaining alarms
    - param end_at: the end timestamp when obtaining alarms
    - param anomaly_type: the type of the anomaly detection
    - param group: whether to group alarms by alarm type and content
    - return: The number of historical alarms
         e.g. {"data":30,"success":true}
    """
    return data_transformer.get_history_alarms_count(
        instance,
        alarm_type,
        alarm_level,
        metric_name,
        start_at,
        end_at,
        anomaly_type,
        group)


@request_mapping(api_prefix + '/summary/sql/slow/latest', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": False},
    current={"type": ParameterChecker.UINT2, "optional": True},
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    group={"type": ParameterChecker.BOOL, "optional": False},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    query={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_recent_slow_queries(pagesize: int = 20, current: int = 0, instance: str = None, query: str = None,
                            start_time: int = None, end_time: int = None, group: bool = False):
    """
    Obtain the list of the latest slow queries from meta-database

    - param pagesize: the number of records per page, which is a positive integer
    - param current: current page number, which is a non-negative integer
    - param instance: the instance of slow query belongs to
    - param query: the text of slow query
    - param start_time: the start timestamp when obtaining slow query
    - param end_time: the end timestamp when obtaining slow query
    - param group: whether to group alarms by alarm type and content
    - return: The list of the latest slow queries
         e.g. {"data":{"header":["instance","schema_name","db_name","query","template_id","hit_rate","fetch_rate",
              "cpu_time","data_io_time","parse_time","plan_time","db_time","root_cause","suggestion","start_at",
              "duration_time"],"rows":[(ip1, 'public', 'db1', 'select 1', t_id, ...), (...), ...]},"success":true}
    """
    return data_transformer.get_slow_queries(pagesize, current, instance, query, start_time, end_time, group)


@request_mapping(api_prefix + '/summary/sql/killed', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": False},
    current={"type": ParameterChecker.UINT2, "optional": True},
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    query={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_killed_slow_queries(pagesize: int = 20, current: int = 0, instance: str = None, query: str = None,
                            start_time: int = None, end_time: int = None):
    """
    Obtain the list of slow queries that are killed

    - param pagesize: the number of records per page, which is a positive integer
    - param current: current page number, which is a non-negative integer
    - param instance: the instance of slow query belongs to
    - param query: the text of slow query
    - param start_time: the start timestamp when obtaining slow query
    - param end_time: the end timestamp when obtaining slow query
    - return: The list of the killed slow queries
         e.g. {"data":{"header":["killed_query_id","instance","db_name","query","killed","username","elapsed_time",
              "killed_time"],"rows":[(t_id, instance, db1, 'select 1', true, user, 100, time), (...), ...]},
              "success":true}
    """
    return data_transformer.get_killed_slow_queries(pagesize, current, instance, query, start_time, end_time)


@request_mapping(api_prefix + '/summary/sql/killed/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    query={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_killed_slow_queries_count(instance: str = None, query: str = None, start_time: int = None,
                                  end_time: int = None):
    """
    Obtain the number of slow queries that are killed

    - param instance: the instance of slow query belongs to
    - param query: the text of slow query
    - param start_time: the start timestamp when obtaining slow query
    - param end_time: the end timestamp when obtaining slow query
    - return: The number of killed slow queries
         e.g. {"data":0,"success":true}
    """
    return data_transformer.get_killed_slow_queries_count(instance, query, start_time, end_time)


@request_mapping(api_prefix + '/summary/sql/top', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_top_queries():
    """
    Obtain the top 10 most frequently executed SQL templates

    - return: The list of top 10 most frequently executed SQL templates
         e.g. {"data":{"header":["user_name","unique_sql_id","query","n_calls","min_elapse_time","max_elapse_time",
              "avg_elapse_time","n_returned_rows","db_time","cpu_time","execution_time","parse_time","last_updated",
              "sort_spill_count","hash_spill_count"],"rows":[["user",1961954918,"vacuum;",1,38958701,38958701,
              "38919781.218781218781",0,38959021...]]},"success":true}
    """
    username, password = oauth2.credential
    return data_transformer.get_top_queries(username, password)


@request_mapping(api_prefix + '/summary/sql/active', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_active_queries():
    """
    Obtain the list of active SQL statements

    - return: The list of active SQL statements
         e.g. {"data":{"header":["datname","usename","application_name","client_addr","query_start","waiting","state",
              "query","connection_info"], "rows":[["data1","db1","app1","ip:port","time"...]]},"success":true}
    """
    username, password = oauth2.credential
    return data_transformer.get_active_query(username, password)


@request_mapping(api_prefix + '/summary/sql/locking', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_locking_queries():
    """
    Obtain the SQL statements currently holding the lock

    - return: The list of SQL statements currently holding the lock
         e.g. {"data":{"header":null,"rows":[]},"success":true}
    """
    username, password = oauth2.credential
    return data_transformer.get_holding_lock_query(username, password)


@request_mapping(api_prefix + '/summary/database-list', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_db_list():
    """
    Obtain the list of databases under the current instance

    - return: The list of the database
         e.g. {"data":["db1","db2","db3","db4","db5"...],"success":true}
    """
    return data_transformer.get_database_list()


@request_mapping(api_prefix + '/summary/knob-recommendation/snapshots', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": False},
    current={"type": ParameterChecker.UINT2, "optional": True}
)
@standardized_api_output
def get_knob_recommendation_snapshot(pagesize: int = 20, current: int = 0):
    """
    Obtain the snapshots of the recommended knobs

    - param pagesize: the number of records per page for recommended-knob snapshot from meta-database,
     which is a positive integer
    - param current: the current page for recommended-knob snapshot from meta-database, which is a non-negative integer
    - return: The snapshots of the recommended knobs
         e.g. {"data":{"header":["instance","metric","value"],"rows":[]},"success":true}
    """
    return data_transformer.get_knob_recommendation_snapshot(pagesize, current)


@request_mapping(api_prefix + '/summary/knob-recommendation/snapshots/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_snapshot_count():
    """
    Obtain the number of the snapshots of the recommended knobs from meta-database

    - return: The number of the snapshots of the recommended knobs from meta-database
         e.g. {"data":0,"success":true}
    """
    return data_transformer.get_knob_recommendation_snapshot_count()


@request_mapping(api_prefix + '/summary/knob-recommendation/warnings', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": False},
    current={"type": ParameterChecker.UINT2, "optional": True}
)
@standardized_api_output
def get_knob_recommendation_warnings(pagesize: int = 20, current: int = 0):
    """
    Obtain the snapshots of the recommendation warnings

    - param pagesize: the number of records per page for recommendation warnings from meta-database,
    which is a positive integer
    - param current: the current page for recommendation warnings from meta-database from meta-database,
    which is a non-negative integer
    - return: The snapshots of the recommendation warnings
         e.g. {"data":{"header":["instance","level","comment"],"rows":[]},"success":true}
    """
    return data_transformer.get_knob_recommendation_warnings(pagesize, current)


@request_mapping(api_prefix + '/summary/knob-recommendation/warnings/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_warnings_count():
    """
    Obtain the number of the warnings of the recommendation warnings from meta-database

    - return: The number of the warnings of the recommended knobs from meta-database
         e.g. {"data":0,"success":true}
    """
    return data_transformer.get_knob_recommendation_warnings_count()


@request_mapping(api_prefix + '/summary/knob-recommendation/details', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": False},
    current={"type": ParameterChecker.UINT2, "optional": True}
)
@standardized_api_output
def get_knob_recommendation(pagesize: int = 20, current: int = 0):
    """
    Obtain the snapshots of the recommendation warnings

    - param pagesize: the number of records per page for recommended knob from meta-database,
    which is a positive integer
    - param current: the current page for recommended knob from meta-database, which is a non-negative integer
    - return: The snapshots of the recommendation warnings
         e.g. {"data":{"header":["instance","name","current","recommend","min","max"],"rows":[]},"success":true}
    """
    return data_transformer.get_knob_recommendation(pagesize, current)


@request_mapping(api_prefix + '/summary/knob-recommendation/details/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_recommendation_count():
    """
    Obtain the number of the details of the recommended knobs  from meta-database

    - return: The number of the details of the recommended knobs  from meta-database
         e.g. {"data":0,"success":true}
    """
    return data_transformer.get_knob_recommendation_count()


@request_mapping(api_prefix + '/summary/sql/slow', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_slow_query_summary():
    """
    Obtain slow SQL summary information from the meta-database

    - return: The list of the slow queries
         e.g. {"data":{"distribution":{"delete":0,"insert":0,"select":0,"update":0},"main_slow_queries":0,
              "mean_buffer_hit_rate":-1,"mean_cpu_time":-1,"mean_fetch_rate":-1,"mean_io_time":-1,
              "nb_unique_slow_queries":0,"slow_query_count":{"timestamps":[],"values":[]},
              "slow_query_template":{"header":["template_id","count","query"],"rows":[]},"slow_query_threshold":2.0,
              "statistics_for_database":{},"statistics_for_schema":{},"systable":{"business_table":0,"system_table":0}},
              "success":true}
    """
    return data_transformer.get_slow_query_summary()


@request_mapping(api_prefix + '/summary/sql/slow/latest/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    distinct={"type": ParameterChecker.BOOL, "optional": False},
    group={"type": ParameterChecker.BOOL, "optional": False},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    query={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_slow_query_count(instance: str = None, distinct: bool = False, query: str = None,
                         start_time: int = None, end_time: int = None, group: bool = False):
    """
    Obtain the number of the latest slow queries from meta-database

    - param instance: the instance which slow query belongs to
    - param distinct: whether the count should be distinct or not
    - param query: the text of query
    - param start_time: the start timestamp when obtaining slow query
    - param end_time: the start timestamp when obtaining slow query
    - param group: whether to group slow query by unique query id
    - return: The number of the latest slow queries
         e.g. {"data":0,"success":true}
    """
    return data_transformer.get_slow_queries_count(instance, distinct, query, start_time, end_time, group)


@request_mapping(api_prefix + '/app/index-recommendation', methods=['POST'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    max_index_num={"type": ParameterChecker.PINT32, "optional": True},
    max_index_storage={"type": ParameterChecker.PINT32, "optional": True},
    database={"type": ParameterChecker.NAME, "optional": True}
)
@standardized_api_output
def advise_indexes(instance: str, database: str, max_index_num: int, max_index_storage: int, sqls: list):
    """
    Obtain the list of recommended indexes

    - param instance: the instance which index belong to
    - param database: the database which index belong to
    - param max_index_num: max number of index advised. If this value is not a valid positive integer, it will be
    replaced with the default value.
    - param max_index_storage: max storage usage of index advised. If this value is not a valid positive integer,
    it will be replaced with the default value.
    - param sqls: the text of sql
    - return: The list of recommended indexes
         e.g. {"data":[{"advise_indexes":[],"redundant_indexes":[],"total":0,"useless_indexes":
         [{"columns":"c1","schemaName":"public","statement":"DROP INDEX t1_c1_idx;","tbName":"t1","type":3},
         {"columns":"c2","schemaName":"public","statement":"DROP INDEX t2_c2_idx;","tbName":"t2","type":3}]},{}],
         "success":true}
    """
    username, password = oauth2.credential
    return data_transformer.toolkit_index_advise(username, password, instance, database, ''.join(sqls), max_index_num,
                                                 max_index_storage)


class DynamicConfig(BaseModel):
    config: str
    name: str
    value: str


@request_mapping(api_prefix + '/configs/dynamic-config', methods=['GET', 'PUT'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    config={"type": ParameterChecker.NAME, "optional": True},
    name={"type": ParameterChecker.NAME, "optional": True}
)
@standardized_api_output
def get_setting(request: Request, config: str = None, name: str = None,
                config_model: DynamicConfig = None):
    """
    method GET: get the value of the specialized dynamic config

    - param config: the category of configs, only used for GET method
    - param name: the name of configs, only used for GET method
    - return:  The value of the specialized dynamic config
        e.g. {"data":"50","success":true}

    method PUT: Set the DBMind dynamic configs, currently DBMind cannot modify the static configs,
    you should modify the configs by using the command line

    - param config_model: dynamic config model, only used for PUT method
    - return: The execution result of the setting
         e.g. {"data":"success","success":true}
    """
    if request.method == 'GET':
        if config is None:
            raise Exception(f'The config parameter is None.')
        if name is None:
            raise Exception(f'The name parameter is None.')
        if config.upper() == 'IV_TABLE':
            raise Exception(f'The config {config} parameter is not correct.')
        return global_vars.dynamic_configs.get(config, name)
    elif request.method == 'PUT':
        if config_model is None:
            raise Exception(f'The config_model is None.')
        if '' in (config_model.config.strip(), config_model.name.strip(), config_model.value.strip()):
            raise Exception('You should input correct setting.')
        if config_model.config.upper() == 'IV_TABLE':
            raise Exception(f'The config {config_model.config} parameter is not correct.')
        global_vars.dynamic_configs.set(config_model.config, config_model.name, config_model.value)
        return 'success'


@request_mapping(api_prefix + '/configs/dynamic-config/list', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    is_default={"type": ParameterChecker.BOOL, "optional": False}
)
@standardized_api_output
def list_setting(is_default: bool = False):
    """
    Get the DBMind dynamic configs list

    - param is_default: if is_default is True, get configs from local config file
    - return: The list of DBMind dynamic configs
        e.g. {"data":{"dynamic":{"detection_params":[["esd_test_alpha","0.05","The Significance
        level."]...]}...},"success":true}
    """
    if is_default:
        return {'dynamic': data_transformer.get_local_dynamic_configs()}
    return {'dynamic': global_vars.dynamic_configs.list()}


@request_mapping(api_prefix + '/app/slow-sql-rca', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    db_name={"type": ParameterChecker.NAME, "optional": True},
    schema_name={"type": ParameterChecker.STRING, "optional": True},
    template_id={"type": ParameterChecker.DIGIT, "optional": True},
    debug_query_id={"type": ParameterChecker.DIGIT, "optional": True},
    n_soft_parse={"type": ParameterChecker.INT32, "optional": True},
    n_hard_parse={"type": ParameterChecker.INT32, "optional": True},
    n_returned_rows={"type": ParameterChecker.INT32, "optional": True},
    n_tuples_fetched={"type": ParameterChecker.INT32, "optional": True},
    n_tuples_returned={"type": ParameterChecker.INT32, "optional": True},
    n_tuples_inserted={"type": ParameterChecker.INT32, "optional": True},
    n_tuples_updated={"type": ParameterChecker.INT32, "optional": True},
    n_tuples_deleted={"type": ParameterChecker.INT32, "optional": True},
    n_blocks_fetched={"type": ParameterChecker.INT32, "optional": True},
    n_blocks_hit={"type": ParameterChecker.INT32, "optional": True},
    db_time={"type": ParameterChecker.INT32, "optional": True},
    cpu_time={"type": ParameterChecker.INT32, "optional": True},
    parse_time={"type": ParameterChecker.INT32, "optional": True},
    plan_time={"type": ParameterChecker.INT32, "optional": True},
    data_io_time={"type": ParameterChecker.INT32, "optional": True},
    hash_spill_count={"type": ParameterChecker.UINT2, "optional": True},
    sort_spill_count={"type": ParameterChecker.UINT2, "optional": True},
    n_calls={"type": ParameterChecker.PINT32, "optional": False},
    lock_wait_time={"type": ParameterChecker.INT32, "optional": True},
    lwlock_wait_time={"type": ParameterChecker.INT32, "optional": True},
    query={"type": ParameterChecker.STRING, "optional": True},
    advise={"type": ParameterChecker.STRING, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True},
    query_plan={"type": ParameterChecker.STRING, "optional": True},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    finish_time={"type": ParameterChecker.TIMESTAMP, "optional": True}
)
@standardized_api_output
def diagnose_slow_query(query: str = None,
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
                        tz: str = None):
    """
    Obtain the root cause analysis result of slow SQL statement

    - param query: the text of slow SQL statement
    - param advise: the advise from DFX analysis
    - param db_name: the database of slow SQL statement
    - param schema_name: the schema of slow SQL statement
    - param start_time: the start time of slow SQL statement
    - param finish_time: the finish time of slow SQL statement
    - param template_id: the unique SQL id of slow SQL statement
    - param debug_query_id: the debug SQL id of slow SQL statement
    - param n_soft_parse: the soft parse number of slow SQL statement
    - param n_hard_parse: the hard parse number of slow SQL statement
    - param query_plan: the query plan of slow SQL statement
    - param n_returned_rows: the returned tuples of slow SQL statement
    - param n_tuples_fetched: the randomly scan tuples of slow SQL statement
    - param n_tuples_returned: the sequentially scan tuples of slow SQL statement
    - param n_tuples_inserted: the inserted tuples of slow SQL statement
    - param n_tuples_updated: the updated tuples of slow SQL statement
    - param n_tuples_deleted: the deleted tuples of slow SQL statement
    - param n_blocks_fetched: the number of block accesses of the buffers
    - param n_blocks_hit: the number of buffers block hits
    - param db_time: the db_time of slow SQL statement
    - param cpu_time: the cpu_time of slow SQL statement
    - param parse_time: the parse_time of slow SQL statement
    - param plan_time: the plan_time of slow SQL statement
    - param data_io_time: the data_io_time of slow SQL statement
    - param hash_spill_count: the hash_spill_count of slow SQL statement
    - param sort_spill_count: the sort_spill_count of slow SQL statement
    - param n_calls: the call number of slow SQL statement
    - param lock_wait_time: the lock_wait_time of slow SQL statement
    - param lwlock_wait_time: the lwlock_wait_time of slow SQL statement
    - param tz: slow SQL related time zones, only support 'UTC'
    - return: The RCA result of the specified slow SQL statement
         e.g. {"data":["Seq Scan on t1  (cost=0.00..1868.00 rows=100 width=70)\n",
              [[["1. HEAVY_SCAN_OPERATOR: (1.00) Existing expensive seq scans. Detail:..."]]...]],"success":true}
    """
    username, password = oauth2.credential
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
    return data_transformer.toolkit_slow_query_rca(username, password, **params)


@request_mapping(api_prefix + '/summary/regular-inspection', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    inspection_type={"type": ParameterChecker.NAME, "optional": True}
)
@standardized_api_output
def get_regular_inspections(inspection_type: str = None):
    """
    Obtain the regular inspection result from meta-database

    - param inspection_type: The specified type of the regular inspection,
    choices=('daily_check', 'weekly_check', 'monthly_check')
    - return: The list of regular inspection result
         e.g. {"data":{"header":["instance","report","start","end"],"rows":[["ip:port",
              {"connection":{"active_connection":{"avg":3.0,"max":4.0,"min":1.0,"the_95th":4.0},
              "total_connection":{"avg":3.2321,"max":5.0,"min":1.0,"the_95th":4.0}}...},"success":true}
    """
    return data_transformer.get_regular_inspections(inspection_type)


@request_mapping(api_prefix + '/summary/regular-inspection/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    inspection_type={"type": ParameterChecker.NAME, "optional": True}
)
@standardized_api_output
def get_regular_inspections_count(inspection_type: str = None):
    """
    Obtain the number of the regular inspection from meta-database

    - param inspection_type: The specified type of the regular inspection
    - return: The number of the regular inspection
         e.g. {"data":9,"success":true}
    """
    return data_transformer.get_regular_inspections_count(inspection_type)


class InspectionItems(BaseModel):
    system_resource: List[Optional[Union[str, dict]]]
    instance_status: List[Optional[Union[str, dict]]]
    database_resource: List[Optional[Union[str, dict]]]
    database_performance: List[Optional[Union[str, dict]]]
    diagnosis_optimization: List[Optional[Union[str, dict]]]


@request_mapping(api_prefix + '/app/real-time-inspection', methods=['POST', 'DELETE'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    inspection_type={"type": ParameterChecker.STRING, "optional": True},
    instance={"type": ParameterChecker.IP_WITH_PORT, "optional": False},
    spec_id={"type": ParameterChecker.STRING, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True}
)
@standardized_api_output
async def exec_real_time_inspections(request: Request, inspection_type: str = None,
                                     start_time: str = None, end_time: str = None,
                                     instance: str = None, spec_id: str = None,
                                     inspection_items: InspectionItems = None,
                                     tz: str = None):
    """
    method POST: get the inspection result and insert into meta-database

    - param item: The specified params that needs to exec inspection
    - return: result of the inspection
         e.g. {"system_resource": {xxx}, xxx}

    method DELETE: delete the inspection result from meta-database

    - param spec_id: The specified row of the inspection
    - return: status of the deletion
         e.g. {"success": True}
    """
    if request.method == 'POST':
        username, password = oauth2.credential
        func = partial(
            data_transformer.exec_real_time_inspections,
            username, password, inspection_type,
            start_time, end_time,
            instance, inspection_items, tz
        )
        result = await asyncio.get_event_loop().run_in_executor(io_thread_pool, func)
        return result
    elif request.method == 'DELETE':
        result = data_transformer.delete_real_time_inspections(instance, spec_id)
        return result


@request_mapping(api_prefix + '/app/real-time-inspection/list', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.IP_WITH_PORT, "optional": False}
)
@standardized_api_output
def list_real_time_inspections(instance: str = None):
    """
    List all inspection result from meta-database for specify instance

    - return: The list of inspection result
         e.g. {"data":{"header":["instance","start","end", "id", "state", "cost_time", "inspection_type"],"rows":[["ip:port",
              {"connection":{"active_connection":{"avg":3.0,"max":4.0,"min":1.0,"the_95th":4.0},
              "total_connection":{"avg":3.2321,"max":5.0,"min":1.0,"the_95th":4.0}}...},"success":true}
    """
    return data_transformer.list_real_time_inspections(instance)


@request_mapping(api_prefix + '/summary/real-time-inspection', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.IP_WITH_PORT, "optional": False},
    spec_id={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def report_real_time_inspections(instance: str = None, spec_id: str = None):
    """
    report the inspection result from meta-database

    - param spec_id: The specified row of the inspection
    - return: The inspection result
         e.g. {"data":{"header":["instance","report","start","end", "id", "state",
               "cost_time", "inspection_type"],"rows":[["ip:port",
              {"connection":{"active_connection":{"avg":3.0,"max":4.0,"min":1.0,"the_95th":4.0},
              "total_connection":{"avg":3.2321,"max":5.0,"min":1.0,"the_95th":4.0}}...},"success":true}
    """
    return data_transformer.report_real_time_inspections(instance, spec_id)


@request_mapping(api_prefix + '/app/correlation', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    metric_name={"type": ParameterChecker.NAME, "optional": False},
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": False},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": False},
    metric_filter={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_correlation_result(metric_name: str = None, instance: str = None,
                           start_time: int = None, end_time: int = None,
                           metric_filter: str = None):
    """
    Obtain the correlation analysis results of abnormal metric

    - param metric_name: The name of the metric to be analyzed
    - param instance: the instance where abnormal metric from
    - param start_time: the abnormal start time of metric
    - param end_time: the abnormal end time of metric
    - return: The correlation analysis result
         e.g. {"data":{"os_mem_usage from ip":[["os_mem_usage from ip",1.0,0,[0.32950820234487...]]...]},"success":true}
    """
    return data_transformer.get_correlation_result(metric_name, instance,
                                                   start_time, end_time,
                                                   metric_filter=metric_filter)


@request_mapping(api_prefix + '/status/schedulers', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_timed_task_status():
    """
    Obtain the status of scheduled tasks

    - return: The status of scheduled tasks
        e.g. {"data":{"header":["name","current_status","running_interval"],
        "rows":[["anomaly_detection","Running",180]...]},"success":true}
    """
    return data_transformer.get_timed_task_status()


@request_mapping(api_prefix + '/app/risk-analysis/{metric}', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    metric={"type": ParameterChecker.NAME, "optional": False},
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    warning_hours={"type": ParameterChecker.PINT32, "optional": False},
    upper={"type": ParameterChecker.FLOAT, "optional": True},
    lower={"type": ParameterChecker.FLOAT, "optional": True},
    labels={"type": ParameterChecker.STRING, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True}
)
@standardized_api_output
def risk_analysis(metric: str, instance: str = None, warning_hours: int = 1, upper: float = None,
                  lower: float = None, labels: str = None, tz: str = None):
    """
    Obtain the risk analysis result of the specified metric

    - param metric: the metric name to be implemented risk analysis
    - param instance: the instance where abnormal metric from
    - param warning_hours: length of risk analysis, unit is hours
    - param upper: the upper limit of the metric
    - param lower: the lower limit of the metric
    - param labels: the labels for metric, used to precisely find metric
    - param tz: the timezone information, example: UTC-8, UTC+8
    - return: The risk analysis result
         e.g. {"data":{},"success":true}
    """
    return data_transformer.risk_analysis(metric, instance, warning_hours,
                                          upper=upper, lower=lower, labels=labels, tz=tz)


class SSLContext(BaseModel):
    ssl_certfile: str = None
    ssl_keyfile: str = None
    ssl_ca_file: str = None
    ssl_keyfile_password: str = None


@request_mapping(api_prefix + '/status/collection-system', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    exporter_type={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_collection_system_status(exporter_type: str = None, ssl_context: SSLContext = None):
    """
    Obtain the status of the collection system

    - param exporter_type: The type of exporter, choice=('opengauss_exporter', 'cmd_exporter', 'reprocessing_exporter')
    - ssl_context: The context of ssl

    - return: The status of the collection system
         e.g. {"data":{"header":["component","listen_address","is_alive"],
              "rows":[["opengauss_exporter","ip",true]...],"suggestions":[]},"success":true}
    """
    return data_transformer.get_exporter_status(exporter_type, ssl_context)


@request_mapping(api_prefix + '/configs/anomaly-detection/default-settings', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_detector_init_defaults():
    """
    Get the default settings of the anomaly detection

    - return: The default settings of the anomaly detection
         e.g. {"data":{"AlarmInfo":{"alarm_cause":null,"alarm_content":null,...,"window":10}},"success":true}
    """
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


class JsonDict(BaseModel):
    duration: int
    forecasting_seconds: int
    alarm_info: AlarmInfo
    detector_info: List[DetectorInfo]


@request_mapping(api_prefix + '/app/anomaly-detection/detectors/{name}', methods=['PUT', 'GET', 'DELETE'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    name={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def manage_detector(name, request: Request, json_dict: JsonDict = None):
    """
    method GET: Get the state of the specified anomaly detection detector

    - param name: The name of the specified detector, only used for GET and DELETE method
    - return: The state of the specified anomaly detection detector
        e.g. {"data":{"alarm_info":{"alarm_cause":null,"alarm_content":null,"alarm_level":"ERROR","alarm_type":"SYSTEM",
        "extra":null},"detector_info":[...],"duration":10,"forecasting_seconds":0,"running":1},"success":true}

    method PUT: Change the state of the specified anomaly detection detector
    - param json_dict: only used for PUT method

    method DELETE: Delete the specified anomaly detection detector

    - param name: The name of the specified detector, only used for GET and DELETE method
    - return: The execution result
         e.g. {"data":"Success: delete qps_spike_detector","success":true}
    """

    data_transformer.update_agent_list(force=True)
    detector_operation = data_transformer.DetectorOperation(name)
    if request.method == 'GET':
        return detector_operation.view()

    elif request.method == 'PUT':
        if name in ["all"]:
            raise Exception(f'Illegal detector name: {name}.')

        if json_dict is None:
            raise Exception(f'The json_dict is None.')

        if len(json_dict.detector_info) == 0:
            raise Exception(f'The json_dict has no detector_info.')

        valid_alarm_type_list = [k for k in ALARM_TYPES.__dict__ if not k.endswith("__")]
        valid_alarm_level_list = list(ALARM_LEVEL.__members__)
        if json_dict.alarm_info.alarm_type not in valid_alarm_type_list:
            raise Exception('The alarm_type is not valid, use proper type in {}.'.format(valid_alarm_type_list))

        if json_dict.alarm_info.alarm_level not in valid_alarm_level_list:
            raise Exception('The alarm_level is not valid, use proper level in {}.'.format(valid_alarm_level_list))

        adding_result = detector_operation.add(json_dict.dict(), fuzzy_match=True)

        all_metrics = data_transformer.get_all_metrics()
        for detector_info in json_dict.detector_info:
            if detector_info.metric_name not in all_metrics:
                return adding_result + (' Warning: You are adding or modifying a detector '
                                        f'for a new metric {detector_info.metric_name}, '
                                        'the detector will not be effective '
                                        'if the metric never shows up.')
        return adding_result

    elif request.method == 'DELETE':
        if name in ["all"]:
            raise Exception(f'Illegal detector name: {name}.')

        return detector_operation.delete()


@request_mapping(api_prefix + '/app/anomaly-detection/detectors/{name}/pause', methods=['PUT'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    name={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def pause_detector(name):
    """
    Pause the specified anomaly detection detector

    - param name: The name of the specified detector
    - param instance: The specific instance to which the detector belongs, default is current instance
    - return: The execution result
         e.g. {"data":"Success: pause my_metric2","success":true}
    """
    detector_operation = data_transformer.DetectorOperation(name)
    return detector_operation.pause()


@request_mapping(api_prefix + '/app/anomaly-detection/detectors/{name}/resume', methods=['PUT'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    name={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def resume_detector(name):
    """
    Resume the specified anomaly detection detector

    - param name: The name of the specified detector
    - param instance: The specific instance to which the detector belongs, default is current instance
    - return: The execution result
         e.g. {"data":"Success: resume my_metric2","success":true}
    """
    detector_operation = data_transformer.DetectorOperation(name)
    return detector_operation.resume()


@request_mapping(api_prefix + '/app/anomaly-detection/detectors/all/rebuild', methods=['PUT'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def rebuild_detector():
    """
    Rebuild all anomaly detection detectors based on the back-end flushing information

    - param instance: The specific instance to which the detector belongs, default is current instance
    - return: The execution result
         e.g. {"data":"Success: rebuild detectors","success":true}
    """
    detector_operation = data_transformer.DetectorOperation(name=None)
    return detector_operation.rebuild()


@request_mapping(api_prefix + '/app/anomaly-detection/detectors', methods=['DELETE'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def clear_detector():
    """
    Clear all anomaly detection detectors

    - param instance: The specific instance to which the detector belongs, default is current instance
    - return: The execution result
         e.g. {"data":"Success: clear detectors","success":true}
    """
    detector_operation = data_transformer.DetectorOperation(name=None)
    return detector_operation.clear()


@request_mapping(api_prefix + '/status/data-directory', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    latest_minutes={"type": ParameterChecker.PINT32, "optional": False}
)
@standardized_api_output
def get_database_data_directory_status(instance: str, latest_minutes: int = 5):
    """
    Obtain the database data directory status

    - param instance: the address of any node in the instance
    - param latest_minutes: the length of time you want to get the most recent information, unit is minute
    - return: The status of the database data directory
         e.g. {"data":{"free_space":2161.57,"tilt_rate":0.03,"total_space":3298.17...},"success":true}

    """
    return data_transformer.get_database_data_directory_status(instance, latest_minutes)


@request_mapping(api_prefix + '/status/overview', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    latest_minutes={"type": ParameterChecker.PINT32, "optional": False}
)
@standardized_api_output
def get_front_oveview(latest_minutes: int = 3):
    """
    Obtain the database overview information

    - param latest_minutes:
    - return: The overview information of the database
         e.g. {"data":{"deployment_mode":"centralized",...,"strength_version":"openGauss 2.1.0..."},"success":true}
    """
    return data_transformer.get_front_overview(latest_minutes=latest_minutes)


@request_mapping(api_prefix + '/status/instances', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_current_instance_status():
    """
    Obtain the database instances status

    - return: The status of the database instances
         e.g. {"data":{"header":["instance","role","state"],"rows":[["ip:port,","primary",false]...]},"success":true}
    """
    return data_transformer.get_current_instance_status()


@request_mapping(api_prefix + '/status/agents', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_agent_status():
    """
    Obtain the database agents status

    - return: The status of the database agents
         e.g. {"data":{"agent_address":"ip","status":true},"success":true}
    """
    return data_transformer.get_agent_status()


@request_mapping(api_prefix + '/app/workload-collection', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    duration={"type": ParameterChecker.INT32, "optional": False},
    template_id={"type": ParameterChecker.DIGIT, "optional": True},
    start_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_time={"type": ParameterChecker.TIMESTAMP, "optional": True},
    sql_types={"type": ParameterChecker.STRING, "optional": True},
    db_users={"type": ParameterChecker.STRING, "optional": True},
    schemas={"type": ParameterChecker.STRING, "optional": True},
    databases={"type": ParameterChecker.STRING, "optional": True},
    data_source={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def collect_workloads(data_source: str = None, databases: str = None, schemas: str = None, start_time: int = None,
                      end_time: int = None, db_users: str = None, sql_types: str = None, template_id: str = None,
                      duration: int = 60):
    """
    Obtain workloads from different data source

    - param data_source: the data source of workloads, choice=('asp', 'statement_history', 'pg_stat_activity')
    - param databases: the database lists of the workloads
    - param schemas: the schema list of the workloads
    - param start_time: the start timestamp of the collection interval
    - param end_time: The end timestamp of the collection interval,
    only works when data_source in ('asp', 'statement_history')
    - param db_users: the user list of the workloads
    - param sql_types: the sql type list of workloads, choice=('SELECT', 'UPDATE', 'DELETE', 'INSERT')
    - param template_id: the unique_sql_id of SQL template
    - param duration: the SQL execution time of workloads,
    only works when data_source in ('statement_history', 'pg_stat_activity')
    - return: The collection result of the workload data
         e.g. {"data":{"header":["user_name","db_name","schema_name","application_name","unique_query_id","start_time",
              "finish_time","duration","n_returned_rows","n_tuples_fetched","n_tuples_returned","n_tuples_inserted",
              "n_tuples_updated","n_tuples_deleted","n_blocks_fetched","n_blocks_hit","n_soft_parse","n_hard_parse",
              "db_time","cpu_time","parse_time","plan_time","data_io_time","lock_wait_time","lwlock_wait_time","query"],
              "rows":[["user1","db1","public"...]...]},"success":true}
    """
    username, password = oauth2.credential
    return data_transformer.collect_workloads(username, password, data_source,
                                              databases, schemas, start_time, end_time,
                                              db_users, sql_types, template_id, duration=duration)


@request_mapping(api_prefix + '/app/cluster-diagnosis', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    timestamp={"type": ParameterChecker.TIMESTAMP, "optional": True},
    method={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def get_cluster_diagnosis(instance: str, role: str, method: str, timestamp: int):
    """
    Obtain the cluster diagnosis result of the specified cluster
    CN node status map: {0: "Normal", 1: "Down", 2: "Deleted", 3: "ReadOnly"}
    DN node status map: {0: "Normal", 1: "Unknown", 2: "Need repair", 3: "Waiting for promoting, Promoting, Demoting",
    4: "Disk damaged", 5: "Port conflicting", 6: "Building", 7: "Build failed", 8: "CoreDump", 9: "ReadOnly",
    10: "Manually stopped"}

    - param instance: The specified instance to be diagnosed
    - param role: The role of instance for diagnosis, choice=('cn', 'dn')
    - param method: Method for the model, choice=('logical', 'tree')
    - param timestamp:  Time for diagnosis in timestamp(ms)
    - return: The cluster diagnosis result
         e.g. {"detail":[{"loc":["query","instance"],"msg":"field required","type":"value_error.missing"}...]}
    """
    return data_transformer.get_cluster_diagnosis(instance, role, method, timestamp)


@request_mapping(api_prefix + '/summary/cluster-diagnosis', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    pagesize={"type": ParameterChecker.PINT32, "optional": False},
    current={"type": ParameterChecker.UINT2, "optional": True},
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    instance_like={"type": ParameterChecker.INSTANCE, "optional": True},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    cluster_role={"type": ParameterChecker.NAME, "optional": True},
    diagnosis_method={"type": ParameterChecker.NAME, "optional": True},
    status_code={"type": ParameterChecker.INT2, "optional": True},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    is_normal={"type": ParameterChecker.BOOL, "optional": False}
)
@standardized_api_output
def get_history_cluster_diagnosis(pagesize: int = 20, current: int = 0,
                                  instance: str = None, instance_like: str = None,
                                  start_at: int = None, end_at: int = None,
                                  cluster_role: str = None, diagnosis_method: str = None,
                                  status_code: int = None, alarm_type=None, alarm_level=None,
                                  is_normal: bool = True):
    """
    Obtain the historical cluster diagnosis list

    - param pagesize: the number of records per page, which is a positive integer
    - param current: current page number, which is a non-negative integer
    - param instance: the instance of the cluster diagnosis record belongs to
    - param instance_like: the fuzzy match of the instance
    - param start_at: the start timestamp of the query interval
    - param end_at: the end timestamp of the query interval
    - param cluster_role: the role of the cluster, options -- ['cn', 'dn']
    - param diagnosis_method: the model cluster diagnosis applies, options -- ['logical', 'tree']
    - param status_code: the status code of cluster diagnosis result
    - param alarm_type: the type of alarm
    - param alarm_level: the level of alarm
    - param is_normal: whether contains normal data
    - return: The list of historical cluster diagnosis record
         e.g. {"data":{"header":["diagnosis_id","instance","timestamp","cluster_role","diagnosis_method",
              "cluster_feature","diagnosis_result", "status_code", "alarm_type", "alarm_level"],
              "rows":[[65,"ip",1684762547001,"dn","logical",{"bind_ip_failed":0,"cms_phonydead_restart":0,
              "cms_restart_pending":0,"dn_disk_damage":0,"dn_manual_stop":0,"dn_nic_down":0,"dn_ping_standby":0,
              "dn_port_conflict":0,"dn_read_only":0,"dn_status":0,"dn_writable":0,"ffic_updated":0,"ping":1},
              "DN down/disconnection"]]},"success":true}
    """
    return data_transformer.get_history_cluster_diagnosis(
        pagesize,
        current,
        instance,
        instance_like,
        start_at,
        end_at,
        cluster_role,
        diagnosis_method,
        status_code,
        alarm_type,
        alarm_level,
        is_normal
    )


@request_mapping(api_prefix + '/summary/cluster-diagnosis/count', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance={"type": ParameterChecker.INSTANCE, "optional": True},
    instance_like={"type": ParameterChecker.INSTANCE, "optional": True},
    start_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end_at={"type": ParameterChecker.TIMESTAMP, "optional": True},
    cluster_role={"type": ParameterChecker.NAME, "optional": True},
    diagnosis_method={"type": ParameterChecker.NAME, "optional": True},
    status_code={"type": ParameterChecker.INT2, "optional": True},
    alarm_type={"type": ParameterChecker.NAME, "optional": True},
    alarm_level={"type": ParameterChecker.NAME, "optional": True},
    is_normal={"type": ParameterChecker.BOOL, "optional": False}
)
@standardized_api_output
def get_history_cluster_diagnosis_count(instance: str = None, instance_like: str = None,
                                        start_at: int = None, end_at: int = None,
                                        cluster_role: str = None, diagnosis_method: str = None,
                                        status_code: int = None, alarm_type=None,
                                        alarm_level=None, is_normal: bool = True):
    """
    Obtain the number of historical cluster diagnosis records

    - param instance: the instance of the cluster diagnosis record belongs to
    - param instance_like: the fuzzy match of the instance
    - param start_at: the start timestamp of the query interval
    - param end_at: the end timestamp of the query interval
    - param cluster_role: the role of the cluster, options -- ['cn', 'dn']
    - param diagnosis_method: the model cluster diagnosis applies, options -- ['logical', 'tree']
    - param status_code: the status code of cluster diagnosis result
    - param alarm_type: the type of alarm
    - param alarm_level: the level of alarm
    - param is_normal: whether contains normal data
    - return: The number of historical cluster diagnosis records
         e.g. {"data":30,"success":true}
    """
    return data_transformer.get_history_cluster_diagnosis_count(
        instance,
        instance_like,
        start_at,
        end_at,
        cluster_role,
        diagnosis_method,
        status_code,
        alarm_type,
        alarm_level,
        is_normal
    )


@request_mapping(api_prefix + '/app/metric-diagnosis', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    metric_name={"type": ParameterChecker.NAME, "optional": False},
    metric_filter={"type": ParameterChecker.STRING, "optional": True},
    alarm_cause={"type": ParameterChecker.STRING, "optional": True},
    start={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end={"type": ParameterChecker.TIMESTAMP, "optional": True}
)
@standardized_api_output
def get_metric_diagnosis(metric_name: str, metric_filter: str, alarm_cause: str, start: int, end: int):
    """
    - param metric_name: The specified metric to be diagnosed
    - param metric_filter: The filter to indicate the very metric to be diagnosed
        e.g. {\"from_instance\":\"127.0.0.1\",\"from_job\":\"node_exporter\",\"instance\":\"127.0.0.1:8181\",
              \"job\":\"reprocessing_exporter\"}
    - param alarm_cause: The pre-set root cause for the anomaly
        e.g. [\"high_cpu_usage\"]
    - param start:  Start time for diagnosis in timestamp(ms)
    - param end:  End time for diagnosis in timestamp(ms)
    - return: The metric diagnosis result
        e.g. {"data":{[{'reason1': 0.0, 'reason2': 1.0}, 'conclusion', 'advice']},"success":true}
    """
    return data_transformer.get_root_cause_analysis(metric_name, metric_filter, start, end,
                                                    alarm_cause=alarm_cause)[:3]


@request_mapping(api_prefix + '/app/metric-diagnosis-detail', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    metric_name={"type": ParameterChecker.NAME, "optional": False},
    metric_filter={"type": ParameterChecker.STRING, "optional": True},
    alarm_cause={"type": ParameterChecker.STRING, "optional": True},
    start={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end={"type": ParameterChecker.TIMESTAMP, "optional": True},
    reason_name={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_metric_diagnosis_detail(metric_name: str, metric_filter: str, start: int, end: int,
                                alarm_cause: str = None, reason_name: str = None):
    """
    - param metric_name: The specified metric to be diagnosed
    - param metric_filter: The filter to indicate the very metric to be diagnosed
        e.g. {\"from_instance\":\"127.0.0.1\",\"from_job\":\"node_exporter\",\"instance\":\"127.0.0.1:8181\",
              \"job\":\"reprocessing_exporter\"}
    - param alarm_cause: The pre-set root cause for the anomaly
        e.g. [\"high_cpu_usage\"]
    - param start:  Start time for diagnosis in timestamp(ms)
    - param end:  End time for diagnosis in timestamp(ms)
    - param reason_name: The reason_name assumed for the anomaly
    - return: The metric diagnosis result
        e.g. {"data":{[{'reason1': 0.0, 'reason2': 1.0}, 'conclusion', 'advice']},"success":true}
    """
    return data_transformer.get_root_cause_analysis(metric_name, metric_filter, start, end,
                                                    alarm_cause=alarm_cause,
                                                    reason_name=reason_name)


@request_mapping(api_prefix + '/app/metric-diagnosis-insight', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    metric_name={"type": ParameterChecker.NAME, "optional": False},
    metric_filter={"type": ParameterChecker.STRING, "optional": True},
    alarm_cause={"type": ParameterChecker.STRING, "optional": True},
    start={"type": ParameterChecker.TIMESTAMP, "optional": True},
    end={"type": ParameterChecker.TIMESTAMP, "optional": True},
    reason_name={"type": ParameterChecker.STRING, "optional": True}
)
@standardized_api_output
def get_metric_diagnosis_insight(metric_name: str, metric_filter: str, alarm_cause: str,
                                 start: int, end: int, reason_name: str = None):
    """
    - param metric_name: The specified metric to be diagnosed
    - param metric_filter: The filter to indicate the very metric to be diagnosed
        e.g. {\"from_instance\":\"127.0.0.1\",\"from_job\":\"node_exporter\",\"instance\":\"127.0.0.1:8181\",
              \"job\":\"reprocessing_exporter\"}
    - param alarm_cause: The pre-set root cause for the anomaly
        e.g. [\"high_cpu_usage\"]
    - param start:  Start time for diagnosis in timestamp(ms)
    - param end:  End time for diagnosis in timestamp(ms)
    - param reason_name: The reason_name assumed for the anomaly
    - return: The metric diagnosis result
        e.g. {"data":{[{'reason1': 0.0, 'reason2': 1.0}, 'conclusion', 'advice']},"success":true}
    """
    return data_transformer.get_insight(metric_name, metric_filter, start, end,
                                        alarm_cause=alarm_cause, reason_name=reason_name)


@request_mapping(api_prefix + '/summary/sql-trace', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    instance_id={"type": ParameterChecker.STRING, "optional": True},
    labels={"type": ParameterChecker.STRING, "optional": True},
    is_online={"type": ParameterChecker.BOOL, "optional": False},
    from_timestamp={"type": ParameterChecker.TIMESTAMP, "optional": True},
    to_timestamp={"type": ParameterChecker.TIMESTAMP, "optional": True},
    tz={"type": ParameterChecker.TIMEZONE, "optional": True}
)
@standardized_api_output
def get_sql_trace(instance_id: str, labels: str, is_online: bool, from_timestamp: int = None,
                  to_timestamp: int = None, tz: str = None):
    """
    - param instance_id: The specified instance_id
    - param labels: the parameter that represents a specific label to filter the sequence
    - param is_online: The specified data source
    """
    return data_transformer.get_sql_trace(instance_id, is_online, labels, from_timestamp, to_timestamp, tz)


@request_mapping(api_prefix + '/security/scenarios', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_scenarios_list():
    return data_transformer.get_scenarios_list()


@request_mapping(api_prefix + '/security/scenarios/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    name={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def get_scenario_metrics(name):
    return data_transformer.get_scenario_metrics(name)


@request_mapping(api_prefix + '/summary/metric-unit', methods=['GET'], api=True)
@oauth2.token_authentication()
@ParameterChecker.define_rules(
    metric={"type": ParameterChecker.NAME, "optional": False}
)
@standardized_api_output
def get_metric_unit(metric: str):
    """
    - param metric: The metric name
    """
    return data_transformer.get_metric_unit(metric)
