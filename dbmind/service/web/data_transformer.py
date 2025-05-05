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
"""Imply that the module is responsible for
transforming data into a specific format,
 which in this case is JSON."""

import json
import datetime
import logging
import os
import threading
import time
import struct
from collections import defaultdict, Counter
from functools import wraps
from itertools import groupby

import requests
import sqlparse

from dbmind import constants
from dbmind import global_vars
from dbmind.app.monitoring import ad_pool_manager, generic_anomaly_detector, regular_inspection, monitoring_constants
from dbmind.app.monitoring.monitoring_constants import AlarmInfo, DetectorInfo
from dbmind.app.optimization import TemplateArgs
from dbmind.app.optimization.index_recommendation import rpc_index_advise
from dbmind.app.optimization.index_recommendation_rpc_executor import RpcExecutor
from dbmind.app.timed_task_utils import detect_anomaly, diagnose_cluster_state
from dbmind.common.algorithm.anomaly_detection import detectors as detector_algorithm
from dbmind.common.dispatcher import TimedTaskManager
from dbmind.common.exceptions import ModeError
from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.metric_info import METRIC_INFO
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types import ALARM_LEVEL, ALARM_TYPES
from dbmind.common.types import Sequence
from dbmind.common.utils import string_to_dict, adjust_timezone, escape_double_quote
from dbmind.common.utils.checking import (
    check_instance_valid,
    check_name_valid,
    check_string_valid,
    date_type,
    existing_special_char,
    path_type,
    prepare_ip,
    split_ip_port,
    WITH_PORT
)
from dbmind.components.anomaly_analysis import get_sequences, get_correlations
from dbmind.components.cluster_diagnosis.cluster_diagnosis import (
    cluster_diagnose,
    WINDOW_IN_MINUTES
)
from dbmind.components.cluster_diagnosis.utils import ANSWER_ORDERS, METHOD
from dbmind.components.extract_log import get_workload_template
from dbmind.components.fetch_statement.collect_workloads import (
    collect_statement_from_statement_history,
    collect_statement_from_activity,
    collect_statement_from_asp
)
from dbmind.components.forecast import early_warning
from dbmind.components.metric_diagnosis.root_cause_analysis import rca, check_params, insight_view
from dbmind.components.slow_query_diagnosis.slow_query_diagnosis import analyze_slow_query_with_rpc
from dbmind.metadatabase import dao
from dbmind.metadatabase.schema.config_dynamic_params import DynamicParams
from dbmind.service import dai
from dbmind.service.cluster_info import get_current_cn_dn_ip_set
from dbmind.service.utils import SequenceUtils

from .context_manager import ACCESS_CONTEXT_NAME, get_access_context
from .jsonify_utils import (
    sqlalchemy_query_jsonify, psycopg2_dict_jsonify,
    sqlalchemy_query_records_count_logic, sqlalchemy_query_union_records_logic
)
from .wait_events import LWLOCK_EVENTS, LOCK_EVENTS, IO_EVENTS
from ..agent_factory import DistributedAgent
from ...app.diagnosis.security.security_metrics_settings import get_security_metrics_settings
from ...app.diagnosis.security.security_scenarios import load_scenarios_yaml_definitions
from ...constants import SCENARIO_YAML_FILE_NAME, PORT_SUFFIX
from ...metadatabase.dao.security_anomalies import get_calibration_model_age_in_minutes


def microservice(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        local = threading.local()
        connection_info = args[0]
        local.user = connection_info.user
        local.pwd = connection_info.pwd
        local.master_url = f"{prepare_ip(connection_info.host)}:{connection_info.port}"
        local.token = None
        try:
            try:
                logging.info(f"Trying to create an agent context for {local.master_url}.")
                global_vars.agent_proxy_setter.set(DistributedAgent())
                global_vars.agent_proxy_setter.set_agent_info(
                    agent_mode='microservice',
                    ssl_certfile=None,
                    ssl_keyfile=None,
                    ssl_keyfile_password=None,
                    ssl_ca_file=None,
                    agent_username=local.user,
                    agent_pwd=local.pwd,
                    auto_discover_mode=None,
                    master_url=local.master_url
                )
                local.token = global_vars.agent_proxy.set(global_vars.agent_proxy_setter.get_agent())
            except Exception as e:
                raise e

            logging.info(f"Context created successfully for {local.master_url}.")
            return f(*args, **kwargs)

        except Exception as e:
            logging.exception(e)
            raise e
        finally:
            if local.token is not None:
                global_vars.agent_proxy.reset(local.token)

    return wrapper


# The following functions are
# used to override LazyFetcher so that
# we can filter sequences by custom rules, such as
# agent ip and port.
def _override_fetchall(self):
    self.rv = self._read_buffer()

    valid_addresses = get_access_context(
        ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
    i = 0
    while i < len(self.rv):
        addr = SequenceUtils.from_server(self.rv[i])
        # Filter the metric that doesn't come
        # from the valid address list.
        if WITH_PORT.match(addr) and addr not in valid_addresses:
            self.rv.pop(i)
            i -= 1
        i += 1
    return self.rv


def _override_fetchone(self):
    self.rv = self.rv or self._read_buffer()
    try:
        valid_addresses = get_access_context(
            ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        s = self.rv.pop(0)
        addr = SequenceUtils.from_server(s)
        while WITH_PORT.match(addr) and addr not in valid_addresses:
            s = self.rv.pop(0)
            addr = SequenceUtils.from_server(s)
        return s
    except IndexError:
        return Sequence()


def get_metric_sequence_internal(metric_name, from_timestamp=None, to_timestamp=None, step=None):
    """Timestamps are microsecond level."""
    if to_timestamp is None:
        to_timestamp = int(time.time() * 1000)
    if from_timestamp is None:
        from_timestamp = to_timestamp - 0.5 * 60 * 60 * 1000  # It defaults to show a half of hour.
    from_datetime = datetime.datetime.fromtimestamp(from_timestamp / 1000)
    to_datetime = datetime.datetime.fromtimestamp(to_timestamp / 1000)
    fetcher = dai.get_metric_sequence(metric_name, from_datetime, to_datetime, step)
    from_server_predicate = get_access_context(ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX)
    if from_server_predicate:
        fetcher.from_server_like(from_server_predicate)

    # monkeypatch trick
    setattr(fetcher, 'fetchall', lambda: _override_fetchall(fetcher))
    setattr(fetcher, 'fetchone', lambda: _override_fetchone(fetcher))
    return fetcher


def fetcher_filtration(fetcher, labels, regex_labels):
    if labels is not None:
        try:
            labels = json.loads(labels)
        except json.decoder.JSONDecodeError:
            labels = string_to_dict(labels)

        fetcher.filter(**labels)

    if regex_labels is not None:
        try:
            regex_labels = json.loads(regex_labels)
        except json.decoder.JSONDecodeError:
            regex_labels = string_to_dict(regex_labels)

        fetcher.filter_like(**regex_labels)

    return fetcher


def get_metric_sequence(metric_name, instance, from_timestamp=None, to_timestamp=None, step=None, fetch_all=False,
                        regex=False, labels=None, regex_labels=None, min_value=None, max_value=None, tz=None):
    # notes: this method must ensure that the front-end and back-end time are consistent
    tz = adjust_timezone(tz) if tz else tz
    if to_timestamp is None:
        to_timestamp = int(datetime.datetime.timestamp(datetime.datetime.now(tz=tz)) * 1000)
    if from_timestamp is None:
        from_timestamp = to_timestamp - 0.5 * 60 * 60 * 1000  # It defaults to show a half of hour.

    from_datetime = datetime.datetime.fromtimestamp(from_timestamp / 1000, tz=tz)
    to_datetime = datetime.datetime.fromtimestamp(to_timestamp / 1000, tz=tz)
    fetcher = dai.get_metric_sequence(metric_name, from_datetime, to_datetime, step=step,
                                      min_value=min_value, max_value=max_value)
    if instance is None:
        from_server_predicate = get_access_context(ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX)
        if from_server_predicate:
            fetcher.from_server_like(from_server_predicate)
    else:
        if regex:
            host = split_ip_port(instance)[0]
            instance = f"{prepare_ip(host)}{PORT_SUFFIX}|{host}"
            fetcher.from_server_like(instance)
        else:
            fetcher.from_server(instance)

    fetcher = fetcher_filtration(fetcher, labels, regex_labels)

    if fetch_all:
        result = fetcher.fetchall()
    else:
        result = [fetcher.fetchone()]
    return list(map(lambda s: s.jsonify(), result))


def get_latest_metric_sequence(metric_name, instance, latest_minutes, step=None, fetch_all=False, regex=False,
                               labels=None, regex_labels=None, min_value=None, max_value=None, tz=None):
    # this function can actually be replaced by 'get_metric_sequence', but in order to avoid
    # the hidden problems of that method, we add 'instance', 'fetch_all' and 'labels' to solve it
    # notes: the format of labels is 'key1=val1, key2=val2, key3=val3, ...'
    if latest_minutes is None or latest_minutes <= 0:
        fetcher = dai.get_latest_metric_value(metric_name)
    else:
        tz = adjust_timezone(tz)
        fetcher = dai.get_latest_metric_sequence(metric_name, latest_minutes, step=step, min_value=min_value,
                                                 max_value=max_value, tz=tz)

    if instance is None:
        from_server_predicate = get_access_context(ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX)
        if from_server_predicate:
            fetcher.from_server_like(from_server_predicate)

    # This logic is mainly to temporarily solve the bug caused by the listen_address assignment problem of cmd exporter
    elif instance in ['0.0.0.0', '::']:
        pass
    else:
        if regex:
            host = split_ip_port(instance)[0]
            instance = f"{prepare_ip(host)}{PORT_SUFFIX}|{host}"
            fetcher.from_server_like(instance)
        else:
            fetcher.from_server(instance)

    fetcher = fetcher_filtration(fetcher, labels, regex_labels)

    if fetch_all:
        result = fetcher.fetchall()
    else:
        result = [fetcher.fetchone()]
    return list(map(lambda s: s.jsonify(), result))


def get_metric_value(metric_name):
    fetcher = dai.get_latest_metric_value(metric_name)
    from_server_predicate = get_access_context(ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX)
    if from_server_predicate:
        fetcher.from_server_like(from_server_predicate)

    # monkeypatch trick
    setattr(fetcher, 'fetchall', lambda: _override_fetchall(fetcher))
    setattr(fetcher, 'fetchone', lambda: _override_fetchone(fetcher))
    return fetcher


def delete_metric_sequence(metric_name, instance, from_timestamp=None,
                           to_timestamp=None, regex=False, labels=None, regex_labels=None, flush=False, tz=None):
    """This function is used to manually clean up the sequence in TSDB"""
    try:
        labels = json.loads(labels) if labels is not None else dict()
    except json.decoder.JSONDecodeError:
        labels = string_to_dict(labels)
    try:
        regex_labels = json.loads(regex_labels) if regex_labels is not None else dict()
    except json.decoder.JSONDecodeError:
        regex_labels = string_to_dict(regex_labels)
    if instance is not None:
        if regex:
            host = split_ip_port(instance)[0]
            instance = f"{prepare_ip(host)}{PORT_SUFFIX}|{host}"
            regex_labels.update({"from_instance": instance})
        else:
            labels.update({"from_instance": instance})
    tz = adjust_timezone(tz)
    from_datetime = datetime.datetime.fromtimestamp(from_timestamp // 1000, tz=tz) if from_timestamp else None
    to_datetime = datetime.datetime.fromtimestamp(to_timestamp // 1000, tz=tz) if to_timestamp else None
    return dai.delete_metric_sequence(metric_name, from_datetime, to_datetime, labels, regex_labels, flush)


def stat_object_proportion(obj_read_metric, obj_hit_metric):
    obj_read = get_metric_value(obj_read_metric).fetchall()
    obj_read_tbl = defaultdict(dict)
    for s in obj_read:
        instance = SequenceUtils.from_server(s)
        datname = s.labels['datname']
        value = s.values[0]
        obj_read_tbl[instance][datname] = value
    obj_hit = get_metric_value(obj_hit_metric).fetchall()
    obj_hit_tbl = defaultdict(dict)
    for s in obj_hit:
        instance = SequenceUtils.from_server(s)
        datname = s.labels['datname']
        value = s.values[0]
        obj_hit_tbl[instance][datname] = value
    buff_hit_tbl = defaultdict(dict)
    for instance in obj_read_tbl.keys():
        for datname in obj_read_tbl[instance].keys():
            read_value = obj_read_tbl[instance][datname]
            hit_value = obj_hit_tbl[instance][datname]
            buff_hit_tbl[instance][datname] = hit_value / (hit_value + read_value + 0.0001)

    return buff_hit_tbl


def stat_buffer_hit():
    return stat_object_proportion('pg_db_blks_read', 'pg_db_blks_hit')


def stat_idx_hit():
    return stat_object_proportion('pg_index_idx_blks_read', 'pg_index_idx_blks_hit')


def stat_xact_successful():
    return stat_object_proportion('pg_db_xact_rollback', 'pg_db_xact_commit')


def stat_group_by_instance(to_agg_tbl):
    each_db = to_agg_tbl
    return {
        instance: sum(each_db[instance].values()) / len(each_db[instance])
        for instance in each_db
    }


def get_database_list():
    sequences = get_metric_value('pg_database_size_bytes').fetchall()
    rv = set()
    for seq in sequences:
        if seq.values[0] != 1:
            rv.add(seq.labels['datname'])
    return list(rv)


def get_database_list_dis(instance):
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(seconds=60)
    sequences = dai.get_metric_sequence(
        'pg_database_size_bytes', start_time=start_time, end_time=end_time,
    ).from_server(instance).fetchall()
    rv = set()
    for seq in sequences:
        if seq.values[0] != 1:
            rv.add(seq.labels['datname'])
    return list(rv)


def get_knob_recommendation_snapshot(pagesize, current):
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return sqlalchemy_query_jsonify(dao.knob_recommendation.select_metric_snapshot(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
        offset=offset, limit=limit),
        field_names=('instance', 'metric', 'value'))


def get_knob_recommendation_snapshot_count():
    return dao.knob_recommendation.count_metric_snapshot(
        get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT))


def get_knob_recommendation_warnings(pagesize, current):
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return sqlalchemy_query_jsonify(dao.knob_recommendation.select_warnings(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
        offset=offset, limit=limit),
        field_names=('instance', 'level', 'comment'))


def get_knob_recommendation_warnings_count():
    return dao.knob_recommendation.count_warnings(
        get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT))


def get_knob_recommendation(pagesize, current):
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return sqlalchemy_query_jsonify(dao.knob_recommendation.select_details(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
        offset=offset, limit=limit),
        field_names=('instance', 'name', 'current', 'recommend', 'min', 'max'))


def get_knob_recommendation_count():
    return dao.knob_recommendation.count_details(
        get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT))


def get_db_schema_table_count():
    db_set = set()
    schema_set = set()
    table_set = set()
    instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    results = dai.get_latest_metric_value('pg_tables_size_relsize').from_server(instance).fetchall()
    for res in results:
        db_name, schema_name, table_name = res.labels['datname'], res.labels['nspname'], res.labels['relname']
        db_set.add(db_name)
        schema_set.add((db_name, schema_name))
        table_set.add((db_name, schema_name, table_name))
    return len(db_set), len(schema_set), len(table_set)


def get_index_change():
    timestamps = []
    index_count_change = dict()
    recommendation_stat = dao.index_recommendation.get_recommendation_stat(
        get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT))
    for res in recommendation_stat:
        timestamp = res.occurrence_time
        if timestamp not in timestamps:
            timestamps.append(timestamp)
        if timestamp not in index_count_change:
            index_count_change[timestamp] = defaultdict(int)
        index_count_change[timestamp]['suggestions'] += res.recommend_index_count
        index_count_change[timestamp]['redundant_indexes'] += res.redundant_index_count
        index_count_change[timestamp]['invalid_indexes'] += res.invalid_index_count
    suggestions_change = {
        'timestamps': timestamps,
        'values': [index_count_change[timestamp]['suggestions'] for timestamp in timestamps]
    }
    redundant_indexes_change = {
        'timestamps': timestamps,
        'values': [index_count_change[timestamp]['redundant_indexes'] for timestamp in
                   timestamps]
    }
    invalid_indexes_change = {
        'timestamps': timestamps,
        'values': [index_count_change[timestamp]['invalid_indexes'] for timestamp in timestamps]
    }
    return {'suggestions': suggestions_change, 'redundant_indexes': redundant_indexes_change,
            'invalid_indexes': invalid_indexes_change}


def get_advised_index():
    advised_indexes = dict()
    _advised_indexes = dao.index_recommendation.get_advised_index(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT))
    advised_indexes['header'] = ['schema', 'database', 'table', 'advised_indexes', 'number_of_indexes', 'select',
                                 'update',
                                 'delete', 'insert', 'workload_improvement_rate']
    advised_indexes['rows'] = []
    group_func = lambda x: (x.schema_name, x.db_name, x.tb_name)
    for (schema_name, db_name, tb_name), group in groupby(sorted(_advised_indexes, key=group_func),
                                                          key=group_func):
        group_result = list(group)
        row = [schema_name, db_name, tb_name,
               ';'.join([x.columns for x in group_result]),
               len(group_result),
               int(group_result[0].select_ratio * group_result[0].stmt_count / 100),
               int(group_result[0].update_ratio * group_result[0].stmt_count / 100),
               int(group_result[0].delete_ratio * group_result[0].stmt_count / 100),
               int(group_result[0].insert_ratio * group_result[0].stmt_count / 100),
               sum(float(x.optimized) / len(group_result) for x in group_result)]
        advised_indexes['rows'].append(row)

    return {'advised_indexes': advised_indexes}


def get_all_metrics():
    return dai.get_all_metrics()


def update_agent_list(force=False):
    if not isinstance(force, bool):
        raise ValueError('Incorrect value for parameter force')

    if not force:
        global_vars.agent_proxy.agent_lightweight_update()
        return True

    if not global_vars.agent_proxy.agent_can_heavyweight_update():
        return False

    global_vars.agent_proxy.agent_heavyweight_update()
    return True


def get_all_agents():
    agents = global_vars.agent_proxy.agent_get_all()
    if not agents:
        global_vars.agent_proxy.agent_lightweight_update()
        # try again
        agents = global_vars.agent_proxy.agent_get_all()
    return agents


def get_current_cn_dn_instance():
    instance = set()
    cn_dn_ip_set = get_current_cn_dn_ip_set()
    for role in cn_dn_ip_set:
        instance |= cn_dn_ip_set[role]
    return list(instance)


def get_history_alarms(pagesize=None, current=None, instance=None, alarm_type=None,
                       alarm_level=None, metric_name=None, start_at=None, end_at=None,
                       anomaly_type=None, group: bool = False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    if alarm_type is not None and getattr(ALARM_TYPES, alarm_type, None) is None:
        raise ValueError('Invalid value for parameter alarm_type')
    if alarm_level is None:
        pass
    elif getattr(ALARM_LEVEL, alarm_level, None) is None:
        raise ValueError('Invalid value for parameter alarm_level')
    else:
        alarm_level = getattr(ALARM_LEVEL, alarm_level).value
    if start_at and end_at and start_at > end_at:
        raise ValueError('Start time must be earlier than end time')

    return sqlalchemy_query_union_records_logic(
        query_function=dao.alarms.select_history_alarm,
        instances=instances,
        alarm_type=alarm_type,
        alarm_level=alarm_level,
        metric_name=metric_name,
        start_at=start_at,
        end_at=end_at,
        anomaly_type=anomaly_type,
        offset=offset,
        limit=limit,
        group=group
    )


def get_history_alarms_count(instance=None, alarm_type=None, alarm_level=None,
                             metric_name=None, start_at=None, end_at=None,
                             anomaly_type=None, group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    if alarm_type is not None and getattr(ALARM_TYPES, alarm_type, None) is None:
        raise ValueError('Invalid value for parameter alarm_type')
    if alarm_level is None:
        pass
    elif getattr(ALARM_LEVEL, alarm_level, None) is None:
        raise ValueError('Invalid value for parameter alarm_level')
    else:
        alarm_level = getattr(ALARM_LEVEL, alarm_level).value
    if start_at and end_at and start_at > end_at:
        raise ValueError('Start time must be earlier than end time')

    return sqlalchemy_query_records_count_logic(
        count_function=dao.alarms.count_history_alarms,
        instances=instances,
        alarm_type=alarm_type,
        alarm_level=alarm_level,
        metric_name=metric_name,
        start_at=start_at,
        end_at=end_at,
        anomaly_type=anomaly_type,
        group=group
    )


def check_cluster_diagnosis_params(cluster_role, diagnosis_method, alarm_type, alarm_level):
    if cluster_role is not None and cluster_role not in ('cn', 'dn'):
        raise ValueError('Invalid value for parameter cluster_role')

    if diagnosis_method is not None and diagnosis_method not in ('logical', 'tree'):
        raise ValueError('Invalid value for parameter diagnosis_method')

    if alarm_type is not None and getattr(ALARM_TYPES, alarm_type, None) is None:
        raise ValueError('Invalid value for parameter alarm_type')

    if alarm_level is None:
        pass
    elif getattr(ALARM_LEVEL, alarm_level, None) is None:
        raise ValueError('Invalid value for parameter alarm_level')
    else:
        alarm_level = getattr(ALARM_LEVEL, alarm_level).value

    return alarm_level


def get_history_cluster_diagnosis(pagesize=None, current=None, instance=None, instance_like=None,
                                  start_at=None, end_at=None, cluster_role=None, diagnosis_method=None,
                                  status_code=None, alarm_type=None, alarm_level=None, is_normal=True):
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    alarm_level = check_cluster_diagnosis_params(cluster_role, diagnosis_method, alarm_type, alarm_level)
    if instance is None:
        instance = get_current_cn_dn_instance()

    diagnosis_args = {
        "instance": instance,
        "instance_like": instance_like,
        "cluster_role": cluster_role,
        "diagnosis_method": diagnosis_method,
        "start_at": start_at,
        "end_at": end_at,
        "status_code": status_code,
        "alarm_type": alarm_type,
        "alarm_level": alarm_level,
        "is_normal": is_normal
    }
    result = dao.cluster_diagnosis_records.select_history_cluster_diagnosis(
        offset=offset,
        limit=limit,
        **diagnosis_args
    )
    field_names = result.statement.columns.keys()
    return sqlalchemy_query_jsonify(result, field_names)


def get_history_cluster_diagnosis_count(instance=None, instance_like=None, start_at=None, end_at=None,
                                        cluster_role=None, diagnosis_method=None, status_code=None,
                                        alarm_type=None, alarm_level=None, is_normal=True):
    alarm_level = check_cluster_diagnosis_params(cluster_role, diagnosis_method, alarm_type, alarm_level)
    if instance is None:
        instance = get_current_cn_dn_instance()

    diagnosis_args = {
        "instance": instance,
        "instance_like": instance_like,
        "cluster_role": cluster_role,
        "diagnosis_method": diagnosis_method,
        "start_at": start_at,
        "end_at": end_at,
        "status_code": status_code,
        "alarm_type": alarm_type,
        "alarm_level": alarm_level,
        "is_normal": is_normal
    }
    return dao.cluster_diagnosis_records.count_history_cluster_diagnosis(**diagnosis_args)


def get_slow_queries(pagesize=None, current=None, instance=None, query=None, start_time=None, end_time=None,
                     group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    if query and len(sqlparse.split(query)) > 1:
        query = sqlparse.split(query)[0]
    return sqlalchemy_query_union_records_logic(
        query_function=dao.slow_queries.select_slow_queries,
        instances=instances, only_with_port=True,
        target_list=(),
        query=query,
        start_time=start_time,
        end_time=end_time,
        offset=offset,
        limit=limit,
        group=group
    )


def get_slow_queries_count(instance=None, distinct=False, query=None, start_time=None, end_time=None, group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    if query and len(sqlparse.split(query)) > 1:
        query = sqlparse.split(query)[0]
    return sqlalchemy_query_records_count_logic(
        count_function=dao.slow_queries.count_slow_queries,
        instances=instances, only_with_port=True,
        distinct=distinct, query=query,
        start_time=start_time, end_time=end_time, group=group)


def get_killed_slow_queries(pagesize=None, current=None, instance=None, query=None, start_time=None, end_time=None):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    if query and len(sqlparse.split(query)) > 1:
        query = sqlparse.split(query)[0]
    return sqlalchemy_query_union_records_logic(
        query_function=dao.slow_queries.select_killed_slow_queries,
        instances=instances, only_with_port=True,
        query=query, start_time=start_time, end_time=end_time, offset=offset, limit=limit
    )


def get_killed_slow_queries_count(instance=None, query=None, start_time=None, end_time=None):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    if query and len(sqlparse.split(query)) > 1:
        query = sqlparse.split(query)[0]
    return sqlalchemy_query_records_count_logic(
        count_function=dao.slow_queries.count_killed_slow_queries,
        instances=instances, only_with_port=True,
        query=query, start_time=start_time, end_time=end_time)


def get_slow_query_summary():
    # Maybe multiple nodes, but we don't need to care.
    # Because that is an abnormal scenario.
    sequence = get_metric_value(
        'pg_settings_setting'
    ).filter(
        name='log_min_duration_statement'
    ).fetchone()
    # fix the error which occurs in the interface of DBMind
    if dai.is_sequence_valid(sequence):
        threshold = sequence.values[-1]
    else:
        threshold = 'Nan'

    return {
        'nb_unique_slow_queries': dao.slow_queries.count_slow_queries(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'slow_query_threshold': threshold,
        'main_slow_queries': dao.slow_queries.count_slow_queries(
            distinct=True, instance=get_access_context(
                ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT
            )
        ),
        'statistics_for_database': dao.slow_queries.group_by_dbname(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'statistics_for_schema': dao.slow_queries.group_by_schema(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'systable': dao.slow_queries.count_systable(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'slow_query_count': dao.slow_queries.slow_query_trend(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'distribution': dao.slow_queries.slow_query_distribution(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'mean_cpu_time': dao.slow_queries.mean_cpu_time(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'mean_io_time': dao.slow_queries.mean_io_time(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'mean_buffer_hit_rate': dao.slow_queries.mean_buffer_hit_rate(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'mean_fetch_rate': dao.slow_queries.mean_fetch_rate(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
        ),
        'slow_query_template': sqlalchemy_query_jsonify(
            dao.slow_queries.slow_query_template(
                instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
            ),
            ['template_id', 'count', 'query'])
    }


def get_top_queries(username, password):
    stmt = """
        SELECT 
            user_name,
            unique_sql_id,
            query,
            n_calls,
            min_elapse_time,
            max_elapse_time,
            case when (n_calls = 0) then total_elapse_time else total_elapse_time / n_calls end as avg_elapse_time,
            n_returned_rows,
            n_tuples_fetched,
            n_tuples_returned,
            n_tuples_inserted,
            n_tuples_updated,
            n_tuples_deleted,
            n_blocks_fetched,
            n_blocks_hit,
            n_soft_parse,
            n_hard_parse,
            db_time,
            cpu_time,
            execution_time,
            parse_time,
            plan_time,
            rewrite_time,
            sort_count,
            sort_time,
            sort_mem_used,
            sort_spill_count,
            sort_spill_size,
            hash_count,
            hash_time,
            hash_mem_used,
            hash_spill_count,
            hash_spill_size,
            last_updated
        FROM 
            dbe_perf.statement
        ORDER BY 
            n_calls DESC,
            avg_elapse_time DESC
        LIMIT 50;
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    return psycopg2_dict_jsonify(res)


def get_active_query(username, password):
    stmt = """
        SELECT 
            datname,
            pid,
            sessionid,
            query_id,
            usename,
            application_name,
            backend_start,
            xact_start,
            query_start,
            waiting,
            state,
            query
        FROM 
            pg_catalog.pg_stat_activity
        WHERE  
            query_id != 0 
            AND  
            application_name != 'DBMind-openGauss-exporter' 
        ORDER BY 
            query_start DESC;
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    return psycopg2_dict_jsonify(res)


def get_holding_lock_query(username, password):
    stmt = """
        SELECT 
            datname, 
            pid, 
            sessionid, 
            usename, 
            application_name, 
            backend_start, 
            xact_start, 
            query_start, 
            waiting, 
            state, 
            query 
        FROM 
            pg_catalog.pg_stat_activity 
        WHERE 
            waiting = 't';
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    return psycopg2_dict_jsonify(res)


def check_credential(username, password, scopes=None):
    if global_vars.is_distribute_mode:
        return False, 'DBMind is deployed as distribute mode, v1 API is not allowed.'
    rpc = None
    logging.info("using scopes %s, agents %s.", scopes, global_vars.agent_proxy.agent_get_all().keys())
    if not scopes:
        rpc = global_vars.agent_proxy.current_rpc()
    else:
        logging.debug("Login user %s using scopes '%s'.", username, scopes)
        for instance_addr in scopes:
            if global_vars.agent_proxy.has(instance_addr):
                rpc = global_vars.agent_proxy.get(instance_addr)
                break
    if not rpc:
        return False, 'Unable to find RPC service based on the input scope,' \
                      ' please check your instance and your opengauss_exporter status.'
    return rpc.handshake(username, password, receive_exception=True)


def toolkit_index_advise(username, password, instance, database, sqls, max_index_num, max_index_storage):
    with global_vars.agent_proxy.context(instance, username, password):
        result = sqlparse.split(sqls)
        schema_query = [
            "select distinct(nspname) FROM pg_catalog.pg_namespace nsp JOIN pg_catalog.pg_class rel ON "
            "nsp.oid = rel.relnamespace WHERE nspname NOT IN "
            "('pg_catalog', 'information_schema','snapshot', "
            "'dbe_pldeveloper', 'db4ai', 'dbe_perf') AND rel.relkind = 'r';"
        ]
        executor = RpcExecutor(database, None, None, None, None, 'public')
        schemas = executor.execute_sqls(schema_query)
        if not schemas:
            raise Exception('The instance or database may be incorrect.')
        schema_names = ','.join('"' + escape_double_quote(schema[0]) + '"' for schema in schemas)
        database_templates = dict()
        get_workload_template(database_templates, result, TemplateArgs(10, 5000))
        executor = RpcExecutor(database, None, None, None, None, schema_names)

        # switch instances before each execution, subsequent optimization needed.
        def execute_sqls(queries):
            with global_vars.agent_proxy.context(instance, username, password):
                return RpcExecutor(database, None, None, None, None, schema_names).execute_sqls(queries)

        executor.execute_sqls = execute_sqls
        detail_info = rpc_index_advise(executor, database_templates, max_index_num, max_index_storage)
        if detail_info is None:
            return []
        return index_advise_final_result(detail_info)


@microservice
def toolkit_index_advise_dis(connection_info, database, max_index_num, max_index_storage):
    return toolkit_index_advise(connection_info.user, connection_info.pwd,
                                connection_info.host + ':' + connection_info.port, database,
                                ''.join(connection_info.sqls), max_index_num, max_index_storage)


def index_advise_final_result(detail_info):
    final_result = dict()
    recommend_indexes = detail_info['recommendIndexes']
    useless_indexes = detail_info['uselessIndexes']
    useless_list = []
    redundant_list = []
    recommend_list = []
    for element in useless_indexes:
        if element['type'] == 3:
            useless_list.append(element)
        if element['type'] == 2:
            redundant_list.append(element)
    final_result['redundant_indexes'] = redundant_list
    final_result['useless_indexes'] = useless_list
    for item in recommend_indexes:
        advise_index = dict()
        advise_index['index'] = item['statement']
        advise_index['improve_rate'] = item['workloadOptimized'] + '%'
        advise_index['index_size'] = str('%.2f' % float(item['storage'])) + "MB"
        advise_index['select'] = '%.2f' % item['selectRatio']
        advise_index['delete'] = '%.2f' % item['deleteRatio']
        advise_index['update'] = '%.2f' % item['updateRatio']
        advise_index['insert'] = '%.2f' % item['insertRatio']
        detail_list = []
        for detail in item['sqlDetails']:
            detail_dict = dict()
            if detail['correlationType'] == 1:
                detail_dict['template'] = detail['sqlTemplate']
                detail_dict['count'] = detail['sqlCount']
                detail_dict['improve'] = detail['optimized'] + '%'
                detail_list.append(detail_dict)
        advise_index['templates'] = detail_list
        recommend_list.append(advise_index)
    final_result['advise_indexes'] = recommend_list
    final_result['total'] = len(recommend_list)
    return final_result, dict()


def toolkit_rewrite_sql(username, password, instance, database, sqls):
    with global_vars.agent_proxy.context(instance, username, password):
        return rewrite_sql_api(database, sqls)


def toolkit_slow_query_rca(username, password, **params):
    instance = global_vars.agent_proxy.current_agent_addr()
    with global_vars.agent_proxy.context(instance, username, password):
        query = params.pop('query', '')
        if query and len(sqlparse.split(query)) > 1:
            query = sqlparse.split(query)[0]
        db_name = params.pop('db_name', '')
        schema_name = params.get('schema_name', 'public')
        schema_name = schema_name.split(',')[-1] if ',' in schema_name else schema_name
        if not check_name_valid(db_name) or not check_name_valid(schema_name):
            raise ValueError('invalid schema or db_name.')
        return analyze_slow_query_with_rpc(query, db_name, **params)


@microservice
def toolkit_slow_query_rca_dis(connection_info, **params):
    return toolkit_slow_query_rca(connection_info.user, connection_info.pwd, **params)


def search_slow_query_rca_result(sql, start_time=None, end_time=None, limit=None):
    root_causes, suggestions = [], []
    default_interval = 120 * 1000
    if end_time is None:
        end_time = int(datetime.datetime.now().timestamp()) * 1000
    if start_time is None:
        start_time = end_time - default_interval
    field_names = ('root_cause', 'suggestion')
    # Maximum number of output lines.
    result = dao.slow_queries.select_slow_queries(field_names, sql, start_time, end_time, limit=limit)
    for slow_query in result:
        root_causes.append(getattr(slow_query, 'root_cause').split('\n'))
        suggestions.append(getattr(slow_query, 'suggestion').split('\n'))
    return root_causes, suggestions


def get_regular_inspections(inspection_type):
    if inspection_type not in ('daily_check', 'weekly_check', 'monthly_check'):
        raise ValueError('Incorrect value for parameter inspection_type')

    return sqlalchemy_query_jsonify(
        dao.regular_inspections.select_metric_regular_inspections(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
            inspection_type=inspection_type,
            limit=1
        ),
        field_names=['instance', 'report', 'start', 'end']
    )


def get_regular_inspections_count(inspection_type):
    if inspection_type not in ('daily_check', 'weekly_check', 'monthly_check'):
        raise ValueError('Incorrect value for parameter inspection_type')

    return dao.regular_inspections.count_metric_regular_inspections(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
        inspection_type=inspection_type
    )


def exec_real_time_inspections(username, password, inspection_type, start_time, end_time,
                               instance, inspection_items, tz):
    if not instance:
        raise ValueError('Incorrect value for parameter instance: {}.'.format(instance))

    if inspection_type not in ('daily_check', 'weekly_check', 'monthly_check', 'real_time_check'):
        raise ValueError('Incorrect value for parameter inspection_type: {}.'.format(inspection_type))

    if not inspection_items:
        raise ValueError('Incorrect value for parameter inspection_items: {}.'.format(inspection_items))

    for attribute, inspection_info in inspection_items.__dict__.items():
        if len(inspection_info) == 0:
            continue

        if not (
            all(isinstance(item, str) for item in inspection_info) or
            all(isinstance(item, dict) for item in inspection_info)
        ):
            raise ValueError(
                f'the values in inspection_items.{attribute} should be the same type, '
                'and the type must be str or dict.'
            )

        if all(isinstance(item, dict) for item in inspection_info) and len(inspection_info) != 1:
            raise ValueError(f'the length of dict in inspection_items.{attribute} must be 1.')

    agent_instance = global_vars.agent_proxy.current_agent_addr()
    with global_vars.agent_proxy.context(agent_instance, username, password):
        inspect_result = regular_inspection.real_time_inspection(
            username, password, inspection_type, start_time,
            end_time, instance, inspection_items, tz
        )

    return inspect_result


def list_real_time_inspections(instance):
    if not instance:
        raise ValueError('Incorrect value for parameter instance: {}.'.format(instance))

    return sqlalchemy_query_jsonify(
        dao.regular_inspections.select_metric_regular_inspections(
            instance=instance,
            show_report=False
        ),
        field_names=['instance', 'start', 'end', 'id', 'state', 'cost_time', 'inspection_type']
    )


def report_real_time_inspections(instance, spec_id):
    if not instance:
        raise ValueError('Incorrect value for parameter instance: {}.'.format(instance))

    if not (isinstance(spec_id, str) and spec_id.isdigit()):
        raise ValueError('Incorrect value for parameter spec_id: {}.'.format(spec_id))

    return sqlalchemy_query_jsonify(
        dao.regular_inspections.select_metric_regular_inspections(
            instance=instance,
            spec_id=int(spec_id)
        ),
        field_names=['instance', 'report', 'start', 'end', 'id', 'state', 'cost_time', 'inspection_type']
    )


def delete_real_time_inspections(instance, spec_id):
    if not instance:
        raise ValueError('Incorrect value for parameter instance: {}.'.format(instance))

    if not spec_id:
        raise ValueError('Incorrect value for parameter spec_id: {}.'.format(spec_id))

    inspect_state = None
    spec_ids = spec_id.split(",")
    for s_id in spec_ids:
        if not (isinstance(s_id, str) and s_id.isdigit()):
            raise ValueError('Incorrect value for parameter spec_id: {}.'.format(s_id))

        inspect_state = dao.regular_inspections.delete_metric_regular_inspections(instance, int(s_id))

    return {'success': True if inspect_state == 'success' else False}


def get_correlation_result(metric_name, instance, start_time, end_time,
                           metric_filter=None, topk=10):
    client = TsdbClientFactory.get_tsdb_client()
    all_metrics = client.all_metrics
    # In case the length from start_time to end_time is too short,
    # we fix the least window to 20 intervals or 300 seconds.
    least_window = 300  # empirical
    least_n_intervals = 20  # empirical
    tsdb_interval = client.scrape_interval
    start_time, end_time = int(start_time) // 1000, int(end_time) // 1000
    if isinstance(tsdb_interval, (float, int)):
        window = tsdb_interval * least_n_intervals
    else:
        window = least_window

    end_time = min(int(datetime.datetime.now().timestamp()), end_time + window)
    start_time = end_time - window * 2
    start_datetime = datetime.datetime.fromtimestamp(start_time)
    end_datetime = datetime.datetime.fromtimestamp(end_time)

    sequence_args = [((metric, instance, start_datetime, end_datetime),) for metric in all_metrics]
    try:
        metric_filter = json.loads(metric_filter) if isinstance(metric_filter, str) else dict()
    except json.decoder.JSONDecodeError:
        metric_filter = string_to_dict(metric_filter)
    fetcher = dai.get_metric_sequence(metric_name, start_datetime, end_datetime).filter(**metric_filter)
    this_sequence = fetcher.from_server(instance).fetchone()
    if len(this_sequence) == 0:
        raise ValueError(f'No sequence fetched for the main metric: {metric_name} with {metric_filter}.')

    sequence_results = global_vars.worker.parallel_execute(
        get_sequences, sequence_args
    ) or []

    if all(sequences is None for sequences in sequence_results):
        raise ValueError('The sequence_results is all None.')

    correlation_results = dict()
    this_name = metric_name + " from " + instance
    correlation_args = list()
    for sequences in sequence_results:
        for name, sequence in sequences:
            correlation_args.append(((name, sequence, this_sequence),))

    correlation_results[this_name] = global_vars.worker.parallel_execute(
        get_correlations, correlation_args
    ) or []

    correlation_results[this_name].sort(key=lambda item: item[1], reverse=True)
    del (correlation_results[this_name][topk:])

    return correlation_results


def get_timed_task_status():
    detail = {'header': ['name', 'current_status', 'running_interval'], 'rows': []}
    rows_list = []
    for timed_task, _ in global_vars.timed_task.items():
        detail_list = [timed_task]
        if TimedTaskManager.check(timed_task):
            if TimedTaskManager.is_alive(timed_task):
                detail_list.append('Running')
                detail_list.append(TimedTaskManager.get_interval(timed_task))
        else:
            detail_list.append('Stopping')
            detail_list.append(0)
        rows_list.append(detail_list)
    detail['rows'] = rows_list
    return detail


def risk_analysis(metric, instance, warning_hours, upper, lower, labels, tz=None):
    if instance is None:
        raise ValueError(f'Incorrect value for parameter instance.')

    try:
        labels = json.loads(labels) if labels is not None else None
    except json.decoder.JSONDecodeError:
        labels = string_to_dict(labels)

    upper = float("inf") if upper is None else upper
    lower = 0 if lower is None else lower
    warnings = early_warning(metric, instance, None, warning_hours, upper, lower, labels, tz=tz)
    return warnings


def get_database_data_directory_status(instance, latest_minutes):
    # instance: address of instance agent, format is 'host:port'
    # get the growth_rate of disk usage where the database data directory is located
    return dai.get_database_data_directory_status(instance, latest_minutes)


def get_current_instance():
    instance = global_vars.agent_proxy.current_agent_addr()
    return instance


def get_front_overview(latest_minutes=5):
    overview_detail = {'status': 'stopping', 'strength_version': 'unknown', 'deployment_mode': 'unknown',
                       'operating_system': 'unknown', 'general_risk': 0, 'major_risk': 0, 'high_risk': 0, 'low_risk': 0}
    # this method can be used to front-end
    # instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    instance = get_current_instance()
    if instance is None:
        return overview_detail
    instance_without_port = split_ip_port(instance)[0]
    instance_regex = prepare_ip(split_ip_port(instance_without_port)[0]) + PORT_SUFFIX

    # get the status of instance
    overview_detail['status'] = dai.check_instance_status().get('status')

    # get version of opengauss
    version_sequence = dai.get_latest_metric_value('pg_node_info_uptime').from_server(instance).fetchone()
    if dai.is_sequence_valid(version_sequence):
        overview_detail['strength_version'] = version_sequence.labels['version']

    # get version of system
    operating_system_sequence = dai.get_latest_metric_value(
        'node_uname_info'
    ).filter_like(instance=instance_regex).fetchone()
    if dai.is_sequence_valid(operating_system_sequence):
        overview_detail['operating_system'] = operating_system_sequence.labels['machine']

    # get summary of alarm between start at and end_at
    end_time = int(time.time() * 1000)
    start_time = end_time - latest_minutes * 60 * 1000
    history_alarms = dao.alarms.select_history_alarm(
        instance=instance,
        start_at=start_time,
        end_at=end_time
    ).all()
    alarm_level = [item.alarm_level for item in history_alarms]
    for item, times in Counter(alarm_level).items():
        if times >= 0 and item == ALARM_LEVEL.WARNING.value:
            overview_detail['general_risk'] = times
        elif times >= 0 and item == ALARM_LEVEL.CRITICAL.value:
            overview_detail['major_risk'] = times
        elif times >= 0 and item == ALARM_LEVEL.ERROR.value:
            overview_detail['high_risk'] = times
        elif times >= 0 and item == ALARM_LEVEL.INFO.value:
            overview_detail['low_risk'] = times

    # get deployment mode of instance
    # need to change to: clusters = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
    clusters = global_vars.agent_proxy.current_cluster_instances()
    if len(clusters) == 1:
        overview_detail['deployment_mode'] = 'single'
    elif len(clusters) > 1:
        cluster_sequence = dai.get_latest_metric_value('opengauss_cluster_state').fetchone()
        if 'cn_state' in cluster_sequence.labels.keys():
            overview_detail['deployment_mode'] = 'distributed'
        else:
            overview_detail['deployment_mode'] = 'centralized'

    return overview_detail


def get_agent_status():
    agent_status = dai.check_agent_status()
    agent_status['status'] = True if agent_status['status'] == 'up' else False
    return agent_status


def get_current_instance_status():
    detail = {'header': ['instance', 'role', 'state'], 'rows': []}
    instance_status = dai.check_instance_status()
    primary = instance_status.get('primary')
    normal_list = instance_status.get('normal')
    status = instance_status.get('status')
    if instance_status.get('deployment_mode', '') == 'single':
        detail['rows'].append([primary, 'primary', status == 'normal'])
    elif instance_status.get('deployment_mode', '') == 'centralized':
        detail['rows'].append([primary, 'primary', primary in normal_list])
        for instance in instance_status['standby']:
            detail['rows'].append([instance, 'standby', instance in normal_list])
        for instance in instance_status['abnormal']:
            detail['rows'].append([instance, '', False])
    elif instance_status.get('deployment_mode', '') == 'distributed':
        cn_list = instance_status.get('cn')
        standby_list = instance_status.get('standby')
        for cn in cn_list:
            detail['rows'].append([cn, 'cn', True])
        for instance in primary:
            detail['rows'].append([instance, 'dn_primary', instance in normal_list])
        for instance in standby_list:
            detail['rows'].append([instance, 'dn_standby', instance in normal_list])
        for instance in instance_status['abnormal']:
            detail['rows'].append([instance, '', False])
    return detail


def get_detector_init_defaults():
    return ad_pool_manager.get_detector_init_defaults()


class DetectorOperation:
    def __init__(self, name):
        self.name = name
        self.primary = ""
        self.nodes = list()

    def refresh_node_info(self):
        self.primary = get_current_instance()
        self.nodes = global_vars.agent_proxy.agent_get_all().get(self.primary, [])
        if self.primary not in self.nodes:
            self.nodes.append(self.primary)

    def add(self, json_dict, fuzzy_match=True):
        self.refresh_node_info()
        if self.name in monitoring_constants.LONG_TERM_DETECTOR_NAMES:
            json_dict["duration"] = 0
        elif json_dict["duration"] <= 0:
            raise ValueError(f"duration({json_dict['duration']}) must be positive.")

        return ad_pool_manager.add_detector(self.primary, self.nodes,
                                            self.name, json_dict,
                                            fuzzy_match=fuzzy_match)

    def delete(self):
        self.refresh_node_info()
        return ad_pool_manager.delete_detector(self.nodes, self.name)

    def pause(self):
        self.refresh_node_info()
        return ad_pool_manager.pause_detector(self.nodes, self.name)

    def resume(self):
        self.refresh_node_info()
        return ad_pool_manager.resume_detector(self.nodes, self.name)

    def view(self):
        self.refresh_node_info()
        return ad_pool_manager.view_detector(self.nodes, self.name)

    def rebuild(self):
        self.refresh_node_info()
        return ad_pool_manager.rebuild_detector(self.nodes)

    def clear(self):
        self.refresh_node_info()
        return ad_pool_manager.clear_detector(self.nodes)


def get_collection_system_status():
    collection_detail = {'header': ['component', 'listen_address', 'is_alive'], 'rows': [], 'suggestions': []}
    tsdb_status = dai.check_tsdb_status()
    exporter_status = dai.check_exporter_status()
    collection_detail['suggestions'] = dai.diagnosis_exporter_status(exporter_status)
    for component, details in exporter_status.items():
        for detail in details:
            listen_address = detail['listen_address']
            status = True if detail['status'] == 'up' else False
            collection_detail['rows'].append([component, listen_address, status])
    collection_detail['rows'].append([tsdb_status['name'],
                                      tsdb_status['listen_address'],
                                      True if tsdb_status['status'] == 'up' else False])
    return collection_detail


def get_connection_status(instance, ssl_context):
    """Check for instance connection status."""
    for scheme in ('http', 'https'):
        url = "{}://{}".format(scheme, instance)
        try:
            with create_requests_session(ssl_context=ssl_context, timeout=0.1, max_retry=0) as session:
                response = session.get(
                    url,
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 200:
                    return True
                if response.status_code == 401:
                    continue
        except requests.RequestException:
            continue

    return False


def get_exporter_up_status(arg):
    """get exporter up status."""
    exporter_status = {}
    up_info, exporter_type, ssl_context = arg
    if exporter_type is None:
        if up_info["labels"]["job"] not in ('opengauss_exporter', 'cmd_exporter', 'reprocessing_exporter'):
            return exporter_status
    elif up_info["labels"]["job"] != exporter_type:
        return exporter_status
    exporter_status["component"] = up_info["labels"]["job"]
    exporter_status["endpoint"] = up_info["labels"]["instance"]
    exporter_status["is_alive"] = get_connection_status(up_info["labels"]["instance"], ssl_context)
    return exporter_status


def get_exporter_status(exporter_type, ssl_context):
    """Collecting statistics on exporter status information."""
    flag = False
    if not ssl_context:
        flag = True
    elif not (ssl_context.ssl_certfile or ssl_context.ssl_keyfile or ssl_context.ssl_ca_file):
        flag = True
    elif ssl_context.ssl_certfile and ssl_context.ssl_keyfile:
        path_type(ssl_context.ssl_certfile)
        path_type(ssl_context.ssl_keyfile)
        os.chmod(ssl_context.ssl_certfile, 0o400)
        os.chmod(ssl_context.ssl_keyfile, 0o400)
        if ssl_context.ssl_ca_file:
            path_type(ssl_context.ssl_ca_file)
            os.chmod(ssl_context.ssl_ca_file, 0o400)

        if ssl_context.ssl_keyfile_password and (not check_string_valid(ssl_context.ssl_keyfile_password)):
            raise Exception('ssl_context is not valid')

        flag = True

    if not flag:
        raise Exception('ssl_context is not valid')

    valid_exporter_list = ('opengauss_exporter', 'cmd_exporter', 'reprocessing_exporter')
    if exporter_type is not None and exporter_type not in valid_exporter_list:
        raise Exception(
            'The exporter_type is not valid, use proper '
            f'exporter_type in {valid_exporter_list}.'
        )

    client = TsdbClientFactory.get_tsdb_client()
    if not client.check_connection():
        raise Exception(f'tsdb is disconnected, please check tsdb status.')

    up_sequences = dai.get_latest_metric_value("up").fetchall()
    up_list = list(map(lambda s: s.jsonify(), up_sequences))
    up_args = [((up_info, exporter_type, ssl_context),) for up_info in up_list]
    exporter_status_list = global_vars.worker.parallel_execute(
        get_exporter_up_status, up_args
    ) or []

    return list(filter(None, exporter_status_list))


def collect_workloads(username, password, data_source, databases, schemas, start_time,
                      end_time, db_users, sql_types, template_id, duration=60):
    if data_source not in ('asp', 'dbe_perf.statement_history', 'pg_stat_activity'):
        raise ValueError('Incorrect value for parameter data_source')
    if databases is not None and existing_special_char(databases):
        raise ValueError('Invalid value for parameter databases')
    if schemas is not None and existing_special_char(schemas):
        raise ValueError('Invalid value for parameter schemas')
    if db_users is not None and existing_special_char(db_users):
        raise ValueError('Invalid value for parameter db_users')
    if sql_types is not None and existing_special_char(sql_types):
        raise ValueError('Invalid value for parameter sql_types')
    # transfer timestamps to string format
    if start_time:
        start_time = time.strftime("%Y-%m-%d %H:%M:%S%z", time.localtime(start_time // 1000))
    if end_time:
        end_time = time.strftime("%Y-%m-%d %H:%M:%S%z", time.localtime(end_time // 1000))
    else:
        end_time = time.strftime("%Y-%m-%d %H:%M:%S%z", time.localtime(time.time()))

    # Convert units from ms to s
    duration /= 1000
    if data_source == 'pg_stat_activity':
        stmts = collect_statement_from_activity(databases, db_users, sql_types, duration=duration)
    elif data_source == 'dbe_perf.statement_history':
        stmts = collect_statement_from_statement_history(
            databases, schemas, start_time, end_time, db_users, sql_types, template_id, duration=duration)
    else:
        stmts = collect_statement_from_asp(databases, start_time, end_time, db_users, sql_types)
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmts)
    return psycopg2_dict_jsonify(res)


@microservice
def collect_workloads_dis(connection_info, data_source, databases, schemas, start_time,
                          end_time, db_users, sql_types, template_id, duration=60):
    return collect_workloads(connection_info.user, connection_info.pwd, data_source, databases, schemas,
                             start_time, end_time, db_users, sql_types, template_id, duration)


def get_cluster_diagnosis(instance, role, method, timestamp):
    end_datetime = datetime.datetime.fromtimestamp(timestamp / 1000)
    start_datetime = end_datetime - datetime.timedelta(minutes=WINDOW_IN_MINUTES)
    features, status_code = cluster_diagnose(
        instance,
        role,
        start_datetime,
        end_datetime,
        method=method
    )
    result = ANSWER_ORDERS[role].get(status_code, "Unknown")
    return features, result


def get_local_dynamic_configs():
    config_dict = {}
    for section, param_list in DynamicParams.__default__.items():
        config_dict[section] = param_list
    return config_dict


class DetailsParser:

    def __init__(self, byte_details: bytes):
        self.byte_details = byte_details
        self.endian_type = self.get_endian_type()

    def split_areas(self, areas):
        left = areas
        while left:
            area_type = left[0]
            area_len = struct.unpack(self.endian_type + 'I', left[1:5])[0]
            current_area = left[9:5 + area_len]
            yield area_type, current_area
            left = areas[5 + area_len:]

    def get_endian_type(self):
        if self.byte_details[0] == 1:
            return '<'
        else:
            return '>'

    def parse_areas(self):
        useless_length = 4
        areas = self.byte_details[useless_length + 6:]
        return self.split_areas(areas)

    def parse_wait_events(self, encoded_wait_events):
        left = encoded_wait_events
        while left:
            wait_type = left[0]
            wait_name_len = struct.unpack(self.endian_type + 'H', left[1:3])[0]
            wait_name = left[3:3 + wait_name_len]
            duration = struct.unpack(self.endian_type + 'Q', left[3 + wait_name_len:3 + wait_name_len + 8])[0]
            yield wait_type, wait_name[:-1].decode(), duration
            left = left[3 + wait_name_len + 8:]


class FixedSizeList:
    def __init__(self, max_length):
        self.max_length = max_length
        self.list = []

    def append(self, element):
        if len(self.list) < self.max_length:
            self.list.append(element)

    def values(self):
        return self.list


def get_sql_trace(instance_id, is_online, labels,
                  from_timestamp=None, to_timestamp=None,
                  tz=None):
    if not all(i.isalnum() for i in instance_id.split('_')):
        raise ValueError("Incorrect value for parameter instance_id.")

    base_info = ["component_id", "node_id", "transaction_id", "unique_query_id",
                 "debug_query_id", "db_name", "schema_name", "start_time",
                 "finish_time", "user_name", "client_addr", "client_port",
                 "trace_id", "application_name", "session_id", "is_slow_sql"]

    results = []
    metric_name = f'full_sql_online_{instance_id}' if is_online else f'full_sql_offline_{instance_id}'
    tz = adjust_timezone(tz)
    if from_timestamp is None:
        from_timestamp = int((datetime.datetime.now(tz=tz) - datetime.timedelta(days=14)).timestamp() * 1000)

    to_timestamp = int(to_timestamp) if to_timestamp else int(datetime.datetime.now(tz=tz).timestamp() * 1000)
    sequences = []
    fetch_step = 60 * 15 * 1000
    if labels is not None:
        try:
            labels = json.loads(labels)
        except json.decoder.JSONDecodeError:
            labels = string_to_dict(labels)

    for _timestamp in range(from_timestamp, to_timestamp, fetch_step):
        fetcher = dai.get_metric_sequence(
            metric_name,
            datetime.datetime.fromtimestamp(int(_timestamp / 1000), tz=tz),
            datetime.datetime.fromtimestamp(int((_timestamp + fetch_step) / 1000), tz=tz),
            step=1000
        )
        if labels is not None:
            fetcher.filter(**labels)

        sequences += fetcher.fetchall()

    for _seq in sequences:
        seq = _seq.labels
        if not seq:
            continue

        data_io_time = int(seq['data_io_time'])
        lock_time = int(seq['lock_time'])
        lwlock_time = int(seq['lwlock_time'])
        db_time = int(seq['db_time'])
        details = bytes(seq['details'][2:-1], encoding="latin1").decode('unicode_escape').encode('latin1')
        execution_time_details = {
            'resource_time': {
                'all_time': db_time,
                'resource_time_details': {
                    'cpu_time': int(seq['cpu_time']),
                    'data_io_time': data_io_time,
                    'other_time': db_time - int(seq['cpu_time']) - data_io_time
                }
            }
        }
        kernel_other_time = (
                db_time - int(seq['parse_time']) -
                int(seq['rewrite_time']) -
                int(seq['plan_time']) -
                int(seq['execution_time'])
        )
        execution_time_details.update({
            'kernel_time': {
                'all_time': db_time,
                'kernel_time_details': {
                    'parse_time': int(seq['parse_time']),
                    'rewrite_time': int(seq['rewrite_time']),
                    'plan_time': int(seq['plan_time']),
                    'execution_time': int(seq['execution_time']),
                    'other_time': kernel_other_time
                }
            }
        })
        # parsing details
        data_io_top5 = FixedSizeList(5)
        lwlock_top5 = FixedSizeList(5)
        lock_top5 = FixedSizeList(5)
        code_top5 = FixedSizeList(5)
        data_io_time_of_details = []
        lwlock_time_of_details = []
        lock_time_of_details = []
        code_time_of_details = []
        details_parser = DetailsParser(details)
        for area_type, area in details_parser.parse_areas():
            if area_type == 65:
                wait_events = sorted(details_parser.parse_wait_events(area), key=lambda x: -x[-1])
                for wait_type, wait_name, wait_time in wait_events:
                    # status type
                    if wait_type == 4:
                        code_top5.append({'event_name': wait_name, 'event_time': wait_time})
                        code_time_of_details.append(wait_time)
                    elif wait_name in IO_EVENTS:
                        data_io_top5.append({'event_name': wait_name, 'event_time': wait_time})
                        data_io_time_of_details.append(wait_time)
                    elif wait_name in LWLOCK_EVENTS:
                        lwlock_top5.append({'event_name': wait_name, 'event_time': wait_time})
                        lwlock_time_of_details.append(wait_time)
                    elif wait_name in LOCK_EVENTS:
                        lock_top5.append({'event_name': wait_name, 'event_time': wait_time})
                        lock_time_of_details.append(wait_time)

        code_wait_info = {
            'code_wait_event_time': {
                'all_time': db_time,
                'code_wait_event_time_details': {
                    'events': code_top5.values(),
                    'left_time': str(sum(code_time_of_details) - sum(code_time_of_details[:5])),
                    'other_time': str(db_time - sum(code_time_of_details))
                }
            }
        }

        data_io_info = {
            'data_io_time': {
                'all_time': data_io_time,
                'data_io_time_details': {
                    'events': data_io_top5.values(),
                    'left_time': sum(data_io_time_of_details) - sum(data_io_time_of_details[:5]),
                    'other_time': data_io_time - sum(code_time_of_details)
                }
            }
        }

        lock_info = {
            'lock_time': {
                'all_time': lock_time,
                'lock_time_details': {
                    'events': lock_top5.values(),
                    'left_time': sum(lock_time_of_details) - sum(lock_time_of_details[:5]),
                    'other_time': lock_time - sum(lock_time_of_details)
                }
            }
        }

        lwlock_info = {
            'lwlock_time': {
                'all_time': lwlock_time,
                'lwlock_time_details': {
                    'events': lwlock_top5.values(),
                    'left_time': sum(lwlock_time_of_details) - sum(lwlock_time_of_details[:5]),
                    'other_time': lwlock_time - sum(lwlock_time_of_details)
                }
            }
        }

        resource_details = dict()
        resource_details.update(data_io_info)
        resource_details.update(lock_info)
        resource_details.update(lwlock_info)
        resource_wait_info = {
            'resource_wait_event_time': {
                'all_time': db_time,
                'resource_wait_event_time_details': resource_details,
                'other_time': db_time - data_io_time - lock_time - lwlock_time
            }
        }
        res = dict()
        for _key in base_info:
            res[_key] = seq.get(_key, "")

        res['all_time'] = db_time
        res['execution_time_details'] = execution_time_details
        res['execution_time_details']['wait_event_time'] = code_wait_info
        res['execution_time_details']['wait_event_time'].update(resource_wait_info)
        res['sql_id'] = int(res.pop('unique_query_id'))
        res['sql_exec_id'] = int(res.pop('debug_query_id'))
        res['session_id'] = int(res.pop('session_id'))
        res['client_port'] = int(res.pop('client_port'))
        results.append(res)

    results.sort(key=lambda x: (x['sql_exec_id'], x['start_time']))
    return results


def get_root_cause_analysis(metric_name, metric_filter, start, end,
                            alarm_cause=None, reason_name=None, rca_params=None):
    least_window = 300  # empirical
    least_n_intervals = 40  # empirical
    client = TsdbClientFactory.get_tsdb_client()
    tsdb_interval = client.scrape_interval
    if not (isinstance(tsdb_interval, int) and tsdb_interval):
        tsdb_interval = least_window // least_n_intervals

    metric_filter, alarm_cause_list, reason_name_list, rca_params_dict = check_params(
        metric_name, metric_filter,
        alarm_cause=alarm_cause,
        reason_name=reason_name,
        rca_params=rca_params
    )
    start = date_type(start) if isinstance(start, str) else start
    end = date_type(end) if isinstance(end, str) else end

    return rca(metric_name, metric_filter, start, end, tsdb_interval,
               alarm_cause_list=alarm_cause_list,
               reason_name_list=reason_name_list,
               rca_params_dict=rca_params_dict)


def get_insight(metric_name, metric_filter, start, end,
                alarm_cause=None, reason_name=None):
    least_window = 300  # empirical
    least_n_intervals = 40  # empirical
    client = TsdbClientFactory.get_tsdb_client()
    tsdb_interval = client.scrape_interval
    if not (isinstance(tsdb_interval, int) and tsdb_interval):
        tsdb_interval = least_window // least_n_intervals

    if reason_name is not None:
        alarm_cause = None

    metric_filter, alarm_cause_list, reason_name_list, _ = check_params(
        metric_name, metric_filter,
        alarm_cause=alarm_cause,
        reason_name=reason_name
    )
    start = date_type(start) if isinstance(start, str) else start
    end = date_type(end) if isinstance(end, str) else end

    return insight_view(metric_name, metric_filter, start, end, tsdb_interval,
                        alarm_cause_list=alarm_cause_list,
                        reason_name_list=reason_name_list)


def get_full_sql_statement(metric_name, from_timestamp=None, to_timestamp=None,
                           step=15000, fetch_all=True, labels=None, regex_labels=None,
                           min_value=None, max_value=None, limit=None, tz=None):
    if min_value is None or max_value is None:
        raise ValueError("The min_value or max_value parameters is invalid.")

    from_timestamp, to_timestamp, labels, regex_labels, tz, step = adjust_full_sql_parameter(
        min_value, from_timestamp, to_timestamp, labels, regex_labels, tz, step
    )
    fetch_step = 15 * 1000 if limit is not None else 60 * 1000

    if from_timestamp + fetch_step < to_timestamp:
        to_timestamp_temp = from_timestamp + fetch_step
    else:
        to_timestamp_temp = to_timestamp

    results = []
    while from_timestamp < to_timestamp:
        from_datetime = datetime.datetime.fromtimestamp(int(from_timestamp / 1000), tz=tz)
        to_datetime = datetime.datetime.fromtimestamp(int(to_timestamp_temp / 1000), tz=tz)
        fetcher = dai.get_metric_sequence(
            metric_name, from_datetime, to_datetime,
            step=step, min_value=min_value, max_value=max_value
        )
        result = filter_full_statement(fetch_all, fetcher, labels, regex_labels)
        if limit:
            results = results + result[0: limit - len(results)]
            if len(results) >= limit:
                logging.info('full sql statement size is %s', len(results))
                return list(map(lambda s: s.jsonify(), results))

        else:
            results = results + result

        if to_timestamp_temp == to_timestamp:
            break

        remain = limit - len(results) if limit is not None else 0
        if fetch_step <= 3 * 60 * 60 * 1000 and len(result) * 2 < remain:
            fetch_step = fetch_step * 2

        from_timestamp = to_timestamp_temp
        if from_timestamp + fetch_step >= to_timestamp:
            to_timestamp_temp = to_timestamp
        else:
            to_timestamp_temp = from_timestamp + fetch_step

    logging.info('full sql statement size is %s', len(results))
    return list(map(lambda s: s.jsonify(), results))


def filter_full_statement(fetch_all, fetcher, labels, regex_labels):
    """
    filter full_statement result by labels or regex_labels
    """
    if labels is not None:
        fetcher.filter(**labels)
    if regex_labels is not None:
        fetcher.filter_like(**regex_labels)
    result = fetcher.fetchall() if fetch_all else [fetcher.fetchone()]
    return result


def adjust_full_sql_parameter(min_value, from_timestamp, to_timestamp, labels, regex_labels, tz, step):
    """
    adjust full_sql_parameter
    """
    tz = adjust_timezone(tz) if tz else tz
    logging.info('query full sql statement by tz: %s', tz)
    if from_timestamp is None:
        from_timestamp = int(min_value) if min_value else int(
            (datetime.datetime.now(tz=tz) - datetime.timedelta(days=14)).timestamp() * 1000)
    to_timestamp = int(to_timestamp) if to_timestamp else int(datetime.datetime.now(tz=tz).timestamp() * 1000)
    logging.info('query full sql statement from %s to %s.', from_timestamp, to_timestamp)
    if labels is not None:
        try:
            labels = json.loads(labels)
        except json.decoder.JSONDecodeError:
            labels = string_to_dict(labels)
        logging.info('query full sql statement by labels: %s', labels)
    if regex_labels is not None:
        try:
            regex_labels = json.loads(regex_labels)
        except json.decoder.JSONDecodeError:
            regex_labels = string_to_dict(regex_labels)
        logging.info('query full sql statement by regex_labels: %s', regex_labels)
    step = 15000 if not step else step
    logging.info('query full sql statement by step: %s', step)

    return from_timestamp, to_timestamp, labels, regex_labels, tz, step


def _load_scenarios():
    scenario_file_path = os.path.join(global_vars.confpath, SCENARIO_YAML_FILE_NAME)
    security_scenarios_list = load_scenarios_yaml_definitions(scenario_file_path)
    return security_scenarios_list


def get_scenarios_list():
    security_scenarios_list = _load_scenarios()
    scenarios = [scenario.name for scenario in security_scenarios_list]
    return scenarios


def get_scenario_metrics(scenario_name: str):
    instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    scenarios = _load_scenarios()
    re_calibrate_period = get_security_metrics_settings('re_calibrate_period')

    for scenario in scenarios:
        if scenario.name == scenario_name:
            result = []
            metrics = scenario.get_metrics()
            for metric in metrics:
                model_age = get_calibration_model_age_in_minutes(metric, instance)
                if model_age == 0:
                    metric_status = "NOT CALIBRATED"
                elif model_age > re_calibrate_period:
                    metric_status = "OLD"
                else:
                    metric_status = "CALIBRATED"
                result.append({"metric": metric, "status": metric_status})
            return result
    return []


def get_metric_unit(metric: str):
    """
    - param metric: The metric name
    """
    return str(METRIC_INFO.get(metric, {}).get("unit", {"en": None, "cn": None}))


@microservice
def get_insight_dis(connection_info, metric_name, metric_filter, alarm_cause, start, end):
    return get_insight(metric_name, metric_filter, start, end, alarm_cause=alarm_cause)


def check_time_task_params(time_task_config):
    if time_task_config is None:
        return True, None
    cluster_state = dai.get_latest_metric_sequence('opengauss_cluster_state', WINDOW_IN_MINUTES).fetchall()
    valid_cn_ip, valid_dn_ip = set(), set()
    for state in cluster_state:
        cn_state_list = json.loads(state.labels.get('cn_state', "[]"))
        dn_state_list = json.loads(state.labels.get('dn_state', "[]"))
        for cn_state in cn_state_list:
            cn_ip = cn_state.get('ip', None)
            if cn_ip is not None:
                valid_cn_ip.add(cn_ip)
        for dn_state in dn_state_list:
            dn_ip = dn_state.get('ip', None)
            if dn_ip is not None:
                valid_dn_ip.add(dn_ip)
    if time_task_config.cn is not None:
        if not isinstance(time_task_config.cn, list):
            return False, 'cn'
        for cn in time_task_config.cn:
            if cn not in valid_cn_ip:
                return False, 'cn'
    if time_task_config.dn is not None:
        if not isinstance(time_task_config.dn, list):
            return False, 'dn'
        for dn in time_task_config.dn:
            if dn not in valid_dn_ip:
                return False, 'dn'
    if time_task_config.detector_name is not None:
        if time_task_config.detector_name == 'all' or not check_name_valid(time_task_config.detector_name):
            return False, 'detector_name'
    if time_task_config.duration is not None:
        if not isinstance(time_task_config.duration, int) or\
                time_task_config.duration < 0 or time_task_config.duration > 604800:
            return False, 'duration'
    if time_task_config.alarm_info is not None:
        if getattr(ALARM_TYPES, time_task_config.alarm_info.alarm_type, None) is None:
            return False, 'alarm_info - alarm_type'
        if getattr(ALARM_LEVEL, time_task_config.alarm_info.alarm_level, None) is None:
            return False, 'alarm_info - alarm_level'
    if time_task_config.detector_info is not None:
        if not isinstance(time_task_config.detector_info, list):
            return False, 'detector_info'
        if len(time_task_config.detector_info) == 0:
            return False, 'detector_info'
        for detector in time_task_config.detector_info:
            if not check_name_valid(detector.metric_name):
                return False, 'detector_info - metric_name'
            if detector.detector_name not in detector_algorithm:
                return False, 'detector_info - detector_name'
            if isinstance(detector.metric_filter, dict):
                instance = detector.metric_filter.get('instance', None)
                from_instance = detector.metric_filter.get('from_instance', None)
                if (instance is not None and not check_instance_valid(instance)) or\
                        (from_instance is not None and not check_instance_valid(from_instance)):
                    return False, 'detector_info - metric_filter'

    return True, None


def dispatch_time_task_dis(task_name, time_task_config):
    if task_name == 'anomaly_detection' and any(arg is None for arg in (time_task_config.detector_name,
                                                                        time_task_config.duration,
                                                                        time_task_config.alarm_info,
                                                                        time_task_config.detector_info)):
        raise ValueError('Missing field in JSON data time_task_config.')
    if task_name == 'cluster_diagnose' and all(arg is None for arg in (time_task_config.cn, time_task_config.dn)):
        raise ValueError('Missing field in JSON data time_task_config.')

    param_validity, invalid_key = check_time_task_params(time_task_config)
    if not param_validity:
        raise ValueError(f'Incorrect value for parameter {invalid_key}.')

    result_retention_seconds = global_vars.dynamic_configs.get_int_or_float(
        'self_monitoring', 'result_retention_seconds', fallback=604800
    )
    anomaly_detection_interval = global_vars.configs.getint(
        'TIMED_TASK', 'anomaly_detection_interval', fallback=constants.TIMED_TASK_DEFAULT_INTERVAL
    )
    # This configuration 'result_retention_seconds' can be modified to be passed in through API in the future, but now
    # it is only to minimize changes as much as possible.

    try:
        if task_name == 'discard_expired_results':
            execute_discard_expired_res_time_task_dis(result_retention_seconds)
        elif task_name == 'anomaly_detection':
            alarm_info = AlarmInfo(
                time_task_config.alarm_info.alarm_content,
                time_task_config.alarm_info.alarm_type,
                time_task_config.alarm_info.alarm_level,
                time_task_config.alarm_info.alarm_cause,
                time_task_config.alarm_info.extra
            )
            detector_info = list()
            for detector in time_task_config.detector_info:
                detector_info.append(DetectorInfo(
                    detector.metric_name,
                    detector.detector_name,
                    detector.metric_filter,
                    detector.detector_kwargs
                ))
            anomaly_detector = generic_anomaly_detector.GenericAnomalyDetector(
                time_task_config.detector_name,
                time_task_config.duration,
                0,  # This parameter, forecasting_second, is not open for users to input.
                alarm_info,
                detector_info
            )
            execute_anomaly_detection_time_task_dis(anomaly_detection_interval, anomaly_detector)
        elif task_name == 'cluster_diagnose':
            cn_dn_ip_set = {
                'cn': set(time_task_config.cn) if time_task_config.cn is not None else set(),
                'dn': set(time_task_config.dn) if time_task_config.dn is not None else set()
            }
            execute_cluster_diagnose_time_task_dis(cn_dn_ip_set)
        elif task_name == 'update_statistics':
            execute_update_statistics_time_task_dis()
        else:
            raise ValueError('Invalid value for parameter task_name.')
    except Exception as e:
        raise e

    return {'msg': f'The task {task_name} has been successfully executed.'}


def execute_discard_expired_res_time_task_dis(result_retention_seconds):
    dai.delete_older_result(int(time.time()), result_retention_seconds)
    logging.info(f'Metadata before {(int(time.time() - result_retention_seconds)) * 1000} is successfully '
                 f'cleaned.')


def execute_anomaly_detection_time_task_dis(anomaly_detection_interval,
                                            detector: generic_anomaly_detector.GenericAnomalyDetector):
    long_term_metrics = dai.get_meta_metric_sequence(None, {}, {})
    detector_list = [(detector, long_term_metrics)]
    # Currently we only support passing one detector each time calling the function, to be optimized in the future,
    # now it is to minimize changes.

    history_alarms = global_vars.worker.parallel_execute(
        detect_anomaly, detector_list
    ) or []
    logging.info(f'Anomaly detection by {detector_list[0][0].name} is successfully executed.')
    if history_alarms:
        dai.save_history_alarms(history_alarms, detection_interval=anomaly_detection_interval)


def execute_cluster_diagnose_time_task_dis(cn_dn_ip_set):
    end_datetime = datetime.datetime.now()
    start_datetime = end_datetime - datetime.timedelta(minutes=WINDOW_IN_MINUTES)

    cluster_diagnose_params_set = set()
    for role in ANSWER_ORDERS:
        for instance in cn_dn_ip_set[role]:
            for method in METHOD:
                cluster_diagnose_params_set.add((instance, role, start_datetime, end_datetime, method,))

    diagnosis_record = global_vars.worker.parallel_execute(
        diagnose_cluster_state, cluster_diagnose_params_set
    ) or []

    logging.info(f'Cluster diagnosis for {cn_dn_ip_set} is successfully executed.')
    if diagnosis_record:
        dai.save_history_cluster_diagnosis(diagnosis_record)


def execute_update_statistics_time_task_dis():
    now = int(time.time())
    for metric_name, metric_stat in monitoring_constants.LONG_TERM_METRIC_STATS.items():
        length = metric_stat["length"]
        step = metric_stat["step"]
        dai.update_metric_stats(now, metric_name, length, step)
    logging.info('Metric statistics are successfully updated.')


def mode_check(f):
    """To check whether v2 api is called when DBMind starts as single node mode."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not global_vars.is_distribute_mode:
            try:
                raise ModeError('Version \'v2\' API is forbidden in single deployment mode.')
            except ModeError as e:
                logging.getLogger('uvicorn.error').exception(e)
            return {'success': False, 'msg': 'Internal server error'}

        return f(*args, **kwargs)

    return wrapper
