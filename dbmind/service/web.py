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
"""This module is merely for web service
and does not disturb internal operations in DBMind."""
import datetime
import getpass
import json
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from itertools import groupby

from dbmind import global_vars
from dbmind.common.algorithm.forecasting import quickly_forecast
from dbmind.common.types import ALARM_TYPES
from dbmind.common.types import Sequence
from dbmind.components.slow_query_diagnosis import analyze_slow_query_with_rpc
from dbmind.components.sql_rewriter.sql_rewriter import rewrite_sql_api
from dbmind.metadatabase import dao
from dbmind.service.utils import SequenceUtils
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.components.anomaly_analysis import single_process_correlation_calculation, multi_process_correlation_calculation
from . import dai

_access_context = threading.local()


class ACCESS_CONTEXT_NAME:
    TSDB_FROM_SERVERS_REGEX = 'tsdb_from_servers_regex'
    INSTANCE_IP_WITH_PORT_LIST = 'instance_ip_with_port_list'
    INSTANCE_IP_LIST = 'instance_ip_list'
    AGENT_INSTANCE_IP_WITH_PORT = 'agent_instance_ip_with_port'


def set_access_context(**kwargs):
    """Since the web front-end login user can specify
    a cope, we should also pay attention
    to this context when returning data to the user.
    Through this function, set the effective visible field."""
    for k, v in kwargs.items():
        setattr(_access_context, k, v)


def get_access_context(name):
    return getattr(_access_context, name, None)


def split_ip_and_port(address):
    if not address:
        return None, None
    arr = address.split(':')
    if len(arr) < 2:
        return arr[0], None
    return arr[0], arr[1]


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
        if ':' in addr and addr not in valid_addresses:
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
        while ':' in addr and addr not in valid_addresses:
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


def get_metric_sequence(metric_name, from_timestamp=None, to_timestamp=None, step=None):
    fetcher = get_metric_sequence_internal(metric_name, from_timestamp, to_timestamp, step)
    result = fetcher.fetchall()
    result.sort(key=lambda s: str(s.labels))  # Sorted by labels.
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


def get_forecast_sequence_info(metric_name):
    return _sqlalchemy_query_jsonify_for_multiple_instances(
        query_function=dao.forecasting_metrics.aggregate_forecasting_metric,
        instances=get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST),
        metric_name=metric_name
    )


def get_stored_forecast_sequence(metric_name, instance, start_at=None, limit=None):
    r = dao.forecasting_metrics.select_forecasting_metric(
        metric_name, instance=instance, min_metric_time=start_at)
    metric_forecast_result = dict()
    for row in r:
        metric_time = row.metric_time
        metric_value = row.metric_value
        labels = row.labels
        if labels not in metric_forecast_result:
            metric_forecast_result[labels] = {'timestamps': [], 'values': []}

        # skip duplication
        if len(metric_forecast_result[labels]['timestamps']) > 0 and \
                metric_time <= metric_forecast_result[labels]['timestamps'][-1]:
            continue
        metric_forecast_result[labels]['timestamps'].append(metric_time)
        metric_forecast_result[labels]['values'].append(float(metric_value))
    rv = []
    for labels in metric_forecast_result:
        forecast_entity = metric_forecast_result[labels]
        forecast_entity['labels'] = json.loads(labels)
        forecast_entity['name'] = metric_name
        if limit is not None:
            forecast_entity['timestamps'] = forecast_entity['timestamps'][-limit:]
            forecast_entity['values'] = forecast_entity['values'][-limit:]
        rv.append(forecast_entity)
    # Bring into correspondence with the function get_metric_sequence().
    rv.sort(key=lambda v: str(v['labels']))
    return rv


def get_metric_forecast_sequence(metric_name, from_timestamp=None, to_timestamp=None, step=None):
    fetcher = get_metric_sequence_internal(metric_name, from_timestamp, to_timestamp, step)
    sequences = fetcher.fetchall()
    if len(sequences) == 0:
        return []

    forecast_length_factor = 0.33  # 1 / 3
    if from_timestamp is None or to_timestamp is None:
        forecast_minutes = (sequences[0].timestamps[-1] - sequences[0].timestamps[0]) * \
                           forecast_length_factor / 60 / 1000
    else:
        forecast_minutes = (to_timestamp - from_timestamp) * forecast_length_factor / 60 / 1000

    try:
        metric_value_range = global_vars.metric_value_range_map.get(metric_name)
        lower, upper = map(float, metric_value_range.split(','))
    except Exception:
        lower, upper = 0, float("inf")
    # Sorted by labels to bring into correspondence with get_metric_sequence().
    sequences.sort(key=lambda _s: str(_s.labels))
    future_sequences = global_vars.worker.parallel_execute(
        quickly_forecast, ((sequence, forecast_minutes, lower, upper)
                           for sequence in sequences)
    ) or []

    # pop invalid sequences
    i = 0
    while i < len(future_sequences):
        s = future_sequences[i]
        if not s or not len(s):
            future_sequences.pop(i)
            i -= 1
        i += 1

    return list(map(lambda _s: _s.jsonify(), future_sequences))


def get_xact_status():
    committed = get_metric_value('pg_db_xact_commit').fetchall()
    aborted = get_metric_value('pg_db_xact_rollback').fetchall()

    rv = defaultdict(lambda: defaultdict(dict))
    for seq in committed:
        from_instance = SequenceUtils.from_server(seq)
        datname = seq.labels['datname']
        value = seq.values[0]
        rv[from_instance][datname]['commit'] = value
    for seq in aborted:
        from_instance = SequenceUtils.from_server(seq)
        datname = seq.labels['datname']
        value = seq.values[0]
        rv[from_instance][datname]['abort'] = value

    return rv


def get_cluster_node_status():
    node_list = []
    topo = {'root': [], 'leaf': []}
    sequences = get_metric_value('pg_node_info_uptime').fetchall()
    for seq in sequences:
        node_info = {
            'node_name': seq.labels['node_name'],
            'address': SequenceUtils.from_server(seq),
            'is_slave': seq.labels['is_slave'],
            'installation_path': seq.labels['installpath'],
            'data_path': seq.labels['datapath'],
            'uptime': seq.values[0],
            'version': seq.labels['version'],
            'datname': seq.labels.get('datname', 'unknown')
        }
        node_list.append(node_info)
        if node_info['node_name'].startswith('cn_'):
            topo['root'].append({'address': node_info['address'], 'datname': node_info['datname'], 'type': 'cn'})
        elif node_info['node_name'].startswith('dn_'):
            topo['leaf'].append({'address': node_info['address'], 'datname': node_info['datname'], 'type': 'dn'})
        else:
            if node_info['is_slave'] == 'Y':
                topo['leaf'].append({'address': node_info['address'], 'datname': node_info['datname'], 'type': 'slave'})
            else:
                topo['root'].append(
                    {'address': node_info['address'], 'datname': node_info['datname'], 'type': 'master'})

    return {'topo': topo, 'node_list': node_list}


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


def format_date_key(obj, old_date_key):
    new_date = obj[old_date_key]
    if obj[old_date_key].find('.') >= 0 and obj[old_date_key].find('+00:00') >= 0:
        new_date = datetime.datetime.strptime(obj[old_date_key], "%Y-%m-%d %H:%M:%S.%f+00:00").strftime(
            '%Y-%m-%d %H:%M:%S')
    return new_date


def get_running_status():
    buffer_pool = stat_group_by_instance(stat_buffer_hit())
    index = stat_group_by_instance(stat_idx_hit())
    transaction = stat_group_by_instance(stat_xact_successful())

    instances = set(buffer_pool.keys())
    instances = instances.intersection(index.keys(), transaction.keys())
    rv = defaultdict(dict)
    for instance in instances:
        rv[instance]['buffer_pool'] = buffer_pool[instance]
        rv[instance]['index'] = index[instance]
        rv[instance]['transaction'] = transaction[instance]

        try:
            rv[instance]['cpu'] = 1 - get_metric_value('os_cpu_usage') \
                .fetchone().values[0]
            rv[instance]['mem'] = 1 - get_metric_value('os_mem_usage') \
                .fetchone().values[0]
            rv[instance]['io'] = 1 - get_metric_value('os_disk_usage') \
                .fetchone().values[0]
        except IndexError as e:
            logging.warning('Cannot fetch value from sequence with given fields. '
                            'Maybe relative metrics are not stored in the time-series database or '
                            'there are multiple exporters connecting with a same instance.', exc_info=e)
    return rv


def get_instance_status():
    rv = defaultdict(dict)
    for seq in get_metric_value('os_cpu_usage').fetchall():
        instance = SequenceUtils.from_server(seq)
        rv[instance]['os_cpu_usage'] = seq.values[0]
    for seq in get_metric_value('os_mem_usage').fetchall():
        instance = SequenceUtils.from_server(seq)
        rv[instance]['os_mem_usage'] = seq.values[0]
    for seq in get_metric_value('os_disk_usage').fetchall():
        instance = SequenceUtils.from_server(seq)
        rv[instance]['os_disk_usage'] = seq.values[0]
    for seq in get_metric_value('os_cpu_processor_number').fetchall():
        instance = SequenceUtils.from_server(seq)
        rv[instance]['cpu_cores'] = seq.values[0]
    for seq in get_metric_value('node_uname_info').fetchall():
        instance = SequenceUtils.from_server(seq)
        rv[instance]['instance_name'] = seq.labels['nodename']
        rv[instance]['release'] = seq.labels['release']
    for seq in get_metric_value('node_time_seconds').fetchall():
        instance = SequenceUtils.from_server(seq)
        rv[instance]['node_timestamp'] = seq.values[0]
    for seq in get_metric_value('node_boot_time_seconds').fetchall():
        instance = SequenceUtils.from_server(seq)
        rv[instance]['boot_time'] = rv[instance]['node_timestamp'] - seq.values[0]

    return rv


def get_database_list():
    sequences = get_metric_value('pg_database_size_bytes').fetchall()
    rv = set()
    for seq in sequences:
        if seq.values[0] != 1:
            rv.add(seq.labels['datname'])
    return list(rv)


def get_latest_alert():
    alerts = list(global_vars.self_driving_records)
    alerts.reverse()  # Bring the latest events to the front
    return alerts


def get_cluster_summary():
    node_status = get_cluster_node_status()['node_list']
    nb_node = len(node_status)
    nb_cn = 0
    nb_dn = 0
    nb_not_slave = 0
    nb_slave = 0
    for node in node_status:
        node_name = node['node_name']
        is_slave = False if node['is_slave'] == 'N' else True

        if node_name.startswith('cn'):
            nb_cn += 1
        if node_name.startswith('dn'):
            nb_dn += 1
        if is_slave:
            nb_slave += 1
        else:
            nb_not_slave += 1

    version = node_status[0]['version'] if nb_node > 0 else 'unknown due to configuration error'
    if nb_cn > 0:
        form = 'GaussDB (for openGauss, %d CN and %d DN)' % (nb_cn, nb_dn)
    elif nb_node == 1 and nb_not_slave == 1:
        form = 'openGauss (Single Node)'
    elif nb_slave > 0:
        form = 'openGauss (Single Node, Master-Standby Replication)'
    else:
        form = 'unknown'
    return {
        'cluster_summary': {
            'deployment_form': form,
            'version': version,
            'exporters': nb_node,
            'master': nb_node - nb_slave,
            'slave': nb_slave,
            'cn': nb_cn,
            'dn': nb_dn
        },
        'runtime': {
            'python_version': sys.version,
            'python_file_path': sys.executable,
            'python_path': os.environ.get('PYTHONPATH', '(None)'),
            'deployment_user': getpass.getuser(),
            'path': os.environ.get('PATH', '(None)'),
            'ld_library_path': os.environ.get('LD_LIBRARY_PATH', '(None)')
        }
    }


def toolkit_recommend_knobs_by_metrics(metric_pagesize, metric_current,
                                       warning_pagesize, warning_current,
                                       knob_pagesize, knob_current):
    metric_offset = max(0, (metric_current - 1) * metric_pagesize)
    metric_limit = metric_pagesize
    warning_offset = max(0, (warning_current - 1) * warning_pagesize)
    warning_limit = warning_pagesize
    knob_offset = max(0, (knob_current - 1) * knob_pagesize)
    knob_limit = knob_pagesize
    metric_snapshot = sqlalchemy_query_jsonify(
                dao.knob_recommendation.select_metric_snapshot(
                    instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
                    offset=metric_offset, limit=metric_limit),
                field_names=('instance', 'metric', 'value')
            )
    warnings = sqlalchemy_query_jsonify(
                dao.knob_recommendation.select_warnings(
                   instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
                   offset=warning_offset, limit=warning_limit),
                field_names=('instance', 'level', 'comment')
           )
    
    details = sqlalchemy_query_jsonify(
                dao.knob_recommendation.select_details(
                   instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
                   offset=knob_offset, limit=knob_limit), 
              field_names=('instance', 'name', 'current', 'recommend', 'min', 'max')
          )
    return {"metric_snapshot": metric_snapshot, "warnings": warnings, "details": details}


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


def get_latest_indexes_stat():
    latest_indexes_stat = defaultdict(int)
    latest_recommendation_stat = dao.index_recommendation.get_latest_recommendation_stat(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT))
    for res in latest_recommendation_stat:
        latest_indexes_stat['suggestions'] += res.recommend_index_count
        latest_indexes_stat['redundant_indexes'] += res.redundant_index_count
        latest_indexes_stat['invalid_indexes'] += res.invalid_index_count
        latest_indexes_stat['stmt_count'] += res.stmt_count
        latest_indexes_stat['positive_sql_count'] += res.positive_stmt_count
    latest_indexes_stat['valid_index'] = (
            get_existing_indexes_count() -
            latest_indexes_stat['redundant_indexes'] -
            latest_indexes_stat['invalid_indexes']
    )
    return latest_indexes_stat


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


def get_positive_sql(pagesize, current):
    positive_sql = dict()
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    positive_sql['header'] = ['schema', 'database', 'table', 'template', 'typical_sql_stmt',
                              'number_of_sql_statement']
    positive_sql['rows'] = []
    for positive_sql_result in dao.index_recommendation.get_advised_index_details(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
            offset=offset,
            limit=limit):
        row = [positive_sql_result[2].schema_name, positive_sql_result[2].db_name,
               positive_sql_result[2].tb_name, positive_sql_result[1].template,
               positive_sql_result[0].stmt,
               positive_sql_result[0].stmt_count]
        positive_sql['rows'].append(row)

    return {'positive_sql': positive_sql}


def get_existing_indexes(pagesize, current):
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    filenames = ['db_name', 'tb_name', 'columns', 'index_stmt']
    return sqlalchemy_query_jsonify(dao.index_recommendation.get_existing_indexes(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT), 
        offset=offset, limit=limit),
        filenames)


def get_existing_indexes_count():
    return dao.index_recommendation.count_existing_indexes(
        get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)) 


def get_positive_sql_count():
    return dao.index_recommendation.count_advised_index_detail(
        get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT))


def get_index_advisor_summary(positive_pagesize, positive_current,
                              existing_pagesize, existing_current):
    # Only as a function to initialize the page
    instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    db_count, schema_count, table_count = get_db_schema_table_count()
    index_advisor_summary = {'db': db_count,
                             'schema': schema_count,
                             'table': table_count}
    latest_indexes_stat = get_latest_indexes_stat()
    index_advisor_summary.update(latest_indexes_stat)
    index_advisor_summary.update(get_index_change())
    advised_index = get_advised_index()
    positive_sql = get_positive_sql(positive_pagesize, positive_current)
    index_advisor_summary.update(advised_index)
    index_advisor_summary.update(positive_sql)
    index_advisor_summary.update(
        {'improvement_rate': 100 * float(
            latest_indexes_stat['positive_sql_count'] / latest_indexes_stat['stmt_count']) if latest_indexes_stat[
            'stmt_count'] else 0})
    index_advisor_summary.update({'existing_indexes': get_existing_indexes(existing_pagesize, existing_current)})
    return index_advisor_summary


def get_all_metrics():
    return list(global_vars.metric_map.keys())


def get_all_agents():
    return global_vars.agent_proxy.get_all_agents()


def sqlalchemy_query_jsonify(query, field_names=None):
    rv = {'header': field_names, 'rows': []}
    if not field_names:
        field_names = query.statement.columns.keys()  # in order keys.
    rv['header'] = field_names
    for result in query:
        if hasattr(result, '__iter__'):
            row = list(result)
        else:
            row = [getattr(result, field) for field in field_names]
        rv['rows'].append(row)
    return rv


def _sqlalchemy_query_jsonify_for_multiple_instances(
        query_function, instances, **kwargs
):
    field_names = kwargs.pop('field_names', None)
    if not instances:
        return sqlalchemy_query_jsonify(query_function(**kwargs), field_names=field_names)
    rv = None
    for instance in instances:
        r = sqlalchemy_query_jsonify(query_function(instance=instance, **kwargs), field_names=field_names)
        if not rv:
            rv = r
        else:
            rv['rows'].extend(r['rows'])
    return rv


def psycopg2_dict_jsonify(realdict, field_names=None):
    rv = {'header': field_names, 'rows': []}
    if len(realdict) == 0:
        return rv

    if not field_names:
        rv['header'] = list(realdict[0].keys())
    for obj in realdict:
        old_date_key = 'last_updated'
        if old_date_key in obj.keys():
            obj[old_date_key] = format_date_key(obj, old_date_key)
        row = []
        for field in rv['header']:
            row.append(obj[field])
        rv['rows'].append(row)
    return rv


def _sqlalchemy_query_records_logic(query_function, instances, **kwargs):
    if instances is None or len(instances) != 1:
        r1 = _sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=query_function,
            instances=get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST),
            **kwargs
        )
        r2 = _sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=query_function,
            instances=get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST),
            **kwargs
        )
        r1['rows'].extend(r2['rows'])
        return r1

    instance = instances[0]
    ip, port = split_ip_and_port(instance)
    if port is None:
        if ip not in get_access_context(
                ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST
        ):
            # return nothing
            return sqlalchemy_query_jsonify(query_function(instance='', **kwargs))
    else:
        if instance not in get_access_context(
                ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST
        ):
            # return nothing
            return sqlalchemy_query_jsonify(query_function(instance='', **kwargs))
    r1 = sqlalchemy_query_jsonify(
        query_function(ip, **kwargs)
    )
    if port is None:
        return r1
    r2 = sqlalchemy_query_jsonify(
        query_function(instance, **kwargs)
    )
    r1['rows'].extend(r2['rows'])
    return r1


def _sqlalchemy_query_records_count_logic(count_function, instances, **kwargs):
    result = 0
    only_with_port = kwargs.pop('only_with_port', False)
    if instances is None or len(instances) != 1:
        if only_with_port:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        else:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST) + get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        for instance in instances:
            result += count_function(instance, **kwargs)
        return result

    instance = instances[0]
    result = count_function(instance, **kwargs)
    return result


def _sqlalchemy_query_union_records_logic(query_function, instances, **kwargs):
    only_with_port = kwargs.pop('only_with_port', False)
    offset = kwargs.pop('offset', None)
    limit = kwargs.pop('limit', None)
    r = None
    field_names = None
    if instances is None or len(instances) != 1:
        if only_with_port:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        else:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST) + get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        for instance in instances:
            if r is None:
                r = query_function(instance, **kwargs)
                field_names = r.statement.columns.keys()
            else:
                r = r.union_all(query_function(instance, **kwargs))
    else:
        instance = instances[0]
        r = query_function(instance, **kwargs)
    if r is not None:
        if offset is not None:
            r = r.offset(offset)
        if limit is not None:
            r = r.limit(limit)
        return sqlalchemy_query_jsonify(r, field_names)
    return sqlalchemy_query_jsonify(query_function(instance='', **kwargs))


def get_history_alarms(pagesize=None, current=None, instance=None, alarm_type=None,
                       alarm_level=None, group: bool = False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return _sqlalchemy_query_union_records_logic(
        query_function=dao.alarms.select_history_alarm,
        instances=instances,
        offset=offset, limit=limit, 
        alarm_type=alarm_type, alarm_level=alarm_level, group=group
    )


def get_history_alarms_count(instance=None, alarm_type=None, alarm_level=None, group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    return _sqlalchemy_query_records_count_logic(
        count_function=dao.alarms.count_history_alarms,
        instances=instances,
        alarm_type=alarm_type, alarm_level=alarm_level, group=group)


def get_future_alarms(pagesize=None, current=None, instance=None, metric_name=None, start_at=None, group: bool = False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return _sqlalchemy_query_union_records_logic(
        query_function=dao.alarms.select_future_alarm,
        instances=instances,
        offset=offset, limit=limit, 
        metric_name=metric_name, start_at=start_at, group=group
    )


def get_future_alarms_count(instance=None, metric_name=None, start_at=None, group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    return _sqlalchemy_query_records_count_logic(
        count_function=dao.alarms.count_future_alarms,
        instances=instances,
        metric_name=metric_name, start_at=start_at, group=group)


def get_security_alarms(pagesize=None, current=None, instance=None):
    return get_history_alarms(pagesize=pagesize, current=current, instance=instance, alarm_type=ALARM_TYPES.SECURITY)


def get_healing_info(pagesize=None, current=None, instance=None, action=None, success=None, min_occurrence=None):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return _sqlalchemy_query_union_records_logic(
        query_function=dao.healing_records.select_healing_records,
        instances=instances,
        offset=offset, limit=limit, 
        action=action, success=success, min_occurrence=min_occurrence
    )


def get_healing_info_count(instance=None, action=None, success=None, min_occurrence=None):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    return _sqlalchemy_query_records_count_logic(
        count_function=dao.healing_records.count_healing_records,
        instances=instances,
        action=action, success=success, min_occurrence=min_occurrence)


def get_slow_queries(pagesize=None, current=None, instance=None, query=None, start_time=None, end_time=None, group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return _sqlalchemy_query_union_records_logic(
        query_function=dao.slow_queries.select_slow_queries,
        instances=instances, only_with_port=True, 
        target_list=(), query=query, start_time=start_time, end_time=end_time, offset=offset, limit=limit, group=group
    )


def get_slow_queries_count(instance=None, distinct=False, query=None, start_time=None, end_time=None, group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    return _sqlalchemy_query_records_count_logic(
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
    return _sqlalchemy_query_union_records_logic(
        query_function=dao.slow_queries.select_killed_slow_queries,
        instances=instances, only_with_port=True, 
        query=query, start_time=start_time, end_time=end_time, offset=offset, limit=limit
    )


def get_killed_slow_queries_count(instance=None, query=None, start_time=None, end_time=None):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    return _sqlalchemy_query_records_count_logic(
        count_function=dao.slow_queries.count_killed_slow_queries,
        instances=instances, only_with_port=True, 
        query=query, start_time=start_time, end_time=end_time)


def get_slow_query_summary(pagesize=None, current=None):
    # Maybe multiple nodes, but we don't need to care.
    # Because that is an abnormal scenario.
    threshold = get_metric_value('pg_settings_setting') \
        .filter(name='log_min_duration_statement') \
        .fetchone().values[0]

    return {
        'nb_unique_slow_queries': dao.slow_queries.count_slow_queries(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'slow_query_threshold': threshold,
        'main_slow_queries': dao.slow_queries.count_slow_queries(distinct=True, instance=get_access_context(
            ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'statistics_for_database': dao.slow_queries.group_by_dbname(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'statistics_for_schema': dao.slow_queries.group_by_schema(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'systable': dao.slow_queries.count_systable(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'slow_query_count': dao.slow_queries.slow_query_trend(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'distribution': dao.slow_queries.slow_query_distribution(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'mean_cpu_time': dao.slow_queries.mean_cpu_time(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'mean_io_time': dao.slow_queries.mean_io_time(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'mean_buffer_hit_rate': dao.slow_queries.mean_buffer_hit_rate(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'mean_fetch_rate': dao.slow_queries.mean_fetch_rate(
            instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
        'slow_query_template': sqlalchemy_query_jsonify(
            dao.slow_queries.slow_query_template(
                instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)),
            ['template_id', 'count', 'query']),
        'table_of_slow_query': get_slow_queries(pagesize, current)
    }


def get_top_queries(username, password):
    stmt = """\
    SELECT user_name,
           unique_sql_id,
           query,
           n_calls,
           min_elapse_time,
           max_elapse_time,
           total_elapse_time / ( n_calls + 0.001 ) AS avg_elapse_time,
           n_returned_rows,
           db_time,
           cpu_time,
           execution_time,
           parse_time,
           last_updated,
           sort_spill_count,
           hash_spill_count
    FROM   dbe_perf.statement
    ORDER  BY n_calls,
              avg_elapse_time DESC
    LIMIT  10;
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    sorted_fields = [
        'user_name',
        'unique_sql_id',
        'query',
        'n_calls',
        'min_elapse_time',
        'max_elapse_time',
        'avg_elapse_time',
        'n_returned_rows',
        'db_time',
        'cpu_time',
        'execution_time',
        'parse_time',
        'last_updated',
        'sort_spill_count',
        'hash_spill_count'
    ]
    return psycopg2_dict_jsonify(res, sorted_fields)


def get_active_query(username, password):
    stmt = """\
    SELECT datname,
           usename,
           application_name,
           client_addr,
           query_start,
           waiting,
           state,
           query,
           connection_info
    FROM   pg_stat_activity; 
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    sorted_fields = [
        'datname',
        'usename',
        'application_name',
        'client_addr',
        'query_start',
        'waiting',
        'state',
        'query',
        'connection_info'
    ]
    return psycopg2_dict_jsonify(res, sorted_fields)


def get_holding_lock_query(username, password):
    stmt = """\
    SELECT c.relkind,
       d.datname,
       c.relname,
       l.mode,
       s.query,
       extract(epoch
               FROM pg_catalog.now() - s.xact_start) AS holding_time
    FROM pg_locks AS l
    INNER JOIN pg_database AS d ON l.database = d.oid
    INNER JOIN pg_class AS c ON l.relation = c.oid
    INNER JOIN pg_stat_activity AS s ON l.pid = s.pid
    WHERE s.pid != pg_catalog.pg_backend_pid();
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    return psycopg2_dict_jsonify(res)


def check_credential(username, password, scopes=None):
    rpc = None
    if not scopes:
        rpc = global_vars.agent_proxy.current_rpc()
    else:
        logging.debug("Login user %s using scopes '%s'.", username, scopes)
        for instance_addr in scopes:
            if global_vars.agent_proxy.has(instance_addr):
                rpc = global_vars.agent_proxy.get(instance_addr)
                break
    if not rpc:
        return False
    return rpc.handshake(username, password, receive_exception=True)


def toolkit_index_advise(database, sqls):
    for i, sql in enumerate(sqls):
        advise_stmt = sql.replace("'", "''").strip()
        advise_stmt = "select * from gs_index_advise('%s')" % advise_stmt
        sqls[i] = advise_stmt

    stmt = ' union '.join(sqls) + ';'
    res = global_vars.agent_proxy.current_rpc().call('query_in_database', stmt, database, return_tuples=False)
    return psycopg2_dict_jsonify(res)


def toolkit_rewrite_sql(database, sqls):
    return rewrite_sql_api(database, sqls)


def toolkit_slow_sql_rca(query, dbname=None, schema=None, start_time=None, end_time=None):
    return analyze_slow_query_with_rpc(
        query=query, dbname=dbname, schema=schema,
        start_time=start_time, end_time=end_time
    )


def search_slow_sql_rca_result(sql, start_time=None, end_time=None, limit=None):
    root_causes, suggestions = [], []
    default_interval = 120 * 1000
    if end_time is None:
        end_time = int(datetime.datetime.now().timestamp()) * 1000
    if start_time is None:
        start_time = end_time - default_interval
    field_names = (
        'root_cause', 'suggestion'
    )
    # Maximum number of output lines.
    result = dao.slow_queries.select_slow_queries(field_names, sql, start_time, end_time, limit=limit)
    for slow_query in result:
        root_causes.append(getattr(slow_query, 'root_cause').split('\n'))
        suggestions.append(getattr(slow_query, 'suggestion').split('\n'))
    return root_causes, suggestions


def get_metric_statistic(pagesize=None, current=None, instance=None, metric_name=None):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize 
    return _sqlalchemy_query_union_records_logic(
        query_function=dao.statistical_metric.select_metric_statistic_records,
        instances=instances, 
        offset=offset, limit=limit,
        metric_name=metric_name
    )


def get_metric_statistic_count(instance, metric_name):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    return _sqlalchemy_query_records_count_logic(
        count_function=dao.statistical_metric.count_records,
        instances=instances, metric_name=metric_name)


def get_regular_inspections(inspection_type):
    return sqlalchemy_query_jsonify(dao.regular_inspections.select_metric_regular_inspections(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT), 
        inspection_type=inspection_type, limit=1),
        field_names=['instance', 'report', 'start', 'end'])


def get_regular_inspections_count(inspection_type):
    return dao.regular_inspections.count_metric_regular_inspections(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
        inspection_type=inspection_type)


def get_correlation_result(metric_name, host, start_time, end_time, corr_threshold=0.3, topk=10):
    LEAST_WINDOW = int(7.2e3) * 1000
    client = TsdbClientFactory.get_tsdb_client()
    all_metrics = client.all_metrics
    start_time = int(start_time)
    end_time = int(end_time)
    actual_start_time = min(start_time, end_time - LEAST_WINDOW)
    start_datetime = datetime.datetime.fromtimestamp(actual_start_time / 1000)
    end_datetime = datetime.datetime.fromtimestamp(end_time / 1000)
    sequence_args = [(metric_name, host, start_datetime, end_datetime) for metric_name in all_metrics]
    correlation_result = single_process_correlation_calculation(metric_name, sequence_args, corr_threshold=corr_threshold, topk=topk)
    return correlation_result

