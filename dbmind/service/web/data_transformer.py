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
import datetime
import getpass
import logging
import math
import os
import sys
import time
from collections import defaultdict, Counter
from itertools import groupby

import sqlparse

from dbmind import global_vars
from dbmind.app.monitoring import ad_pool_manager, regular_inspection
from dbmind.app.optimization import get_database_schemas, TemplateArgs
from dbmind.app.optimization.index_recommendation import rpc_index_advise, is_rpc_available
from dbmind.app.optimization.index_recommendation_rpc_executor import RpcExecutor
from dbmind.common.algorithm.forecasting import quickly_forecast
from dbmind.common.types import ALARM_TYPES, ALARM_LEVEL
from dbmind.common.types import Sequence
from dbmind.components.extract_log import get_workload_template
from dbmind.components.slow_query_diagnosis import analyze_slow_query_with_rpc, get_query_plan
from dbmind.components.sql_rewriter.sql_rewriter import rewrite_sql_api
from dbmind.metadatabase import dao
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.components.anomaly_analysis import get_sequences, get_correlations
from dbmind.components.memory_check import memory_check
from dbmind.common.utils import string_to_dict, cast_to_int_or_float
from dbmind.common.dispatcher import TimedTaskManager
from dbmind.components.forecast import early_warning
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils
from dbmind.components.fetch_statement.collect_workloads import (
                          collect_statement_from_statement_history, 
                          collect_statement_from_activity, 
                          collect_statement_from_asp
)

from .context_manager import ACCESS_CONTEXT_NAME, get_access_context
from .jsonify_utils import (
    sqlalchemy_query_jsonify, psycopg2_dict_jsonify,
    sqlalchemy_query_records_count_logic, sqlalchemy_query_union_records_logic
)


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


def get_metric_sequence(metric_name, instance, from_timestamp=None, to_timestamp=None, step=None, fetch_all=False,
                        regrex=False, labels=None, regrex_labels=None):
    # notes: 1) this method must ensure that the front-end and back-end time are consistent
    #        2) this method will return the data of all nodes in the cluster, which is not friendly to some scenarios
    if to_timestamp is None:
        to_timestamp = int(time.time() * 1000)
    if from_timestamp is None:
        from_timestamp = to_timestamp - 0.5 * 60 * 60 * 1000  # It defaults to show a half of hour.
    from_datetime = datetime.datetime.fromtimestamp(from_timestamp / 1000)
    to_datetime = datetime.datetime.fromtimestamp(to_timestamp / 1000)
    fetcher = dai.get_metric_sequence(metric_name, from_datetime, to_datetime, step=step)
    if instance is None:
        from_server_predicate = get_access_context(ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX)
        if from_server_predicate:
            fetcher.from_server_like(from_server_predicate)
    else:
        if regrex:
            instance = instance + ':?.*'
            fetcher.from_server_like(instance)
        else:
            fetcher.from_server(instance)

    if labels is not None:
        labels = string_to_dict(labels, delimiter=',')
        fetcher.filter(**labels)
    if regrex_labels is not None:
        regrex_labels = string_to_dict(regrex_labels, delimiter=',')
        fetcher.filter_like(**regrex_labels)

    if fetch_all:
        result = fetcher.fetchall()
    else:
        result = [fetcher.fetchone()]
    return list(map(lambda s: s.jsonify(), result))


def get_latest_metric_sequence(metric_name, instance, latest_minutes, step=None, fetch_all=False, regrex=False,
                               labels=None, regrex_labels=None):
    # this function can actually be replaced by 'get_metric_sequence', but in order to avoid
    # the hidden problems of that method, we add 'instance', 'fetch_all' and 'labels' to solve it
    # notes: the format of labels is 'key1=val1, key2=val2, key3=val3, ...'
    if latest_minutes is None or latest_minutes <= 0:
        fetcher = dai.get_latest_metric_value(metric_name)
    else:
        fetcher = dai.get_latest_metric_sequence(metric_name, latest_minutes, step=step)
    if instance is None:
        from_server_predicate = get_access_context(ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX)
        if from_server_predicate:
            fetcher.from_server_like(from_server_predicate)
    else:
        if regrex:
            instance = instance + ':?.*'
            fetcher.from_server_like(instance)
        else:
            fetcher.from_server(instance)

    if labels is not None:
        labels = string_to_dict(labels, delimiter=',')
        fetcher.filter(**labels)
    if regrex_labels is not None:
        regrex_labels = string_to_dict(regrex_labels, delimiter=',')
        fetcher.filter_like(**regrex_labels)

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
               float(group_result[0].select_ratio), 
               float(group_result[0].update_ratio), 
               float(group_result[0].delete_ratio), 
               float(group_result[0].insert_ratio), 
               sum(float(x.optimized) / len(group_result) for x in group_result)]
        advised_indexes['rows'].append(row)

    return {'advised_indexes': advised_indexes}


def get_index(index_type):
    indexes = dict()
    index_type_map = {1:'advised_indexes', 3:'invalid_indexes', 2:'redundant_indexes'}
    _indexes = dao.index_recommendation.get_advised_index(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT), index_type=index_type)
    indexes['header'] = ['schema', 'database', 'table', index_type_map[index_type]]
    indexes['rows'] = []
    for _index in _indexes:
        indexes['rows'].append([_index.schema_name, _index.db_name, _index.tb_name, _index.columns])

    return {index_type_map[index_type]: indexes}


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

    return positive_sql


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
    redundant_index = get_index(index_type=2)
    invalid_index = get_index(index_type=3)
    positive_sql = {'positive_sql': get_positive_sql(positive_pagesize, positive_current)}
    index_advisor_summary.update(advised_index)
    index_advisor_summary.update(redundant_index)
    index_advisor_summary.update(invalid_index)
    index_advisor_summary.update(positive_sql)
    index_advisor_summary.update(
        {'improvement_rate': 100 * float(
            latest_indexes_stat['positive_sql_count'] / latest_indexes_stat['stmt_count']) if latest_indexes_stat[
            'stmt_count'] else 0})
    index_advisor_summary.update({'existing_indexes': get_existing_indexes(existing_pagesize, existing_current)})
    logging.error('index_summary_advisor is: %s', str(index_advisor_summary))
    return index_advisor_summary


def get_all_metrics():
    return list(global_vars.metric_map.keys())


def update_agent_list(force=False):
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


def get_history_alarms(pagesize=None, current=None, instance=None, alarm_type=None,
                       alarm_level=None, group: bool = False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return sqlalchemy_query_union_records_logic(
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
    return sqlalchemy_query_records_count_logic(
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
    return sqlalchemy_query_union_records_logic(
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
    return sqlalchemy_query_records_count_logic(
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
    return sqlalchemy_query_union_records_logic(
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
    return sqlalchemy_query_records_count_logic(
        count_function=dao.healing_records.count_healing_records,
        instances=instances,
        action=action, success=success, min_occurrence=min_occurrence)


def get_slow_queries(pagesize=None, current=None, instance=None, query=None, start_time=None, end_time=None,
                     group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
    offset = max(0, (current - 1) * pagesize)
    limit = pagesize
    return sqlalchemy_query_union_records_logic(
        query_function=dao.slow_queries.select_slow_queries,
        instances=instances, only_with_port=True,
        target_list=(), query=query, start_time=start_time, end_time=end_time, offset=offset, limit=limit, group=group
    )


def get_slow_queries_count(instance=None, distinct=False, query=None, start_time=None, end_time=None, group=False):
    if instance is not None:
        instances = [instance]
    else:
        instances = None
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
    return sqlalchemy_query_records_count_logic(
        count_function=dao.slow_queries.count_killed_slow_queries,
        instances=instances, only_with_port=True,
        query=query, start_time=start_time, end_time=end_time)


def get_slow_query_summary(pagesize=None, current=None):
    # Maybe multiple nodes, but we don't need to care.
    # Because that is an abnormal scenario.
    sequence = get_metric_value('pg_settings_setting') \
        .filter(name='log_min_duration_statement') \
        .fetchone()
    # fix the error which occurs in the interface of DBMind
    if dai.is_sequence_valid(sequence):
        threshold = sequence.values[-1]
    else:
        threshold = 'Nan'
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
           total_elapse_time / n_calls AS avg_elapse_time,
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
    FROM   dbe_perf.statement
    ORDER  BY n_calls DESC,
              avg_elapse_time DESC
    LIMIT  50;
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
    FROM   pg_stat_activity where query_id != '0'
    order by query_start desc;
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    return psycopg2_dict_jsonify(res)


def get_holding_lock_query(username, password):
    stmt = """
    select datname, pid, sessionid, usename, application_name, backend_start, xact_start, query_start, waiting, state, query from pg_catalog.pg_stat_activity where waiting = 't';

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


def toolkit_index_advise(current, pagesize, instance, database, sqls, max_index_num, max_index_storage):
    with global_vars.agent_proxy.context(instance):
        result = sqlparse.split(sqls[0])
        database_schemas = get_database_schemas()
        schema_names = []
        if instance in database_schemas and database in database_schemas[instance]:
            schema_names = database_schemas[instance][database]

        database_templates = dict()
        get_workload_template(database_templates, result, TemplateArgs(10, 5000))
        executor = RpcExecutor(database, None, None, None, None, schema_names)
        executor2 = RpcExecutor(database, None, None, None, None, schema_names)

        def execute_sqls(sqls):
            with global_vars.agent_proxy.context(instance):
                return executor2.execute_sqls(sqls)

        executor.execute_sqls = execute_sqls
        detail_info = rpc_index_advise(executor, database_templates, max_index_num, max_index_storage)
        if detail_info is None:
            return []
        return index_advise_final_result(detail_info, current, pagesize)


def index_advise_final_result(detail_info, current, pagesize):
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
    logging.error('recommend_indexes: %s', recommend_indexes)
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
    final_result['advise_indexes'] = pagination(recommend_list, current, pagesize)
    final_result['total'] = len(recommend_list)
    return final_result, dict()


def pagination(data, page, size):
    begin = (page - 1) * size
    end = page * size
    page_num = math.ceil(len(data) / size)
    result = data[begin:end]
    if result and page <= page_num:
        return result
    else:
        result = data[begin:end]
        return result


def toolkit_rewrite_sql(instance, database, sqls):
    with global_vars.agent_proxy.context(instance):
        return rewrite_sql_api(database, sqls)


def toolkit_slow_sql_rca(**params):
    query = params.pop('query')
    db_name = params.pop('db_name')
    return analyze_slow_query_with_rpc(query, db_name, **params)


def toolkit_get_query_plan(**params):
    query = params.pop("query")
    db_name = params.pop("db_name")
    schema_name = params.pop("schema_name", "public")
    return get_query_plan(query, db_name, schema_name)


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


def get_regular_inspections(inspection_type):
    return sqlalchemy_query_jsonify(dao.regular_inspections.select_metric_regular_inspections(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
        inspection_type=inspection_type, limit=1),
        field_names=['instance', 'report', 'start', 'end', 'id', 'state', 'cost_time', 'inspection_type'])


def get_regular_inspections_count(inspection_type):
    return dao.regular_inspections.count_metric_regular_inspections(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT),
        inspection_type=inspection_type)


def exec_real_time_inspections(inspection_type, start_time, end_time, select_metrics):
    if inspection_type not in ('daily_check', 'weekly_check', 'monthly_check', 'real_time_check'):
        raise ValueError('Incorrect value for parameter inspection_type: {}.'.format(inspection_type))
    if not (isinstance(start_time, str) and start_time.isnumeric() and len(start_time) == 13):
        raise ValueError('Incorrect value for parameter start_time: {}.'.format(start_time))
    if not (isinstance(end_time, str) and end_time.isnumeric() and len(end_time) == 13):
        raise ValueError('Incorrect value for parameter end_time: {}.'.format(end_time))
    start_time = datetime.datetime.fromtimestamp(int(start_time)/1000)
    end_time = datetime.datetime.fromtimestamp(int(end_time)/1000)
    cur_instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    inspect_state = regular_inspection.real_time_inspection(cur_instance, start_time, end_time, select_metrics)
    return {'success': True if inspect_state == 'success' else False}


def list_real_time_inspections():
    cur_instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    
    return sqlalchemy_query_jsonify(dao.regular_inspections.select_metric_regular_inspections(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT), show_report=False),
        field_names=['instance', 'start', 'end', 'id', 'state', 'cost_time', 'inspection_type'])

def report_real_time_inspections(spec_id):
    if not (isinstance(spec_id, str) and spec_id.isnumeric()):
        raise ValueError('Incorrect value for parameter spec_id: {}.'.format(spec_id))
    cur_instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    
    return sqlalchemy_query_jsonify(dao.regular_inspections.select_metric_regular_inspections(
        instance=get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT), spec_id=int(spec_id)),
        field_names=['instance', 'report', 'start', 'end', 'id', 'state', 'cost_time', 'inspection_type'])


def delete_real_time_inspections(spec_id):
    spec_ids = spec_id.split(",")
    for s_id in spec_ids:
        if not (isinstance(s_id, str) and s_id.isnumeric()):
            raise ValueError('Incorrect value for parameter spec_id: {}.'.format(s_id))
        inspect_state = dao.regular_inspections.delete_metric_regular_inspections(int(s_id))
    return {'success': True if inspect_state == 'success' else False}


def get_correlation_result(metric_name, instance, start_time, end_time, topk=10,
                           metric_filter=None):
    client = TsdbClientFactory.get_tsdb_client()
    all_metrics = client.all_metrics
    # In case the length from start_time to end_time is too short,
    # we fix the least window to 20 intervals or 300 seconds.
    least_window = 300  # empirical
    least_n_intervals = 20  # empirical
    tsdb_interval = client.scrape_interval
    start_time, end_time = int(start_time) // 1000, int(end_time) // 1000
    window = tsdb_interval * least_n_intervals if tsdb_interval is not None else least_window
    end_time = min(int(datetime.datetime.now().timestamp()), end_time + window)
    start_time = end_time - window * 2
    start_datetime = datetime.datetime.fromtimestamp(start_time)
    end_datetime = datetime.datetime.fromtimestamp(end_time)

    sequence_args = [((metric, instance, start_datetime, end_datetime),) for metric in all_metrics]

    metric_filter = string_to_dict(metric_filter) if isinstance(metric_filter, str) else dict()
    fetcher = dai.get_metric_sequence(metric_name, start_datetime, end_datetime).filter(**metric_filter)
    this_sequence = fetcher.from_server(instance).fetchone()
    if len(this_sequence) == 0:
        raise ValueError('The metric was not found.')

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


def check_memory_context(start_time, end_time):
    return memory_check(start_time, end_time)


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


def risk_analysis(metric, instance, warning_hours, upper, lower, labels):
    labels = string_to_dict(labels, delimiter=',')
    upper = cast_to_int_or_float(upper)
    lower = cast_to_int_or_float(lower)
    warnings = early_warning(metric, instance, None, warning_hours, upper, lower, labels)
    return warnings


def get_database_data_directory_status(instance, latest_minutes):
    # instance: address of instance agent, format is 'host:port'
    # get the growth_rate of disk usage where the database data directory is located
    return dai.get_database_data_directory_status(instance, latest_minutes)


def get_front_overview(latest_minutes=5):
    overview_detail = {'status': 'stopping', 'strength_version': 'unknown', 'deployment_mode': 'unknown',
                       'operating_system': 'unknown', 'general_risk': 0, 'major_risk': 0, 'high_risk': 0, 'low_risk': 0}
    # this method can be used to front-end
    # instance = get_access_context(ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT)
    instance = global_vars.agent_proxy.current_agent_addr()
    if instance is None:
        return overview_detail
    instance_with_no_port = instance.split(':')[0]
    instance_regrex = instance_with_no_port + ':?.*'

    # get the status of instance
    overview_detail['status'] = dai.check_instance_status().get('status')

    # get version of opengauss
    version_sequence = dai.get_latest_metric_value('pg_node_info_uptime').from_server(instance).fetchone()
    if dai.is_sequence_valid(version_sequence):
        overview_detail['strength_version'] = version_sequence.labels['version']

    # get version of system
    operating_system_sequence = dai.get_latest_metric_value('node_uname_info').filter_like(
        instance=instance_regrex).fetchone()
    if dai.is_sequence_valid(operating_system_sequence):
        overview_detail['operating_system'] = operating_system_sequence.labels['machine']

    # get summary of alarm between start at and end_at
    end_time = int(time.time() * 1000)
    start_time = end_time - latest_minutes * 60 * 1000
    history_alarms = dao.alarms.select_history_alarm(instance=instance, start_at=start_time, end_at=end_time).all()
    alarm_level = [item.alarm_level for item in history_alarms]
    count = Counter(alarm_level)
    for item, times in count.items():
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
        overview_detail['deployment_mode'] = 'centralized'

    return overview_detail


def get_agent_status():
    agent_status = dai.check_agent_status()
    agent_status['status'] = True if agent_status['status'] == 'up' else False
    return agent_status


def get_current_instance_status():
    detail = {'header': ['instance', 'role', 'state'], 'rows': []}
    instance_status = dai.check_instance_status()
    if instance_status['deployment_mode'] == 'centralized':
        detail['rows'].append([instance_status['primary'], 'primary',
                               True if instance_status['primary'] in instance_status['normal'] else False])
        for instance in instance_status['standby']:
            detail['rows'].append([instance, 'standby', True if instance in instance_status['normal'] else False])
        for instance in instance_status['abnormal']:
            detail['rows'].append([instance, '', 'abnormal'])
    elif instance_status['deployment_mode'] == 'single':
        detail['rows'].append([instance_status['primary'], 'primary', True if instance_status['status'] == 'normal' else False])
    return detail


def get_detector_init_defaults():
    return ad_pool_manager.get_detector_init_defaults()


def add_detector(name, json_dict):
    return ad_pool_manager.add_detector(name, json_dict)


def delete_detector(name):
    return ad_pool_manager.delete_detector(name)


def pause_detector(name):
    return ad_pool_manager.pause_detector(name)


def resume_detector(name):
    return ad_pool_manager.resume_detector(name)


def view_detector(name):
    return ad_pool_manager.view_detector(name)


def rebuild_detector():
    return ad_pool_manager.rebuild_detector()


def clear_detector():
    return ad_pool_manager.clear_detector()


def get_collection_system_status():
    collection_detail = {'header': ['component', 'instance', 'listen_address', 'is_alive'], 'rows': [], 'suggestions': []}
    tsdb_status = dai.check_tsdb_status()
    exporter_status = dai.check_exporter_status()
    collection_detail['suggestions'] = dai.diagnosis_exporter_status(exporter_status)
    for component, details in exporter_status.items():
        for detail in details:
            listen_address = detail['listen_address']
            instance = detail['instance']
            status = True if detail['status'] == 'up' else False
            collection_detail['rows'].append([component, instance, listen_address, status])
    collection_detail['rows'].append([tsdb_status['name'],
                                      tsdb_status['instance'],
                                      tsdb_status['listen_address'],
                                      True if tsdb_status['status'] == 'up' else False])
    return collection_detail


def collect_workloads(username, password, data_source, databases, schemas, start_time, 
                      end_time, db_users, sql_types, template_id, duration=60):
    # transfer timestamps to string format
    if start_time:
        start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time // 1000))
    if end_time:
        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time // 1000))
    else:
        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    if not schemas:
        schemas = None
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


def pg_terminate_pid(username, password, pid):
    stmt = f"select PG_TERMINATE_BACKEND({pid})"
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(username, password, 'query_in_postgres', stmt)
    return psycopg2_dict_jsonify(res)


def get_wait_status(username, password, pid, sessionid):
    stmt = f"""
    SELECT wait_status,
           wait_event,
           locktag,
           lockmode,
           block_sessionid
    FROM PG_THREAD_WAIT_STATUS
    WHERE tid = {pid} and sessionid={sessionid};
    """
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(username, password, 'query_in_postgres', stmt)
    return psycopg2_dict_jsonify(res)


def get_wait_tree(username, password, sessionid):
    stmt = f"""
    SELECT query,
           PG_STAT_ACTIVITY.query_id as query_id,
           wait_status,
           wait_event,
           locktag,
           lockmode,
           block_sessionid
    FROM PG_STAT_ACTIVITY
    LEFT JOIN PG_THREAD_WAIT_STATUS
    ON PG_THREAD_WAIT_STATUS.sessionid = PG_STAT_ACTIVITY.sessionid
    WHERE PG_STAT_ACTIVITY.sessionid={sessionid}
    """
    if not sessionid:
        return []
    res = global_vars.agent_proxy.current_rpc().call_with_another_credential(username, password, 'query_in_postgres', stmt)
    for _tuple in res:
        return [{'name': _tuple['query_id'],
                 'details': _tuple, 
                 'children': get_wait_tree(username, password, _tuple['block_sessionid']),
                 }]
    return [] 
    
