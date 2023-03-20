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
"""Data Access Interface (DAI):

    - Wrap all data fetching operations from different sources;
    - The module is the main entry for all data;
    - The data obtained from here ensures that the format is clean and uniform;
    - The data has been preprocessed here.
"""
import logging
from datetime import timedelta, datetime
import time

from dbmind import global_vars
from dbmind.app.diagnosis.query.slow_sql import SlowQuery
from dbmind.common import utils
from dbmind.common.dispatcher.task_worker import get_mp_sync_manager
from dbmind.common.platform import LINUX
from dbmind.common.sequence_buffer import SequenceBufferPool
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types import Sequence, EMPTY_SEQUENCE
from dbmind.common.utils import dbmind_assert
from dbmind.metadatabase import dao
from dbmind.service.utils import SequenceUtils
from dbmind.constants import (DISTINGUISHING_INSTANCE_LABEL,
                              EXPORTER_INSTANCE_LABEL)
from dbmind.common.algorithm.anomaly_detection.gradient_detector import linear_fitting

if LINUX:
    mp_shared_buffer = get_mp_sync_manager().defaultdict(dict)
else:
    mp_shared_buffer = None
buff = SequenceBufferPool(600, vacuum_timeout=300, buffer=mp_shared_buffer)


def datetime_to_timestamp(t: datetime):
    return int(t.timestamp() * 1000)


class LazyFetcher:
    def __init__(self, metric_name, start_time=None, end_time=None, step=None):
        # The default filter should contain some labels (or tags)
        # from user's config. Otherwise, there will be lots of data stream
        # fetching from the remote time series database, whereas, we don't need them.
        self.metric_name = _map_metric(metric_name)
        self.start_time = start_time
        self.end_time = end_time
        self.step = step or estimate_appropriate_step_ms(start_time, end_time)
        self.labels = dict.copy(global_vars.must_filter_labels or {})
        self.labels_like = dict()
        self.rv = None

    def filter(self, **kwargs):
        dbmind_assert(
            not self.labels_like.keys() & kwargs.keys(),
            comment="labels and labels_like have duplicated key."
        )
        self.labels.update(kwargs)
        return self

    def from_server(self, host):
        label_name = _get_data_source_flag(self.metric_name)
        dbmind_assert(
            label_name not in self.labels_like,
            comment="labels and labels_like have duplicated key."
        )
        self.labels[label_name] = host
        return self

    def filter_like(self, **kwargs):
        dbmind_assert(
            not self.labels.keys() & kwargs.keys(),
            comment="labels and labels_like have duplicated key."
        )
        self.labels_like.update(kwargs)
        return self

    def from_server_like(self, host_like):
        label_name = _get_data_source_flag(self.metric_name)
        dbmind_assert(
            label_name not in self.labels,
            comment="labels and labels_like have duplicated key."
        )
        self.labels_like[label_name] = host_like
        return self

    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        params = dict()
        if self.labels_like:
            params["labels_like"] = self.labels_like.copy()
        # Labels have been passed.
        if start_time == end_time or (end_time - start_time) / 1000 < 1:
            if start_time is not None:
                params["time"] = start_time
            return TsdbClientFactory.get_tsdb_client().get_current_metric_value(
                metric_name=self.metric_name,
                label_config=self.labels,
                params=params
            )

        # Normal scenario should be start_time > 1e12 and end_time > 1e12.
        step = step or self.step
        return TsdbClientFactory.get_tsdb_client().get_metric_range_data(
            metric_name=self.metric_name,
            label_config=self.labels,
            start_time=datetime.fromtimestamp(start_time / 1000),
            end_time=datetime.fromtimestamp(end_time / 1000),
            step=step // 1000 if step else step,
            params=params
        )

    def _read_buffer(self):
        # Getting current (latest) value is only one sample,
        # don't need to buffer.
        if (self.start_time is None or self.end_time is None) or self.start_time == self.end_time:
            return self._fetch_sequence()

        start_time, end_time = datetime_to_timestamp(self.start_time), datetime_to_timestamp(self.end_time)
        step = self.step
        try:
            buffered = buff.get(
                metric_name=self.metric_name,
                start_time=start_time,
                end_time=end_time,
                step=step,
                labels=self.labels,
                labels_like=self.labels_like,
                fetcher_func=self._fetch_sequence
            )
        except Exception as e:
            logging.error('SequenceBufferPool crashed.', exc_info=e)
            buffered = self._fetch_sequence(start_time, end_time, step)

        dbmind_assert(buffered is not None)
        return buffered

    def fetchall(self):
        self.rv = self._read_buffer()
        return self.rv

    def fetchone(self):
        # Prometheus doesn't provide limit clause, so we
        # still implement it as below.
        self.rv = self.rv or self._read_buffer()
        # If iterator has un-popped elements then return it,
        # otherwise return empty of the sequence.
        try:
            return self.rv.pop(0)
        except IndexError:
            return Sequence()


# Polish later: add reverse mapper.
def _map_metric(metric_name, to_internal_name=True):
    """Use metric_map.conf to map given metric_name
    so as to adapt to the different metric names from different collectors.
    """
    if global_vars.metric_map is None:
        logging.warning(
            'Cannot map the given metric since global_vars.metric_map is NoneType.'
        )
        return metric_name
    return global_vars.metric_map.get(metric_name, metric_name).strip()


def _get_data_source_flag(metric_name):
    """Use this function to determine which
    label can indicate the metric's source.

    For example, the metric `node_dmi_info` comes from
    node exporter and this metric uses the label `instance`
    to indicate where the metric comes from and this label name
    is also Prometheus default. But another
    metric `gaussdb_blks_hit_ratio` uses `from_instance` to
    indicate the same meaning.

    Therefore, this function defines some rules to tell
    a caller what label name is suitable to indicate source.
    """
    if metric_name.strip().startswith('node_'):
        return EXPORTER_INSTANCE_LABEL
    return DISTINGUISHING_INSTANCE_LABEL


def estimate_appropriate_step_ms(start_time, end_time):
    """If we use a fixed step to fetch a metric sequence,
    the response time will be very long while we obtain a
    long-term sequence. So, we should estimate an appropriate
    sampling step to fetch data. Here, we employ the
    down-sampling logic of Prometheus. No matter how
    long it takes to fetch, Prometheus always returns the data in
    time. The data length is similar because Prometheus
    uses a mapper mechanism to calculate a step for this
    fetching. The mapping relationship is data in one
    hour using the default scrape interval. For more than one hour
    of data, increase the step according to
    the above proportional relation.
    """
    if None in (start_time, end_time):
        return None
    interval_second = TsdbClientFactory.get_tsdb_client().scrape_interval
    if not interval_second:
        # If returns None, it will only depend on TSDB's behavior.
        return None

    ONE_HOUR = 3600  # unit: second
    total_seconds = (end_time - start_time).total_seconds()
    if total_seconds <= ONE_HOUR:
        return None
    # return unit: microsecond
    return int(total_seconds * interval_second // ONE_HOUR * 1000) or None


def get_metric_sequence(metric_name, start_time, end_time, step=None):
    """Get monitoring sequence from time-series database between
    start_time and end_time"""

    return LazyFetcher(metric_name, start_time, end_time, step)


def get_latest_metric_sequence(metric_name, minutes, step=None):
    """Get the monitoring sequence from time-series database in
     the last #2 minutes."""
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    return get_metric_sequence(metric_name, start_time, end_time, step=step)


def get_latest_metric_value(metric_name):
    return LazyFetcher(metric_name)


def save_history_alarms(history_alarms, detection_interval):
    if not history_alarms:
        return
    func = dao.alarms.get_batch_insert_history_alarms_functions()
    for alarm in history_alarms:
        if not alarm:
            continue
        query = dao.alarms.select_history_alarm(instance=alarm.instance, alarm_type=alarm.alarm_type,
                                                alarm_content=alarm.alarm_content,
                                                alarm_level=alarm.alarm_level, limit=1)
        field_names = ['history_alarm_id', 'end_at']
        result = []
        if list(query):
            result = [getattr(query[0], field) for field in field_names]
        if result:
            pre_alarm_id = result[0]
            pre_alarm_end_at = result[1]
            cur_alarm_start_at = alarm.start_timestamp
            cur_alarm_end_at = alarm.end_timestamp
            delay = (cur_alarm_start_at - pre_alarm_end_at) / 1000
            # timestamp unit is 'ms'
            if 0 < delay <= detection_interval:
                dao.alarms.update_history_alarm(alarm_id=pre_alarm_id, end_at=cur_alarm_end_at)
                continue
        func.add(
            instance=alarm.instance,
            metric_name=alarm.metric_name,
            alarm_type=alarm.alarm_type,
            start_at=alarm.start_timestamp,
            end_at=alarm.end_timestamp,
            alarm_level=alarm.alarm_level,
            alarm_content=alarm.alarm_content,
            extra_info=alarm.extra,
            anomaly_type=alarm.anomaly_type
        )
        func.commit()


def save_future_alarms(future_alarms):
    if not future_alarms:
        return

    func = dao.alarms.get_batch_insert_future_alarms_functions()

    for alarm in future_alarms:
        if not alarm:
            continue
        func.add(
            instance=alarm.instance,
            metric_name=alarm.metric_name,
            alarm_type=alarm.alarm_type,
            alarm_level=str(alarm.alarm_level),
            start_at=alarm.start_timestamp,
            end_at=alarm.end_timestamp,
            alarm_content=alarm.alarm_content,
            extra_info=alarm.extra
        )
    func.commit()


def save_healing_record(record):
    dao.healing_records.insert_healing_record(
        instance=record.instance,
        trigger_events=record.trigger_events,
        trigger_root_causes=record.trigger_root_causes,
        action=record.action,
        called_method=record.called_method,
        success=record.success,
        detail=record.detail,
        occurrence_at=record.occurrence_at
    )


def save_slow_queries(slow_queries):
    for slow_query in slow_queries:
        if slow_query is None:
            continue

        h1, h2 = slow_query.hash_query()
        instance = '%s:%s' % (slow_query.db_host, slow_query.db_port)

        def insert():
            dao.slow_queries.insert_slow_query(
                instance=instance,
                schema_name=slow_query.schema_name,
                db_name=slow_query.db_name,
                query=slow_query.query,
                hashcode1=h1,
                hashcode2=h2,
                hit_rate=slow_query.hit_rate, fetch_rate=slow_query.fetch_rate,
                cpu_time=slow_query.cpu_time, data_io_time=slow_query.data_io_time,
                db_time=slow_query.db_time, parse_time=slow_query.parse_time,
                plan_time=slow_query.plan_time, root_cause=slow_query.root_causes,
                suggestion=slow_query.suggestions, template_id=slow_query.template_id
            )

        if not slow_query.replicated:
            insert()
        query_id_result = dao.slow_queries.select_slow_query_id_by_hashcode(
            hashcode1=h1, hashcode2=h2
        ).all()
        if len(query_id_result) == 0:
            insert()
            query_id_result = dao.slow_queries.select_slow_query_id_by_hashcode(
                hashcode1=h1, hashcode2=h2
            ).all()

        slow_query_id = query_id_result[0][0]
        dao.slow_queries.insert_slow_query_journal(
            slow_query_id=slow_query_id,
            start_at=slow_query.start_at,
            duration_time=slow_query.duration_time,
            instance=instance
        )


def delete_older_result(current_timestamp, retention_time):
    utils.dbmind_assert(isinstance(current_timestamp, int))
    utils.dbmind_assert(isinstance(retention_time, int))

    before_timestamp = (current_timestamp - retention_time) * 1000  # convert to ms
    clean_actions = (
        dao.slow_queries.delete_slow_queries,
        dao.slow_queries.delete_killed_slow_queries,
        dao.alarms.delete_timeout_history_alarms,
        dao.healing_records.delete_timeout_healing_records
    )
    for action in clean_actions:
        try:
            action(before_timestamp)
        except Exception as e:
            logging.exception(e)


def get_all_last_monitoring_alarm_logs(minutes):
    """NotImplementedError"""
    return []


def get_all_slow_queries(minutes):
    slow_queries = []
    sequences = get_latest_metric_sequence('pg_sql_statement_history_exc_time', minutes).fetchall()
    # The following fields should be normalized.
    for sequence in sequences:
        from_instance = SequenceUtils.from_server(sequence)
        db_host, db_port = from_instance.split(':')
        db_name = sequence.labels['datname'].lower()
        schema_name = sequence.labels['schema'].split(',')[-1] \
            if ',' in sequence.labels['schema'] else sequence.labels['schema']
        track_parameter = True if 'parameters: $1' in sequence.labels['query'].lower() else False
        query = sequence.labels['query']
        query_plan = sequence.labels['query_plan'] if sequence.labels['query_plan'] != 'None' else None
        start_timestamp = int(sequence.labels['start_time'])  # unit: microsecond
        duration_time = int(sequence.labels['finish_time']) - int(sequence.labels['start_time'])  # unit: microsecond
        hit_rate = round(float(sequence.labels['hit_rate']), 4)
        fetch_rate = round(float(sequence.labels['fetch_rate']), 4)
        cpu_time = round(float(sequence.labels['cpu_time']), 4)
        plan_time = round(float(sequence.labels['plan_time']), 4)
        parse_time = round(float(sequence.labels['parse_time']), 4)
        db_time = round(float(sequence.labels['db_time']), 4)
        data_io_time = round(float(sequence.labels['data_io_time']), 4)
        template_id = sequence.labels['unique_query_id']
        query_id = sequence.labels['debug_query_id']
        lock_wait_count = int(sequence.labels['lock_wait_count'])
        lwlock_wait_count = int(sequence.labels['lwlock_wait_count'])
        n_returned_rows = int(sequence.labels['n_returned_rows'])
        n_tuples_returned = int(sequence.labels['n_tuples_returned'])
        n_tuples_fetched = int(sequence.labels['n_tuples_fetched'])
        n_tuples_inserted = int(sequence.labels['n_tuples_inserted'])
        n_tuples_updated = int(sequence.labels['n_tuples_updated'])
        n_tuples_deleted = int(sequence.labels['n_tuples_deleted'])
        slow_sql_info = SlowQuery(
            db_host=db_host, db_port=db_port, query_plan=query_plan,
            schema_name=schema_name, db_name=db_name, query=query,
            start_timestamp=start_timestamp, duration_time=duration_time,
            hit_rate=hit_rate, fetch_rate=fetch_rate, track_parameter=track_parameter,
            cpu_time=cpu_time, data_io_time=data_io_time, plan_time=plan_time,
            parse_time=parse_time, db_time=db_time,
            template_id=template_id, lock_wait_count=lock_wait_count,
            lwlock_wait_count=lwlock_wait_count, n_returned_rows=n_returned_rows,
            n_tuples_returned=n_tuples_returned, n_tuples_fetched=n_tuples_fetched,
            n_tuples_inserted=n_tuples_inserted, n_tuples_updated=n_tuples_updated,
            n_tuples_deleted=n_tuples_deleted, query_id=query_id
        )

        slow_queries.append(slow_sql_info)
    return slow_queries


def save_index_recomm(index_infos):
    dao.index_recommendation.clear_data()
    for index_info in index_infos:
        _save_index_recomm(index_info)


def _save_index_recomm(detail_info):
    now = int(time.time() * 1000)
    db_name = detail_info.get('db_name')
    instance = detail_info.get('instance')
    positive_stmt_count = detail_info.get('positive_stmt_count', 0)
    recommend_index = detail_info.get('recommendIndexes', [])
    uselessindexes = detail_info.get('uselessIndexes', [])
    created_indexes = detail_info.get('createdIndexes', [])
    table_set = set()
    templates = []
    for index_id, _recomm_index in enumerate(recommend_index):
        sql_details = _recomm_index['sqlDetails']
        optimized = _recomm_index['workloadOptimized']
        schemaname = _recomm_index['schemaName']
        tb_name = _recomm_index['tbName']
        columns = _recomm_index['columns']
        create_index_sql = _recomm_index['statement']
        stmtcount = _recomm_index['dmlCount']
        selectratio = _recomm_index['selectRatio']
        insertratio = _recomm_index['insertRatio']
        deleteratio = _recomm_index['deleteRatio']
        updateratio = _recomm_index['updateRatio']
        table_set.add(tb_name)
        dao.index_recommendation.insert_recommendation(instance=instance,
                                                       db_name=db_name,
                                                       schema_name=schemaname, tb_name=tb_name, columns=columns,
                                                       optimized=optimized, stmt_count=stmtcount,
                                                       index_type=1,
                                                       select_ratio=selectratio, insert_ratio=insertratio,
                                                       delete_ratio=deleteratio, update_ratio=updateratio,
                                                       index_stmt=create_index_sql)
        for workload_id, sql_detail in enumerate(sql_details):
            template = sql_detail['sqlTemplate']
            if [template, db_name] not in templates:
                templates.append([template, db_name])
                dao.index_recommendation.insert_recommendation_stmt_templates(template, db_name)
            stmt = sql_detail['sql']
            stmt_count = sql_detail['sqlCount']
            optimized = sql_detail.get('optimized')
            correlation = sql_detail['correlationType']
            dao.index_recommendation.insert_recommendation_stmt_details(
                template_id=templates.index([template, db_name]) + dao.index_recommendation.get_template_start_id(),
                db_name=db_name, stmt=stmt,
                optimized=optimized,
                correlation_type=correlation,
                stmt_count=stmt_count)

    for uselessindex in uselessindexes:
        table_set.add(uselessindex['tbName'])
        dao.index_recommendation.insert_recommendation(instance=instance,
                                                       db_name=db_name,
                                                       schema_name=uselessindex['schemaName'],
                                                       tb_name=uselessindex['tbName'],
                                                       index_type=uselessindex['type'], columns=uselessindex['columns'],
                                                       index_stmt=uselessindex['statement'])
    for created_index in created_indexes:
        table_set.add(created_index['tbName'])
        dao.index_recommendation.insert_existing_index(instance=instance,
                                                       db_name=db_name,
                                                       tb_name=created_index['tbName'],
                                                       columns=created_index['columns'],
                                                       index_stmt=created_index['statement'])
    recommend_index_count = len(recommend_index)
    redundant_index_count = sum(uselessindex['type'] == 2 for uselessindex in uselessindexes)
    invalid_index_count = sum(uselessindex['type'] == 3 for uselessindex in uselessindexes)
    stmt_count = detail_info['workloadCount']
    table_count = len(table_set)
    dao.index_recommendation.insert_recommendation_stat(instance=instance,
                                                        db_name=db_name,
                                                        stmt_count=stmt_count,
                                                        positive_stmt_count=positive_stmt_count,
                                                        table_count=table_count, rec_index_count=recommend_index_count,
                                                        redundant_index_count=redundant_index_count,
                                                        invalid_index_count=invalid_index_count,
                                                        stmt_source=detail_info['stmt_source'],
                                                        time=now
                                                        )
    return None


def save_knob_recomm(recommend_knob_dict):
    dao.knob_recommendation.truncate_knob_recommend_tables()

    for instance, result in recommend_knob_dict.items():
        knob_recomms, metric_dict = result

        # 1. save report msg
        dao.knob_recommendation.batch_insert_knob_metric_snapshot(instance, metric_dict)

        # 2. save recommend setting
        for knob in knob_recomms.all_knobs:
            sequences = get_latest_metric_value('pg_settings_setting') \
                .filter(name=knob.name) \
                .fetchall()
            current_setting = -1
            for s in sequences:
                if SequenceUtils.from_server(s).startswith(instance) and len(s) > 0:
                    current_setting = s.values[0]
                    break
            dao.knob_recommendation.insert_knob_recommend(
                instance,
                name=knob.name,
                current=current_setting,
                recommend=knob.recommend,
                min_=knob.min,
                max_=knob.max,
                restart=knob.restart
            )

        # 3. save recommend warnings
        dao.knob_recommendation.batch_insert_knob_recommend_warnings(instance,
                                                                     knob_recomms.reporter.warn,
                                                                     knob_recomms.reporter.bad)


def save_killed_slow_queries(instance, results):
    for row in results:
        logging.debug('[Killed Slow Query] %s.', str(row))
        dao.slow_queries.insert_killed_slow_queries(instance, **row)


def save_regular_inspection_results(results):
    for row in results:
        logging.debug('[REGULAR INSPECTION] %s', str(row))
        dao.regular_inspections.insert_regular_inspection(**row)


def check_tsdb_status():
    detail = {'status': 'down', 'listen_address': 'unknown', 'instance': '', 'name': ''}
    client = TsdbClientFactory.get_tsdb_client()
    if not client.check_connection():
        return detail
    detail['listen_address'] = f"{TsdbClientFactory.host}:{TsdbClientFactory.port}"
    detail['instance'] = f"{TsdbClientFactory.host}"
    detail['name'] = client.name
    detail['status'] = "up"
    return detail


def check_exporter_status():
    # notes: if the scope is not specified, the global_var.agent_proxy.current_cluster_instances() 
    #        may return 'None' in most scenarios, therefore this method is limited to 
    #        calling when implementing the API for front-end one agent or we only have one agent
    detail = {'opengauss_exporter': [], 'reprocessing_exporter': [], 'node_exporter': [], 'cmd_exporter': []}
    client = TsdbClientFactory.get_tsdb_client()
    if not client.check_connection():
        detail['opengauss_exporter'].append({'status': 'down', 'listen_address': 'unknown', 'instance': 'unknown'})
        detail['reprocessing_exporter'].append({'status': 'down', 'listen_address': 'unknown', 'instance': 'unknown'})
        detail['node_exporter'].append({'status': 'down', 'listen_address': 'unknown', 'instance': 'unknown'})
        detail['cmd_exporter'].append({'status': 'down', 'listen_address': 'unknown', 'instance': 'unknown'})
        return detail
    self_exporters = {'opengauss_exporter': 'pg_node_info_uptime', 'reprocessing_exporter': 'os_cpu_usage',
                      'node_exporter': 'node_boot_time_seconds', 'cmd_exporter': 'gaussdb_cluster_state'}
    instance_with_port = global_vars.agent_proxy.current_cluster_instances()
    instance_with_no_port = [item.split(':')[0] for item in instance_with_port]
    for exporter, metric in self_exporters.items():
        if exporter in ('opengauss_exporter', 'cmd_exporter'):
            instances = instance_with_port
        else:
            instances = instance_with_no_port
        for instance in instances:
            if exporter == 'node_exporter':
                instance_regex = instance + ':?.*'
                sequences = get_latest_metric_value(metric).\
                    from_server_like(instance_regex).fetchall()
            elif exporter == 'cmd_exporter':
                instance_regrex = instance.split(':')[0] + ':?.*'
                # since the cluster state may change, it will be matched again
                # on the 'primary' after the matching fails on the 'standby' to ensure not miss exporter
                sequences = get_latest_metric_value(metric).\
                    filter_like(instance=instance_regrex, standby=f".*{instance}.*").fetchall()
                if not is_sequence_valid(sequences):
                    sequences = get_latest_metric_value(metric).\
                        filter_like(instance=instance_regrex).filter(primary=instance).fetchall()
            else:
                sequences = get_latest_metric_value(metric).from_server(instance).fetchall()
            if is_sequence_valid(sequences):
                for sequence in sequences:
                    listen_address = sequence.labels.get('instance')
                    if exporter == 'reprocessing_exporter':
                        if listen_address not in (item['listen_address'] for item in detail[exporter]):
                            detail[exporter].append(
                                {'instance': instance, 'listen_address': listen_address, 'status': 'up'})
                    else:
                        detail[exporter].append(
                            {'instance': instance, 'listen_address': listen_address, 'status': 'up'})
            else:
                if exporter == 'node_exporter':
                    detail['node_exporter'].append({'instance': instance, 'listen_address': '', 'status': 'down'})
    return detail


def diagnosis_exporter_status(exporter_status):
    # accept the return value of the function 'check_exporter_status'
    # and give suggestions for the current exporter deployment
    suggestions = []
    instance_with_port = global_vars.agent_proxy.current_cluster_instances()
    instance_with_no_port = [item.split(':')[0] for item in instance_with_port]
    agent_address = global_vars.agent_proxy.current_agent_addr()
    # 1) check whether opengauss_exporter is deployed or whether the number of deployed instances is optimal
    number_of_opengauss_exporter = len(set((item['listen_address'] for item in
                                            exporter_status['opengauss_exporter'])))
    if number_of_opengauss_exporter == 0:
        suggestions.append(
            "It is found that the instance has not deployed opengauss_exporter or some exceptions occurs.")
    # 2) check whether the opengauss_exporter bound to the agent is running normally
    if number_of_opengauss_exporter and \
            agent_address not in (item['instance'] for item in exporter_status['opengauss_exporter']):
        suggestions.append(
            "The opengauss_exporter bound to the agent is not deployed or is running abnormally.")
    # 3) check whether too many opengauss_exporters are deployed
    if number_of_opengauss_exporter > len(instance_with_port):
        suggestions.append("Too many opengauss_exporter on instance, "
                           "it is recommended to deploy at most one opengauss_exporter on each instance.")
    # 4) check the number of reprocessing_exporter
    number_of_reprocessing_number = len(set((item['listen_address'] for item in
                                             exporter_status['reprocessing_exporter'])))
    if number_of_reprocessing_number > 1:
        suggestions.append("Only need to start one reprocessing exporter component.")
    if number_of_reprocessing_number < 1:
        suggestions.append("Is is found that the instance has not deployed reprocessing_exporter or some exception occurs.")
    # 5) check whether too many node_exporters are deployed
    number_of_alive_node_exporter = len(set([item['instance'] for item in
                                        exporter_status['node_exporter'] if item['status'] == 'up']))
    if number_of_alive_node_exporter > len(instance_with_no_port):
        suggestions.append("Too many node_exporter on instance, "
                           "it is recommended to deploy one node_exporter on each instance.")
    # 6) check if some nodes do not deploy exporter
    if number_of_alive_node_exporter < len(instance_with_no_port):
        suggestions.append("Is it found that some node has not deployed node_exporter, "
                           "it is recommended to deploy one node_exporter on each instance.")
    return suggestions


def is_sequence_valid(s):
    if isinstance(s, list):
        return len([item for item in s if item.values]) > 0
    else:
        return s and s.values and s.labels


def is_driver_result_valid(s):
    if isinstance(s, list) and len(s) > 0:
        return True
    return False


def get_database_data_directory_status(instance, latest_minutes):
    # return the data-directory information of current cluster
    detail = {}
    data_directory_sequence = get_latest_metric_value('pg_node_info_uptime').from_server(instance).fetchone()
    if not is_sequence_valid(data_directory_sequence):
        return EMPTY_SEQUENCE
    # the data-directory is all same in the cluster
    data_directory = data_directory_sequence.labels.get('datapath')
    instances = global_vars.agent_proxy.get_all_agents()[instance]
    for instance in instances:
        instance_with_no_port = instance.split(':')[0]
        instance_regrex = instance_with_no_port + ':?.*'
        filesystem_total_size_sequences = get_latest_metric_value('node_filesystem_size_bytes').\
                          filter_like(instance=instance_regrex).fetchall()
        os_disk_usage_sequences = get_latest_metric_sequence('os_disk_usage', latest_minutes).\
                          from_server(instance_with_no_port).fetchall()
        if not is_sequence_valid(filesystem_total_size_sequences):
            continue
        if not is_sequence_valid(os_disk_usage_sequences):
            continue
        # in order to avoid mismatching data-directory, we sort sequences by 'mounpoint' first
        filesystem_total_size_sequences.sort(key=lambda item: len(item.labels['mountpoint']), reverse=True)
        os_disk_usage_sequences.sort(key=lambda item: len(item.labels['mountpoint']), reverse=True)
        data_directory_related_sequences = [sequence for sequence in filesystem_total_size_sequences if
                                            data_directory.startswith(sequence.labels['mountpoint'])]
        disk_usage_related_sequences = [sequence for sequence in os_disk_usage_sequences if
                                        data_directory.startswith(sequence.labels['mountpoint'])]
        # transfer bytes to GB
        total_space = '' if not is_sequence_valid(data_directory_related_sequences) else round(data_directory_related_sequences[0].values[-1] / 1024 / 1024 / 1024, 2)
        usage_rate = '' if not is_sequence_valid(disk_usage_related_sequences) else disk_usage_related_sequences[0].values
        tile_rate, used_space, free_space = '', '', ''
        if total_space and usage_rate:
            tile_rate, _ = linear_fitting(range(0, len(usage_rate)), usage_rate)
            # replace tile rate with disk absolute size(unit: mbytes)
            tile_rate = round(total_space * tile_rate * 1024, 2)
            used_space = round(total_space * usage_rate[-1], 2)
            free_space = round(total_space - used_space, 2)
        detail[instance] = {'total_space': total_space, 
                            'tilt_rate': tile_rate, 
                            'usage_rate': round(usage_rate[-1], 2) if usage_rate else '', 
                            'used_space': used_space, 'free_space': free_space}
    return detail


def check_instance_status():
    # there are two scenarios, which are 'centralized' and 'single', the judgment method is as follows:
    #   1) centralized: judging by 'gaussdb_cluster_state which is fetched by 'cmd_exporter'
    #   2) single: judging by 'pg_node_info_uptime' which is fetched by 'opengauss_exporter'
    # notes: if the scope is not specified, the global_var.agent_proxy.current_cluster_instances() 
    #        may return 'None' in most scenarios, therefore this method is limited to 
    #        calling when implementing the API for front-end or we only have one agent
    detail = {'status': 'unknown', 'deployment_mode': 'unknown', 'primary': '', 'standby':[], 'abnormal': []}
    cluster = global_vars.agent_proxy.current_cluster_instances()
    if len(cluster) == 1:
        detail['deployment_mode'] = 'single'
        detail['primary'] = cluster[0]
        sequence = get_latest_metric_value('pg_node_info_uptime').from_server(cluster[0]).fetchone()
        if is_sequence_valid(sequence):
            detail['status'] = 'normal'
        else:
            detail['status'] = 'abnormal'
    elif len(cluster) > 1:
        detail['deployment_mode'] = 'centralized'
        # since the state of cluster may change and we do not know the latest situation of instance, 
        # therefore we try all nodes in turn to ensure not miss key information
        for instance in cluster:
            cluster_sequence = get_latest_metric_value('gaussdb_cluster_state').filter_like(standby=f'.*{instance}.*').fetchone()
            if not is_sequence_valid(cluster_sequence):
                cluster_sequence = get_latest_metric_value('gaussdb_cluster_state').filter(primary=instance).fetchone()
            if is_sequence_valid(cluster_sequence):
                detail['status'] = 'normal' if cluster_sequence.values[-1] == 1 else 'abnormal'
                detail['primary'] = cluster_sequence.labels['primary']
                detail['standby'] = cluster_sequence.labels['standby'].strip(',').split(',')
                normal = cluster_sequence.labels['normal'].strip(',').split(',')
                detail['abnormal'] = list(set([detail['primary']] + detail['standby']) - set(normal))
                detail['status'] = 'abnormal' if detail['abnormal'] else 'normal'
                break
    return detail


def check_agent_status():
    # we judge the status of agent by executing statement, if the result is correct then 
    # it prove the status of agent is normal, otherwise it is abnormal
    # notes: if the scope is not specified, the global_var.agent_proxy.current_agent_addr() 
    #        may return 'None' in most scenarios, therefore this method is limited to 
    #        calling when implementing the API for front-end or we only have one agent 
    detail = {'status': 'unknown'}
    detail['agent_address'] = global_vars.agent_proxy.current_agent_addr()
    try:
        res = global_vars.agent_proxy.call('query_in_database', 'select 1', None, return_tuples=True)
        if res and res[0] and res[0][0] == 1:
            detail['status'] = 'up'
    except Exception:
        detail['status'] = 'down'
    return detail

