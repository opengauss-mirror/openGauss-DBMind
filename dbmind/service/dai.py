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
import json
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
from dbmind.common.types import Sequence
from dbmind.common.utils import dbmind_assert
from dbmind.metadatabase import dao
from dbmind.service.utils import SequenceUtils
from dbmind.constants import (DISTINGUISHING_INSTANCE_LABEL,
                              EXPORTER_INSTANCE_LABEL)

if LINUX:
    mp_shared_buffer = get_mp_sync_manager().defaultdict(dict)
else:
    mp_shared_buffer = None
buff = SequenceBufferPool(600, vacuum_timeout=300, buffer=mp_shared_buffer)
MERGE_INTERVAL = 60 * 1000  # Alarm merge interval, unit is ms


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


def save_history_alarms(history_alarms):
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
            if cur_alarm_start_at - pre_alarm_end_at < MERGE_INTERVAL:
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


def save_forecast_sequence(metric_name, instance, sequence):
    if not sequence:
        return

    dao.forecasting_metrics.batch_insert_forecasting_metric(
        metric_name, instance, sequence.values, sequence.timestamps,
        labels=json.dumps(sequence.labels)
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

    # The timestamp of forecast is in the future. So we cannot delete older
    # results according to `before_timestamp`. Here, we use current time to 
    # delete forecast metrics.
    try:
        dao.forecasting_metrics.delete_timeout_forecasting_metrics(current_timestamp)
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


def save_statistical_metric_records(results):
    dao.statistical_metric.truncate()
    for row in results:
        logging.debug('[STATISTICAL METRIC RECORD] %s', str(row))
        dao.statistical_metric.insert_record(**row)


def save_regular_inspection_results(results):
    for row in results:
        logging.debug('[REGULAR INSPECTION] %s', str(row))
        dao.regular_inspections.insert_regular_inspection(**row)
