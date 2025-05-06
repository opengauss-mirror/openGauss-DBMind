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
import math
import re
import time
from datetime import timedelta, datetime

from dbmind import global_vars
from dbmind.app.monitoring.monitoring_constants import LONG_TERM_METRIC_STATS
from dbmind.common import utils
from dbmind.common.algorithm.stat_utils import stat_funcs
from dbmind.common.dispatcher.task_worker import get_mp_sync_manager
from dbmind.common.platform import LINUX
from dbmind.common.sequence_buffer import SequenceBufferPool
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types import Sequence, SlowQuery
from dbmind.common.utils import dbmind_assert, cast_to_int_or_float
from dbmind.common.utils.checking import (
    check_name_valid,
    prepare_ip,
    split_ip_port,
    transform_instance,
    uniform_labels,
    IPV6_PATTERN,
    SPLIT_INSTANCES_PATTERN
)
from dbmind.constants import (
    DISTINGUISHING_INSTANCE_LABEL,
    EXPORTER_INSTANCE_LABEL,
    PORT_SUFFIX
)
from dbmind.metadatabase import dao
from dbmind.service.multicluster import replace_sequence_ip
from dbmind.service.utils import SequenceUtils

if LINUX:
    mp_shared_buffer = get_mp_sync_manager().defaultdict(dict)
else:
    mp_shared_buffer = None
buff = SequenceBufferPool(600, vacuum_timeout=300, buffer=mp_shared_buffer)


def datetime_to_timestamp(t: datetime):
    return int(t.timestamp() * 1000)


class LazyFetcher:
    def __init__(self, metric_name, start_time=None, end_time=None, step=None, min_value=None, max_value=None):
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
        self.min_value = min_value
        self.max_value = max_value

    def filter(self, **kwargs):
        dbmind_assert(
            not self.labels_like.keys() & kwargs.keys(),
            comment="labels and labels_like have duplicated key."
        )
        if "instance" in kwargs and IPV6_PATTERN.match(split_ip_port(kwargs["instance"])[0]):
            instance = kwargs.pop("instance")
            instance_like = transform_instance(instance)
            self.labels_like["instance"] = instance_like

        self.labels.update(kwargs)
        return self

    def from_server(self, host):
        label_name = _get_data_source_flag(self.metric_name)
        dbmind_assert(
            label_name not in self.labels_like,
            comment="labels and labels_like have duplicated key."
        )
        if label_name == "instance" and IPV6_PATTERN.match(split_ip_port(host)[0]):
            host_like = transform_instance(host)
            self.labels_like[label_name] = host_like
        else:
            self.labels[label_name] = host

        return self

    def filter_like(self, **kwargs):
        dbmind_assert(
            not self.labels.keys() & kwargs.keys(),
            comment="labels and labels_like have duplicated key."
        )
        if "instance" in kwargs:
            instances = SPLIT_INSTANCES_PATTERN.findall(kwargs["instance"])
            for i, instance in enumerate(instances):
                instances[i] = transform_instance(instance)

            kwargs["instance"] = "|".join(instances)

        if "from_instance" in kwargs:
            from_instances = SPLIT_INSTANCES_PATTERN.findall(kwargs["from_instance"])
            for i, from_instance in enumerate(from_instances):
                from_instances[i] = transform_instance(from_instance, full_match=False)

            kwargs["from_instance"] = "|".join(from_instances)

        self.labels_like.update(kwargs)
        return self

    def from_server_like(self, host_like):
        label_name = _get_data_source_flag(self.metric_name)
        dbmind_assert(
            label_name not in self.labels,
            comment="labels and labels_like have duplicated key."
        )
        if label_name == "instance":
            instances = SPLIT_INSTANCES_PATTERN.findall(host_like)
            for i, instance in enumerate(instances):
                instances[i] = transform_instance(instance)

            host_like = "|".join(instances)

        elif label_name == "from_instance":
            from_instances = SPLIT_INSTANCES_PATTERN.findall(host_like)
            for i, from_instance in enumerate(from_instances):
                from_instances[i] = transform_instance(from_instance, full_match=False)

            host_like = "|".join(from_instances)

        self.labels_like[label_name] = host_like
        return self

    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        params = dict()
        if self.labels_like:
            params["labels_like"] = self.labels_like.copy()
        # Labels have been passed.
        if start_time == end_time or (end_time - start_time) / 1000 < 1:
            if start_time is not None:
                params["time"] = start_time // 1000

            return TsdbClientFactory.get_tsdb_client().get_current_metric_value(
                metric_name=self.metric_name,
                label_config=self.labels,
                min_value=self.min_value,
                max_value=self.max_value,
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
            min_value=self.min_value,
            max_value=self.max_value,
            params=params
        )

    def _read_buffer(self):
        # Getting current (latest) value is only one sample,
        # don't need to buffer.
        if self.start_time is None or self.end_time is None:
            return self._fetch_sequence()

        start_time, end_time = datetime_to_timestamp(self.start_time), datetime_to_timestamp(self.end_time)
        if self.start_time == self.end_time:
            return self._fetch_sequence(start_time, end_time)

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
        logging.warning('Cannot map the given metric since global_vars.metric_map is NoneType.')
        return metric_name
    return global_vars.metric_map.get(metric_name, metric_name).strip()


def _get_data_source_flag(metric_name):
    """Use this function to determine which
    label can indicate the metric's source.

    For example, the metric `node_dmi_info` comes from
    node exporter and this metric uses the label `instance`
    to indicate where the metric comes from and this label name
    is also Prometheus default. But another
    metric `opengauss_blks_hit_ratio` uses `from_instance` to
    indicate the same meaning.

    Therefore, this function defines some rules to tell
    a caller what label name is suitable to indicate source.
    """
    instance_flag_prefixes = (
        'node_',  # from node_exporter
        'opengauss_cluster_',  # from cmd_exporter cmd_module
        'opengauss_process_',  # from cmd_exporter cmd_module
        'opengauss_ping_',  # from cmd_exporter cmd_module
        'opengauss_mount_',  # from cmd_exporter cmd_module
        'opengauss_xlog_',  # from cmd_exporter cmd_module
        'opengauss_nic_',  # from cmd_exporter cmd_module
        'opengauss_log_',  # from cmd_exporter log_module
    )
    if metric_name.strip() == 'opengauss_log_errors_rate':
        return DISTINGUISHING_INSTANCE_LABEL
    if metric_name.strip().startswith(instance_flag_prefixes):
        return EXPORTER_INSTANCE_LABEL
    return DISTINGUISHING_INSTANCE_LABEL


def get_metric_source_flag(metric_name):
    return _get_data_source_flag(metric_name)


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
    max_length = 1000

    if None in (start_time, end_time):
        return None

    interval_second = TsdbClientFactory.get_tsdb_client().scrape_interval
    if not interval_second:
        # If returns None, it will only depend on TSDB's behavior.
        return None

    total_seconds = (end_time - start_time).total_seconds()
    if total_seconds < max_length * interval_second:
        return None

    # return unit: microsecond
    return int(total_seconds // max_length) * 1000 or None


def get_metric_sequence(metric_name, start_time, end_time, step=None, min_value=None, max_value=None):
    """Get monitoring sequence from time-series database between
    start_time and end_time"""

    return LazyFetcher(metric_name, start_time, end_time, step, min_value=min_value, max_value=max_value)


def get_latest_metric_sequence(metric_name, minutes, step=None, min_value=None, max_value=None, tz=None):
    """Get the monitoring sequence from time-series database in
     the last #2 minutes."""
    end_time = datetime.now(tz=tz)
    start_time = end_time - timedelta(minutes=minutes)
    return get_metric_sequence(metric_name, start_time, end_time, step=step, min_value=min_value, max_value=max_value)


def get_latest_metric_value(metric_name):
    return LazyFetcher(metric_name)


def delete_metric_sequence(metric_name, from_datetime, to_datetime, labels, regex_labels, flush):
    """Delete data in TSDB, DBMind timestamp unified 13 digits"""
    client = TsdbClientFactory.get_tsdb_client()
    client.delete_metric_data(metric_name, from_datetime, to_datetime, labels, regex_labels, flush)


def get_all_metrics():
    client = TsdbClientFactory.get_tsdb_client()
    return client.all_metrics


def save_anomaly_detectors(record):
    if not record:
        return

    dao.anomaly_detectors.insert_anomaly_detectors(
        cluster_name=record.get('cluster_name'),
        detector_name=record.get('detector_name'),
        alarm_cause=record.get('alarm_cause'),
        alarm_content=record.get('alarm_content'),
        alarm_level=record.get('alarm_level'),
        alarm_type=record.get('alarm_type'),
        extra=record.get('extra'),
        detector_info=record.get('detector_info'),
        duration=record.get('duration'),
        forecasting_seconds=record.get('forecasting_seconds'),
        running=record.get('running')
    )


def save_history_alarms(history_alarms, detection_interval=-1):
    if not history_alarms:
        return

    func = dao.alarms.get_batch_insert_history_alarms_functions()
    for alarm_list in history_alarms:
        if alarm_list is None:
            continue

        for alarm in alarm_list:
            if not alarm:
                continue
            alarm_metric_filter = ",".join([f"{k}={alarm.metric_filter.get(k, '')}"
                                            for k in sorted(alarm.metric_filter.keys())])
            query = dao.alarms.select_history_alarm(
                instance=alarm.instance,
                metric_name=alarm.metric_name,
                metric_filter=alarm_metric_filter,
                alarm_type=alarm.alarm_type,
                alarm_level=alarm.alarm_level,
                alarm_content=alarm.alarm_content,
                anomaly_type=alarm.anomaly_type,
                limit=1
            )  # limit 1 means the latest

            field_names = ['history_alarm_id', 'start_at', 'end_at']
            result = []
            if list(query):
                result = [getattr(query[0], field) for field in field_names]

            if result:
                previous_alarm_id = result[0]
                previous_alarm_start_at = result[1]
                previous_alarm_end_at = result[2]
                current_alarm_start_at = alarm.start_timestamp
                current_alarm_end_at = alarm.end_timestamp
                delay = (current_alarm_start_at - previous_alarm_end_at) / 1000  # timestamp unit is 'ms'
                if (
                    previous_alarm_start_at <= current_alarm_end_at and
                    delay <= detection_interval + 1  # tolerance in 1 second:
                ):
                    dao.alarms.update_history_alarm(previous_alarm_id, end_at=current_alarm_end_at)
                    continue

            func.add(alarm)

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
        instance = f"{prepare_ip(slow_query.db_host)}:{slow_query.db_port}"

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
            start_at=slow_query.start_time,
            duration_time=slow_query.duration_time,
            instance=instance
        )


def save_history_cluster_diagnosis(diagnosis_record):
    if not diagnosis_record:
        return

    func = dao.cluster_diagnosis_records.get_batch_insert_history_cluster_diagnosis_functions()
    for record in diagnosis_record:
        if not record:
            continue

        func.add(record)

    func.commit()


def delete_older_result(current_timestamp, retention_time):
    utils.dbmind_assert(isinstance(current_timestamp, int))
    utils.dbmind_assert(isinstance(retention_time, int))

    before_timestamp = (current_timestamp - retention_time) * 1000  # convert to ms
    clean_actions = (
        dao.slow_queries.delete_slow_queries,
        dao.slow_queries.delete_killed_slow_queries,
        dao.alarms.delete_timeout_history_alarms,
        dao.healing_records.delete_timeout_healing_records,
        dao.cluster_diagnosis_records.delete_timeout_history_cluster_diagnosis,
        dao.high_availability_status.delete_timeout_high_availability_status,
    )
    for action in clean_actions:
        try:
            action(before_timestamp)
        except Exception as e:
            logging.exception(e)
    # The data in inspection function needs to be saved for a long time. in order to support daily inspection,
    # it is currently stored for 31 days and does not support user modification
    try:
        dao.regular_inspections.delete_old_inspection()
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
        db_host, db_port = split_ip_port(from_instance)
        db_name = sequence.labels['datname'].lower()
        schema_name = sequence.labels['schema'].split(',')[-1] \
            if ',' in sequence.labels['schema'] else sequence.labels['schema']
        if not check_name_valid(db_name) or not check_name_valid(schema_name):
            continue
        track_parameter = True if 'parameters: $1' in sequence.labels['query'].lower() else False
        query = sequence.labels['query']
        query_plan = sequence.labels['query_plan'] if sequence.labels['query_plan'] != 'None' else None
        # unit: microsecond
        start_time = cast_to_int_or_float(sequence.labels['start_time'])
        finish_time = cast_to_int_or_float(sequence.labels['finish_time'])
        n_blocks_hit = cast_to_int_or_float(sequence.labels['n_blocks_hit'], precision=2)
        n_blocks_fetched = cast_to_int_or_float(sequence.labels['n_blocks_fetched'], precision=2)
        cpu_time = cast_to_int_or_float(sequence.labels['cpu_time'], precision=4)
        plan_time = cast_to_int_or_float(sequence.labels['plan_time'], precision=4)
        parse_time = cast_to_int_or_float(sequence.labels['parse_time'], precision=4)
        db_time = cast_to_int_or_float(sequence.labels['db_time'], precision=4)
        data_io_time = cast_to_int_or_float(sequence.labels['data_io_time'], precision=4)
        template_id = sequence.labels['unique_query_id']
        debug_query_id = sequence.labels['debug_query_id']
        n_returned_rows = cast_to_int_or_float(sequence.labels['n_returned_rows'])
        n_tuples_returned = cast_to_int_or_float(sequence.labels['n_tuples_returned'])
        n_tuples_fetched = cast_to_int_or_float(sequence.labels['n_tuples_fetched'])
        n_tuples_inserted = cast_to_int_or_float(sequence.labels['n_tuples_inserted'])
        n_tuples_updated = cast_to_int_or_float(sequence.labels['n_tuples_updated'])
        n_tuples_deleted = cast_to_int_or_float(sequence.labels['n_tuples_deleted'])
        n_soft_parse = cast_to_int_or_float(sequence.labels['n_soft_parse'])
        n_hard_parse = cast_to_int_or_float(sequence.labels['n_hard_parse'])
        hash_spill_count = cast_to_int_or_float(sequence.labels['hash_spill_count'])
        sort_spill_count = cast_to_int_or_float(sequence.labels['sort_spill_count'])
        n_calls = cast_to_int_or_float(sequence.labels['n_calls'])
        lock_wait_time = cast_to_int_or_float(sequence.labels['lock_wait_time'])
        lwlock_wait_time: int = cast_to_int_or_float(sequence.labels['lwlock_wait_time'])
        slow_query_info = SlowQuery(
            db_host=db_host, db_port=db_port, query_plan=query_plan, n_calls=n_calls,
            schema_name=schema_name, db_name=db_name, query=query, n_blocks_fetched=n_blocks_fetched,
            start_time=start_time, finish_time=finish_time, n_blocks_hit=n_blocks_hit, track_parameter=track_parameter,
            cpu_time=cpu_time, data_io_time=data_io_time, plan_time=plan_time,
            parse_time=parse_time, db_time=db_time, n_hard_parse=n_hard_parse,
            n_soft_parse=n_soft_parse, template_id=template_id, n_returned_rows=n_returned_rows,
            n_tuples_returned=n_tuples_returned, n_tuples_fetched=n_tuples_fetched,
            n_tuples_inserted=n_tuples_inserted, n_tuples_updated=n_tuples_updated,
            n_tuples_deleted=n_tuples_deleted, debug_query_id=debug_query_id, hash_spill_count=hash_spill_count,
            sort_spill_count=sort_spill_count, lock_wait_time=lock_wait_time, lwlock_wait_time=lwlock_wait_time
        )

        slow_queries.append(slow_query_info)
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
    detail['listen_address'] = f"{prepare_ip(TsdbClientFactory.host)}:{TsdbClientFactory.port}"
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

    self_exporters = {
        'opengauss_exporter': 'pg_node_info_uptime',
        'reprocessing_exporter': 'os_cpu_user_usage',
        'node_exporter': 'node_boot_time_seconds',
        'cmd_exporter': 'opengauss_cluster_state'
    }
    instance_with_port = global_vars.agent_proxy.current_cluster_instances()
    instance_without_port = [split_ip_port(item)[0] for item in instance_with_port]
    for exporter, metric in self_exporters.items():
        if exporter in ('opengauss_exporter', 'cmd_exporter'):
            instances = instance_with_port
        else:
            instances = instance_without_port

        for instance in instances:
            if exporter == 'node_exporter':
                instance_regex = prepare_ip(instance) + PORT_SUFFIX
                sequences = get_latest_metric_value(metric).from_server_like(instance_regex).fetchall()
            elif exporter == 'cmd_exporter':
                instance_regex = prepare_ip(split_ip_port(instance)[0]) + PORT_SUFFIX
                # since the cluster state may change, it will be matched again
                # on the 'primary' after the matching fails on the 'standby' to ensure not miss exporter
                sequences = get_latest_metric_value(
                    metric
                ).filter_like(
                    instance=instance_regex,
                    standby=f"(|.*[0-9],|.*[0-9]],){instance}(,\\[[0-9].*|,[0-9].*|)"
                ).fetchall()

                if not is_sequence_valid(sequences):
                    sequences = get_latest_metric_value(
                        metric
                    ).filter_like(
                        instance=instance_regex,
                        primary=prepare_ip(instance) + PORT_SUFFIX
                    ).fetchall()

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
    instance_without_port = [split_ip_port(item)[0] for item in instance_with_port]
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
        suggestions.append(
            "It is found that the instance has not deployed reprocessing_exporter or some exception occurs.")
    # 5) check whether too many node_exporters are deployed
    number_of_alive_node_exporter = len(set([item['instance'] for item in
                                             exporter_status['node_exporter'] if item['status'] == 'up']))
    if number_of_alive_node_exporter > len(instance_without_port):
        suggestions.append("Too many node_exporter on instance, "
                           "it is recommended to deploy one node_exporter on each instance.")
    # 6) check if some nodes do not deploy exporter
    if number_of_alive_node_exporter < len(instance_without_port):
        suggestions.append("It is found that some node has not deployed node_exporter, "
                           "it is recommended to deploy one node_exporter on each instance.")
    # 7) check whether the cmd_exporter is deployed or not
    cluster = global_vars.agent_proxy.current_cluster_instances()
    number_of_cmd_exproter = len(set((item['listen_address'] for item in
                                      exporter_status['cmd_exporter'])))
    if len(cluster) > 1 and number_of_cmd_exproter == 0:
        suggestions.append("It is found that cmd_exporter is not deployed on each instance.")
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


def get_data_directory_mountpoint_info(instance):
    # return the mountpoint and total size of the disk where the instance data directory is located
    data_directory_sequence = get_latest_metric_value('pg_node_info_uptime').from_server(instance).fetchone()
    if not is_sequence_valid(data_directory_sequence):
        return

    data_directory = data_directory_sequence.labels['datapath']
    instance_without_port = split_ip_port(instance)[0]
    instance_regex = prepare_ip(instance_without_port) + PORT_SUFFIX
    filesystem_total_size_sequences = get_latest_metric_value(
        'node_filesystem_size_bytes'
    ).filter_like(instance=instance_regex).fetchall()

    if not is_sequence_valid(filesystem_total_size_sequences):
        return
    filesystem_total_size_sequences.sort(key=lambda item: len(item.labels['mountpoint']), reverse=True)
    for sequence in filesystem_total_size_sequences:
        if data_directory.startswith(sequence.labels['mountpoint']):
            return sequence.labels['mountpoint'], \
                   sequence.labels['device'], round(sequence.values[-1] / 1024 / 1024 / 1024, 2)


def get_database_data_directory_status(instance, latest_minutes):
    # return the data-directory information of current cluster
    # note: now the node of instance should be deployed opengauss_exporter
    mountpoint, total_space = '', 0.0
    detail = {'total_space': '', 'usage_rate': '', 'used_space': '', 'free_space': ''}
    instance_without_port = split_ip_port(instance)[0]
    # get data_directory of any node in cluster, because all the node have the same data-directory.
    mountpoint_info = get_data_directory_mountpoint_info(instance)
    if mountpoint_info is None:
        return detail
    mountpoint, _, total_space = mountpoint_info
    detail['total_space'] = total_space
    disk_usage_sequence = get_latest_metric_sequence('os_disk_usage', latest_minutes) \
        .from_server(instance_without_port) \
        .filter(mountpoint=mountpoint) \
        .fetchone()
    if not is_sequence_valid(disk_usage_sequence):
        return detail
    detail['usage_rate'] = round(disk_usage_sequence.values[-1], 4)
    detail['used_space'] = round(detail['total_space'] * detail['usage_rate'], 2)
    detail['free_space'] = round(detail['total_space'] - detail['used_space'], 2)
    return detail


def check_instance_status():
    # there are two scenarios, which are 'distributed', 'centralized' and 'single', the judgment method is as follows:
    #   1) centralized, distributed: judging by 'opengauss_cluster_state which is fetched by 'cmd_exporter'
    #   2) single: judging by 'pg_node_info_uptime' which is fetched by 'opengauss_exporter'
    # notes: if the scope is not specified, the global_var.agent_proxy.current_cluster_instances()
    #        may return 'None' in most scenarios, therefore this method is limited to
    #        calling when implementing the API for front-end or we only have one agent
    detail = {'status': 'unknown', 'deployment_mode': 'unknown', 'primary': '',
              'standby': [], 'abnormal': [], 'normal': []}
    cluster = global_vars.agent_proxy.current_cluster_instances()
    if len(cluster) == 1:
        detail['deployment_mode'] = 'single'
        detail['primary'] = cluster[0]
        sequence = get_latest_metric_value('pg_node_info_uptime').from_server(cluster[0]).fetchone()
        detail['status'] = 'normal' if is_sequence_valid(sequence) else 'abnormal'
    elif len(cluster) > 1:
        get_distributed_instance_state(cluster, detail)
        if not detail.get('cn'):
            get_centralized_instance_state(cluster, detail)
    return detail


def get_centralized_instance_state(cluster, detail):
    """
    get instance status in centralized deploy mode

    Args:
        cluster: current_cluster_instances
        detail: instances detail

    Returns:
        instance status in centralized deploy mode

    """
    for instance in cluster:
        ip, port = split_ip_port(instance)
        cluster_sequence = None
        for cluster_sequence in get_latest_metric_sequence('opengauss_cluster_state', 3). \
                filter_like(instance=prepare_ip(ip)+PORT_SUFFIX).fetchall():
            replace_sequence_ip(cluster_sequence)
            if instance in cluster_sequence.labels.get('standby', '') or \
                    instance in cluster_sequence.labels.get('primary', ''):
                break
        if is_sequence_valid(cluster_sequence):
            detail['deployment_mode'] = 'centralized'
            labels = cluster_sequence.labels
            primary = labels.get('primary', '')
            standby = labels.get('standby', '')
            normal = labels.get('normal', '')
            abnormal = labels.get('abnormal', '')
            detail['status'] = 'normal' if cluster_sequence.values[-1] == 1 else 'abnormal'
            detail['primary'] = primary.split(",")[0] if primary else ""
            detail['standby'] = standby.split(",") if standby else []
            detail['normal'] = normal.split(",") if normal else []
            detail['abnormal'] = abnormal.split(",") if abnormal else []
            break


def get_distributed_instance_state(cluster, detail):
    """
    get instance status in distributed deploy mode

    Args:
        cluster: current_cluster_instances
        detail: instances detail

    Returns:
        instance status in distributed deploy mode
    """
    for instance in cluster:
        host, port = split_ip_port(instance)
        cluster_sequence = get_latest_metric_value('opengauss_cluster_state') \
            .filter_like(cn_state=f'.*{host}.*port.*{port}.*').fetchone()
        replace_sequence_ip(cluster_sequence)
        if is_sequence_valid(cluster_sequence):
            detail['deployment_mode'] = 'distributed'
            labels = cluster_sequence.labels
            cn_state = labels.get('cn_state', '')
            primary = labels.get('primary', '')
            standby = labels.get('standby', '')
            normal = labels.get('normal', '')
            abnormal = labels.get('abnormal', '')
            detail['cn'] = [f"{prepare_ip(cn.get('ip'))}:{cn.get('port')}" for cn in json.loads(cn_state)]
            detail['status'] = 'normal' if cluster_sequence.values[-1] == 1 else 'abnormal'
            detail['primary'] = primary.split(",") if primary else ""
            detail['standby'] = standby.split(",") if standby else []
            detail['normal'] = normal.split(",") if normal else []
            detail['abnormal'] = abnormal.split(",") if abnormal else []
            break


def check_agent_status():
    # we judge the status of agent by executing statement, if the result is correct then
    # it prove the status of agent is normal, otherwise it is abnormal
    # notes: if the scope is not specified, the global_var.agent_proxy.current_agent_addr()
    #        may return 'None' in most scenarios, therefore this method is limited to
    #        calling when implementing the API for front-end or we only have one agent
    detail = {'status': 'unknown', 'agent_address': global_vars.agent_proxy.current_agent_addr()}
    try:
        res = global_vars.agent_proxy.call('query_in_database', 'select 1', None, return_tuples=True)
        if res and res[0] and res[0][0] == 1:
            detail['status'] = 'up'
    except Exception:
        detail['status'] = 'down'

    return detail


def calculate_default_step(minutes_back):
    if minutes_back <= 0:
        raise ValueError("minutes_back for step must be positive number")
    MAX_DATA_POINTS = 10000
    MIN_STEP = 15000  # 15 seconds is the lower resolution we use
    step = MIN_STEP
    data_points = minutes_back * 60 * 1000 / MIN_STEP
    if data_points > MAX_DATA_POINTS:
        step = minutes_back * 60 * 1000 // MAX_DATA_POINTS
    return step


def update_metric_stats(now, metric_name, length, step):
    """The timed app function for metric statistics"""

    def single_update():
        if metric_filter in update_tuples:
            tup = update_tuples[metric_filter]
            end = tup["start"] + len(tup["value_list"]) * step
            if not tup["value_list"]:
                update_tuples[metric_filter]["value_list"].append(value)
                update_tuples[metric_filter]["start"] = ts
            elif ts + step > end:
                update_tuples[metric_filter]["value_list"].append(value)
            elif end - step < ts + step <= end:
                update_tuples[metric_filter]["value_list"][-1] = value

        elif metric_filter in insert_tuples:
            insert_tuples[metric_filter]["value_list"].append(value)
        else:
            insert_tuples[metric_filter] = {
                "metric_name": metric_name,
                "method": method,
                "length": length,
                "step": step,
                "start": ts,
                "metric_filter": metric_filter,
                "value_list": [value]
            }

    metric, method = metric_name.rsplit("_", 1)
    func = stat_funcs[method]

    query = dao.metric_statistics.select_metric_statistics(
        metric_name=metric_name,
        method=method,
        length=length,
        step=step,
    )

    if not list(query):  # first insertion
        start_time = datetime.fromtimestamp(now - step)
        end_time = datetime.fromtimestamp(now)
        sequences = get_metric_sequence(metric, start_time, end_time).fetchall()
        for sequence in sequences:
            value = round(func(sequence.values), 5) if sequence.values else None
            labels = sequence.labels
            metric_filter = {k: labels[k] for k in sorted(labels.keys())}
            dao.metric_statistics.insert_metric_statistics(
                metric_name=metric_name,
                method=method,
                length=length,
                step=step,
                start=now - step,
                metric_filter=json.dumps(metric_filter),
                value_list=json.dumps([value])
            )

        return

    earliest_start = min(tup.start for tup in list(query))
    grid_now = earliest_start + (now - earliest_start) // step * step
    grid_min_ts = max(earliest_start, grid_now - length * step)

    earliest_end = grid_now
    update_tuples = dict()
    for tup in list(query):
        stat_id = tup.stat_id
        start = tup.start
        metric_filter = tup.metric_filter
        value_list = json.loads(tup.value_list)
        len_values = len(value_list)

        grid_start = earliest_start + round((start - earliest_start) / step) * step
        grid_end = grid_start + len_values * step
        length_remained = max(0, grid_end - grid_min_ts) // step
        divided_values = value_list[::-1][:length_remained][::-1]
        new_grid_start = max(grid_min_ts, grid_end - len(divided_values) * step)
        earliest_end = min(earliest_end, new_grid_start + len(divided_values) * step)

        update_tuples[metric_filter] = {
            "stat_id": stat_id,
            "metric_name": metric_name,
            "method": method,
            "length": length,
            "step": step,
            "start": new_grid_start,
            "metric_filter": metric_filter,
            "value_list": divided_values
        }

    insert_tuples = dict()
    for ts in range(earliest_end, grid_now, step):
        start_time = datetime.fromtimestamp(ts)
        end_time = datetime.fromtimestamp(ts + step)

        scraped_metric_filter = set()
        sequences = get_metric_sequence(metric, start_time, end_time).fetchall()
        for sequence in sequences:
            value = round(func(sequence.values), 5) if sequence.values else None
            labels = sequence.labels
            metric_filter = json.dumps({k: labels[k] for k in sorted(labels.keys())})
            scraped_metric_filter.add(metric_filter)
            single_update()

        for metric_filter in insert_tuples.keys() - scraped_metric_filter:
            insert_tuples[metric_filter]["value_list"].append(None)

        for metric_filter in update_tuples.keys() - scraped_metric_filter:
            tup = update_tuples[metric_filter]
            end_time = tup["start"] + len(tup["value_list"]) * step
            if not tup["value_list"]:
                update_tuples[metric_filter]["value_list"].append(None)
                update_tuples[metric_filter]["start"] = ts
            elif ts + step > end_time:
                update_tuples[metric_filter]["value_list"].append(None)

    for update_tuple in update_tuples.values():
        metric_statistics_args = update_tuple.copy()
        stat_id = metric_statistics_args.pop("stat_id")
        if all([i is None for i in metric_statistics_args["value_list"]]):
            dao.metric_statistics.delete_metric_statistics(stat_id)
        else:
            dao.metric_statistics.update_metric_statistics(stat_id, **metric_statistics_args)

    for insert_tuple in insert_tuples.values():
        dao.metric_statistics.insert_metric_statistics(
            metric_name=insert_tuple["metric_name"],
            method=insert_tuple["method"],
            length=insert_tuple["length"],
            step=insert_tuple["step"],
            start=insert_tuple["start"],
            metric_filter=insert_tuple["metric_filter"],
            value_list=json.dumps(insert_tuple["value_list"])
        )


def labels_matched(full_labels, label, label_like):
    for name, val in label.items():
        if val != full_labels.get(name):
            return False

    for name, pattern in label_like.items():
        if pattern == full_labels.get(name):
            continue
        if not re.match(pattern, full_labels.get(name)):
            return False

    return True


def get_meta_metric_sequence(metric_name, metric_filter, metric_filter_like):
    """To get sequence from meta-database"""
    result = []
    if isinstance(metric_name, str) and metric_name not in LONG_TERM_METRIC_STATS:
        return result

    if isinstance(metric_name, str):
        method = metric_name.rsplit("_", 1)[1]
    else:
        method = None

    query = dao.metric_statistics.select_metric_statistics(
        metric_name=metric_name,
        method=method,
    )

    for tup in list(query):
        name = tup.metric_name
        step = tup.step
        start = tup.start
        labels = uniform_labels(json.loads(tup.metric_filter))
        value_list = json.loads(tup.value_list)

        if not labels_matched(labels, metric_filter, metric_filter_like):
            continue

        timestamps = list()
        seq_values = list()
        for i, value in enumerate(value_list):
            if value is None or math.isnan(value):
                continue

            timestamps.append(int((start + i * step) * 1000))
            seq_values.append(value)

        sequence = Sequence(
            timestamps=timestamps,
            values=seq_values,
            name=name,
            labels=labels,
            step=step * 1000
        )
        result.append(sequence)

    return result
