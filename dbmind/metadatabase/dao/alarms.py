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

from sqlalchemy import update, desc, func, and_, or_

from dbmind.common.types import Alarm
from dbmind.metadatabase.ddl import truncate_table
from ..result_db_session import get_session
from ..schema import HistoryAlarms


def get_batch_insert_history_alarms_functions():
    objs = []

    class _Inner:
        def add(self, alarm: Alarm):
            alarm_metric_filter = ",".join([f"{k}={alarm.metric_filter.get(k, '')}"
                                            for k in sorted(alarm.metric_filter.keys())])
            obj = HistoryAlarms(
                instance=alarm.instance,
                metric_name=alarm.metric_name,
                metric_filter=alarm_metric_filter,
                alarm_type=str(alarm.alarm_type),
                alarm_level=alarm.alarm_level,
                start_at=alarm.start_timestamp,
                end_at=alarm.end_timestamp,
                alarm_content=alarm.alarm_content,
                extra_info=alarm.extra,
                anomaly_type=alarm.anomaly_type,
                alarm_cause=alarm.alarm_cause
            )
            objs.append(obj)
            return self

        @staticmethod
        def commit():
            with get_session() as session:
                session.bulk_save_objects(objs)

    return _Inner()


def select_history_alarm(instance=None, offset=None, limit=None, group: bool = False,
                         **alarm_args):
    with get_session() as session:
        if group:
            result = session.query(
                HistoryAlarms.instance,
                HistoryAlarms.alarm_content,
                func.count(HistoryAlarms.alarm_content),
            )
        else:
            result = session.query(HistoryAlarms)

        if instance is not None:
            if isinstance(instance, list):
                result = result.filter(HistoryAlarms.instance.in_(instance))
            elif isinstance(instance, str):
                result = result.filter(HistoryAlarms.instance == instance)

        metric_name = alarm_args.get("metric_name")
        if metric_name is not None:
            result = result.filter(HistoryAlarms.metric_name == metric_name)

        metric_filter = alarm_args.get("metric_filter")
        if metric_filter is not None:
            result = result.filter(HistoryAlarms.metric_filter == metric_filter)

        alarm_type = alarm_args.get("alarm_type")
        if alarm_type is not None:
            result = result.filter(HistoryAlarms.alarm_type == alarm_type)

        alarm_level = alarm_args.get("alarm_level")
        if alarm_level is not None:
            result = result.filter(HistoryAlarms.alarm_level == alarm_level)

        start_at = alarm_args.get("start_at")
        end_at = alarm_args.get("end_at")
        if start_at is not None and end_at is not None:
            result = result.filter(
                or_(
                    and_(HistoryAlarms.start_at >= start_at, HistoryAlarms.start_at <= end_at),
                    and_(HistoryAlarms.start_at <= start_at, HistoryAlarms.end_at >= start_at)
                )
            )
        elif start_at is not None:
            result = result.filter(HistoryAlarms.start_at >= start_at)
        elif end_at is not None:
            result = result.filter(HistoryAlarms.end_at <= end_at)

        alarm_content = alarm_args.get("alarm_content")
        if alarm_content is not None:
            result = result.filter(HistoryAlarms.alarm_content == alarm_content)

        anomaly_type = alarm_args.get("anomaly_type")
        if anomaly_type is not None:
            result = result.filter(HistoryAlarms.anomaly_type == anomaly_type)

        alarm_cause = alarm_args.get("alarm_cause")
        if alarm_cause is not None:
            result = result.filter(HistoryAlarms.alarm_cause == alarm_cause)

        if group:
            return result.group_by(HistoryAlarms.instance, HistoryAlarms.alarm_content)

        result = result.order_by(desc(HistoryAlarms.start_at),
                                 desc(HistoryAlarms.history_alarm_id))
        if offset is not None:
            result = result.offset(offset)

        if limit is not None:
            result = result.limit(limit)

        return result


def count_history_alarms(instance=None, **alarm_args):
    return select_history_alarm(instance=instance, **alarm_args).count()


def delete_timeout_history_alarms(oldest_occurrence_time):
    with get_session() as session:
        session.query(HistoryAlarms).filter(
            HistoryAlarms.end_at <= oldest_occurrence_time
        ).delete()


def truncate_history_alarm():
    truncate_table(HistoryAlarms.__tablename__)


def update_history_alarm(alarm_id, **alarm_args):
    alarm_filter = dict()
    for column, value in alarm_args.items():
        if value is not None:
            alarm_filter[column] = value

    if not alarm_filter:
        return

    with get_session() as session:
        session.execute(
            update(HistoryAlarms)
            .where(HistoryAlarms.history_alarm_id == alarm_id)
            .values(**alarm_filter)
            .execution_options(synchronize_session="fetch")
        )
