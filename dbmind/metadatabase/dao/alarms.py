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
from sqlalchemy import update, desc, func

from ._common import truncate_table
from ..business_db import get_session
from ..schema import FutureAlarms
from ..schema import HistoryAlarms


def get_batch_insert_history_alarms_functions():
    objs = []

    class _Inner:
        def add(self, instance, alarm_type, start_at, end_at, metric_name,
                alarm_level=None, alarm_content=None, extra_info=None,
                anomaly_type=None
                ):
            obj = HistoryAlarms(
                instance=instance,
                metric_name=metric_name,
                alarm_type=alarm_type,
                alarm_level=alarm_level,
                start_at=start_at,
                end_at=end_at,
                alarm_content=alarm_content,
                extra_info=extra_info,
                anomaly_type=anomaly_type
            )
            objs.append(obj)
            return self

        @staticmethod
        def commit():
            with get_session() as session:
                session.bulk_save_objects(objs)

    return _Inner()


def select_history_alarm(instance=None, alarm_type=None, alarm_level=None, alarm_content=None,
                         start_at=None,
                         end_at=None, offset=None, limit=None, group: bool = False):
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
            result = result.filter(HistoryAlarms.instance == instance)
        if alarm_type is not None:
            result = result.filter(HistoryAlarms.alarm_type == alarm_type)
        if alarm_level is not None:
            result = result.filter(HistoryAlarms.alarm_level == alarm_level)
        if alarm_content is not None:
            result = result.filter(HistoryAlarms.alarm_content == alarm_content)
        if start_at is not None:
            result = result.filter(HistoryAlarms.start_at >= start_at)
        if end_at is not None:
            result = result.filter(HistoryAlarms.end_at <= end_at)
        if group:
            return result.group_by(
                HistoryAlarms.instance,
                HistoryAlarms.alarm_content
            )
        result = result.order_by(desc(HistoryAlarms.start_at))
        if offset is not None:
            result = result.offset(offset)
        if limit is not None:
            result = result.limit(limit)
        return result


def count_history_alarms(instance=None, alarm_type=None, alarm_level=None, group: bool = False):
    return select_history_alarm(
        instance=instance, alarm_type=alarm_type, alarm_level=alarm_level, group=group
    ).count()


def delete_timeout_history_alarms(oldest_occurrence_time):
    with get_session() as session:
        session.query(HistoryAlarms).filter(
            HistoryAlarms.start_at <= oldest_occurrence_time
        ).delete()


def truncate_history_alarm():
    truncate_table(HistoryAlarms.__tablename__)


def update_history_alarm(alarm_id, alarm_status=None, end_at=None, recovery_time=None):
    kwargs = dict()
    if alarm_status is not None:
        kwargs.update(alarm_status=alarm_status)
    if recovery_time is not None:
        kwargs.update(recovery_at=recovery_time)
    if end_at is not None:
        kwargs.update(end_at=end_at)
    if len(kwargs) == 0:
        return

    with get_session() as session:
        session.execute(
            update(HistoryAlarms)
            .where(HistoryAlarms.history_alarm_id == alarm_id)
            .values(**kwargs)
            .execution_options(synchronize_session="fetch")
        )


def get_batch_insert_future_alarms_functions():
    objs = []

    class _Inner:
        def add(self, instance, metric_name, alarm_type,
                alarm_level=None, start_at=None,
                end_at=None, alarm_content=None, extra_info=None
                ):
            obj = FutureAlarms(
                instance=instance,
                metric_name=metric_name,
                alarm_type=alarm_type,
                alarm_level=alarm_level,
                start_at=start_at,
                end_at=end_at,
                alarm_content=alarm_content,
                extra_info=extra_info
            )
            objs.append(obj)
            return self

        @staticmethod
        def commit():
            with get_session() as session:
                session.bulk_save_objects(objs)

    return _Inner()


def select_future_alarm(instance=None, metric_name=None, start_at=None, end_at=None, offset=None, limit=None, group: bool = False):
    with get_session() as session:
        if group:
            result = session.query(
                FutureAlarms.instance,
                FutureAlarms.alarm_content,
                func.count(FutureAlarms.alarm_content)
            )
        else:
            result = session.query(FutureAlarms)
        if metric_name is not None:
            result = result.filter(FutureAlarms.metric_name == metric_name)
        if instance is not None:
            result = result.filter(FutureAlarms.instance == instance)
        if start_at is not None:
            result = result.filter(FutureAlarms.start_at >= start_at)
        if end_at is not None:
            result = result.filter(FutureAlarms.end_at <= end_at)

        if group:
            return result.group_by(FutureAlarms.alarm_content, FutureAlarms.instance)
        result = result.order_by(desc(FutureAlarms.start_at))
        if offset is not None:
            result = result.offset(offset)
        if limit is not None:
            result = result.limit(limit)
        return result


def count_future_alarms(instance=None, metric_name=None, start_at=None, group=False):
    return select_future_alarm(instance=instance, metric_name=metric_name, start_at=start_at, group=group).count()


def truncate_future_alarm():
    truncate_table(FutureAlarms.__tablename__)
