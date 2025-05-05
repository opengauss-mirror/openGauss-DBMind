# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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

"""to control the history cluster diagnosis records"""

from sqlalchemy import asc, update

from dbmind.metadatabase.ddl import truncate_table
from ..result_db_session import get_session
from ..schema import HistoryClusterDiagnosis


def get_batch_insert_history_cluster_diagnosis_functions():
    objs = []

    class _Inner:
        def add(self, record: dict):
            obj = HistoryClusterDiagnosis(
                instance=record.get('instance'),
                timestamp=record.get('timestamp'),
                cluster_role=record.get('role'),
                diagnosis_method=record.get('method'),
                cluster_feature=record.get('feature'),
                diagnosis_result=record.get('result'),
                status_code=record.get('status_code'),
                alarm_type=record.get('alarm_type'),
                alarm_level=record.get('alarm_level'),
            )
            objs.append(obj)
            return self

        @staticmethod
        def commit():
            with get_session() as session:
                session.bulk_save_objects(objs)

    return _Inner()


def select_history_cluster_diagnosis(query_all=True, offset=None, limit=None,
                                     **diagnosis_args):
    """to select history cluster diagnosis records"""
    with get_session() as session:
        result = session.query(HistoryClusterDiagnosis)

        if not query_all:
            result = result.with_entities(
                HistoryClusterDiagnosis.timestamp,
                HistoryClusterDiagnosis.status_code
            )

        instance = diagnosis_args.get("instance")
        if instance is not None:
            if isinstance(instance, list):
                result = result.filter(HistoryClusterDiagnosis.instance.in_(instance))
            elif isinstance(instance, str):
                result = result.filter(HistoryClusterDiagnosis.instance == instance)

        cluster_role = diagnosis_args.get("cluster_role")
        if cluster_role is not None:
            result = result.filter(HistoryClusterDiagnosis.cluster_role == cluster_role)

        diagnosis_method = diagnosis_args.get("diagnosis_method")
        if diagnosis_method is not None:
            result = result.filter(HistoryClusterDiagnosis.diagnosis_method == diagnosis_method)

        start_at = diagnosis_args.get("start_at")
        if start_at is not None:
            result = result.filter(HistoryClusterDiagnosis.timestamp >= start_at)

        end_at = diagnosis_args.get("end_at")
        if end_at is not None:
            result = result.filter(HistoryClusterDiagnosis.timestamp <= end_at)

        status_code = diagnosis_args.get('status_code')
        if status_code is not None:
            result = result.filter(HistoryClusterDiagnosis.status_code == status_code)

        is_normal = diagnosis_args.get('is_normal')
        if not is_normal:
            result = result.filter(HistoryClusterDiagnosis.status_code != -1)

        alarm_type = diagnosis_args.get("alarm_type")
        if alarm_type is not None:
            result = result.filter(HistoryClusterDiagnosis.alarm_type == alarm_type)

        alarm_level = diagnosis_args.get("alarm_level")
        if alarm_level is not None:
            result = result.filter(HistoryClusterDiagnosis.alarm_level == alarm_level)

        result = result.order_by(asc(HistoryClusterDiagnosis.timestamp),
                                 asc(HistoryClusterDiagnosis.diagnosis_id))

        if offset is not None:
            result = result.offset(offset)

        if limit is not None:
            result = result.limit(limit)

        return result


def count_history_cluster_diagnosis(**diagnosis_args):
    """to count history cluster diagnosis records"""
    return select_history_cluster_diagnosis(**diagnosis_args).count()


def delete_timeout_history_cluster_diagnosis(oldest_occurrence_time):
    """to delete timeout history cluster diagnosis records"""
    with get_session() as session:
        session.query(HistoryClusterDiagnosis).filter(
            HistoryClusterDiagnosis.timestamp <= oldest_occurrence_time
        ).delete()


def truncate_history_cluster_diagnosis():
    """to truncate the history cluster diagnosis records"""
    truncate_table(HistoryClusterDiagnosis.__tablename__)


def update_history_cluster_diagnosis(diagnosis_id, **diagnosis_args):
    """to update the history cluster diagnosis records"""
    diagnosis_filter = dict()
    for column, value in diagnosis_args.items():
        if value is not None:
            diagnosis_filter[column] = value

    if not diagnosis_filter:
        return

    with get_session() as session:
        session.execute(
            update(HistoryClusterDiagnosis)
            .where(HistoryClusterDiagnosis.diagnosis_id == diagnosis_id)
            .values(**diagnosis_filter)
            .execution_options(synchronize_session="fetch")
        )
