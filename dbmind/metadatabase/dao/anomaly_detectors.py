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

"""to manipulate the anomaly detectors table"""

from sqlalchemy import update, asc

from dbmind.metadatabase.ddl import truncate_table
from ..result_db_session import get_session
from ..schema import AnomalyDetectors


def insert_anomaly_detectors(
        cluster_name,
        detector_name,
        alarm_cause,
        alarm_content,
        alarm_level,
        alarm_type,
        extra,
        detector_info,
        duration,
        forecasting_seconds,
        running
):
    """to insert new detectors into anomaly detectors table"""
    with get_session() as session:
        session.add(AnomalyDetectors(
            cluster_name=cluster_name,
            detector_name=detector_name,
            alarm_cause=alarm_cause,
            alarm_content=alarm_content,
            alarm_level=alarm_level,
            alarm_type=alarm_type,
            extra=extra,
            detector_info=detector_info,
            duration=duration,
            forecasting_seconds=forecasting_seconds,
            running=running
        ))


def select_anomaly_detectors(**detector_args):
    """to select the anomaly detectors that meet requirements"""
    with get_session() as session:
        result = session.query(AnomalyDetectors)

        detector_name = detector_args.get("detector_name")
        if detector_name is not None:
            result = result.filter(AnomalyDetectors.detector_name == detector_name)

        result = result.order_by(asc(AnomalyDetectors.detector_id))

        return result


def delete_anomaly_detectors(detector_id_list):
    """to delete the expired anomaly detectors"""
    if not detector_id_list:
        return

    with get_session() as session:
        session.query(AnomalyDetectors).filter(
            AnomalyDetectors.detector_id.in_(detector_id_list)
        ).delete()


def truncate_anomaly_detectors():
    """to truncate the anomaly detectors table"""
    truncate_table(AnomalyDetectors.__tablename__)


def update_anomaly_detectors(detector_id, **detector_args):
    """to update the anomaly detectors"""
    detector_dict = dict()
    for column, value in detector_args.items():
        if value is not None:
            detector_dict[column] = value

    if not detector_dict:
        return

    with get_session() as session:
        session.execute(
            update(AnomalyDetectors)
            .where(AnomalyDetectors.detector_id == detector_id)
            .values(**detector_dict)
            .execution_options(synchronize_session="fetch")
        )
