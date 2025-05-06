# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
"""
dao of security anomalies
"""
import logging
from sqlalchemy import func

from dbmind.metadatabase.dao.forecast_calibration_info import calibration_ts
from dbmind.metadatabase.result_db_session import get_session
from dbmind.metadatabase.schema.security_metric_models import SecurityMetricModels
from dbmind.metadatabase.schema import SecurityAnomalies


def count_anomalies(metric_name, start_period, end_period, host):
    """
    Counts the anomalies a metric had in a given time period
    @param metric_name: string
    @param start_period: int timestamp
    @param end_period: int timestamp
    @param host: str
    @return: int number of anomalies in time period
    """
    count_records = 0
    with get_session() as session:
        query = session.query(func.count(SecurityAnomalies.id)).\
            filter((SecurityAnomalies.host == host),
                   (SecurityAnomalies.metric_name == metric_name),
                   (SecurityAnomalies.metric_time >= start_period),
                   (SecurityAnomalies.metric_time <= end_period))
        for record in query:
            count_records = record[0]
    return count_records


def add_abnormal_value(metric_name, host, anomaly_value, anomaly_time):
    """
    save one abnormal values in the database
    @param metric_name: string
    @param host: string
    @param anomaly_value: double the value
    @param anomaly_time: int timestamp of the anomaly
    @return: None
    """
    try:
        with get_session() as session:
            query = session.query(SecurityAnomalies).filter((SecurityAnomalies.metric_name == metric_name),
                                                            (SecurityAnomalies.metric_time == anomaly_time))
            for _ in query:
                logging.debug("Abnormal value already exists for %s and anomaly_time: %s",
                              metric_name, anomaly_time)
                return
            session.add(
                SecurityAnomalies(
                    host=host,
                    metric_name=metric_name,
                    metric_value=anomaly_value,
                    metric_time=anomaly_time
                )
            )
    except Exception as error:
        logging.error("_add_abnormal_value failed metric_name: %s, anomaly_time: %s", metric_name, anomaly_time)
        logging.exception(error)


def should_first_calibrate_security_metric(metric_name, host):
    """
    @param metric_name: str metric to check for
    @param host: str host to check for
    @return: boolean True if calibration was not done yet
    """
    with get_session() as session:
        query = session.query(func.count(SecurityMetricModels.id)).\
            filter((SecurityMetricModels.metric_name == metric_name), (SecurityMetricModels.host == host))

        count_records = 0
        for record in query:
            count_records = record[0]
        logging.debug("should_calibrate_security_metric -- metric_name: %s, count_records: %s",
                      metric_name, count_records)
        if count_records == 0:
            return True
        return False


def get_calibration_model_age_in_minutes(metric_name, host):
    """
    @param metric_name: str metric to check for
    @param host: str host to check for
    @return: number of minutes since the metric was calibrate,
    if no model found, it will return 0
    """
    min_calibration_ts = 0
    with get_session() as session:
        query = session.query(func.min(SecurityMetricModels.time_saved)).\
            filter((SecurityMetricModels.metric_name == metric_name),
                   (SecurityMetricModels.host == host))
        for record in query:
            min_calibration_ts = record[0]
    if not min_calibration_ts:
        return 0
    return (calibration_ts() - min_calibration_ts) / 60
