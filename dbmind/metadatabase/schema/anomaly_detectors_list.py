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

"""Anomaly detectors meta-Database Structure"""

from sqlalchemy import Column, Index, Integer, String, TEXT

from .. import ResultDbBase


class AnomalyDetectors(ResultDbBase):
    """Anomaly detectors meta-Database Structure"""
    __tablename__ = "tb_anomaly_detectors"

    detector_id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_name = Column(TEXT, nullable=False)
    detector_name = Column(String(120), nullable=False)
    alarm_cause = Column(TEXT, nullable=True)
    alarm_content = Column(TEXT, nullable=True)
    alarm_level = Column(String(10), nullable=False)
    alarm_type = Column(String(12), nullable=False)
    extra = Column(TEXT)
    detector_info = Column(TEXT, nullable=False)
    duration = Column(Integer, nullable=False)
    forecasting_seconds = Column(Integer, nullable=False)
    running = Column(Integer, nullable=False)

    idx_anomaly_detectors = Index(
        "idx_anomaly_detectors",
        detector_name
    )
