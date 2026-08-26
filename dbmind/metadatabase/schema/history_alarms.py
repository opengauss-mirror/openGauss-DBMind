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

from sqlalchemy import Column, String, Integer, BigInteger, Index, TEXT

from .. import ResultDbBase


class HistoryAlarms(ResultDbBase):
    __tablename__ = "tb_history_alarms"

    history_alarm_id = Column(Integer, primary_key=True, autoincrement=True)
    instance = Column(String(64), nullable=False)
    metric_name = Column(String(64), nullable=False)
    metric_filter = Column(TEXT)
    alarm_type = Column(String(16), nullable=False)
    alarm_level = Column(Integer, nullable=False)
    start_at = Column(BigInteger, nullable=False)  # unix timestamp
    end_at = Column(BigInteger, nullable=False)  # unix timestamp
    alarm_content = Column(String(1024))
    extra_info = Column(TEXT)
    anomaly_type = Column(String(64), nullable=False)
    alarm_cause = Column(TEXT)

    idx_history_alarms = Index(
        "idx_history_alarms",
        instance,
        metric_name,
        anomaly_type,
        start_at,
        end_at,
    )
