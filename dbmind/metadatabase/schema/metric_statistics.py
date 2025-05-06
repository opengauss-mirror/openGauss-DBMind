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

"""The metric statistics table"""

from sqlalchemy import Column, String, Integer, BigInteger, Index, TEXT

from .. import ResultDbBase


class MetricStatistics(ResultDbBase):
    """The metric statistics table"""
    __tablename__ = "tb_metric_statistics"

    stat_id = Column(Integer, primary_key=True, autoincrement=True)
    metric_name = Column(String(64), nullable=False)
    method = Column(String(24), nullable=False)
    length = Column(Integer, nullable=False)
    step = Column(Integer, nullable=False)
    start = Column(BigInteger, nullable=False)  # unix timestamp
    metric_filter = Column(TEXT, nullable=False)
    value_list = Column(TEXT, nullable=False)

    idx_history_alarms = Index(
        "tb_statistics",
        metric_name,
        method,
        length,
        step
    )
