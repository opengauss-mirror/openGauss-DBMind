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
"""
SecurityAnomalies model
"""

from sqlalchemy import Column, String, Integer, BigInteger, Float

from .. import ResultDbBase


class SecurityAnomalies(ResultDbBase):
    """Usually immutable."""
    __tablename__ = "tb_security_anomaly_values"
    id = Column(Integer, primary_key=True, autoincrement=True)
    host = Column(String(64), nullable=False)
    metric_name = Column(String(1024), nullable=False)
    metric_time = Column(BigInteger, nullable=False)
    metric_value = Column(Float, nullable=False)
