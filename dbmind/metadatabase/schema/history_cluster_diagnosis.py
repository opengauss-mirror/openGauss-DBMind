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

"""Meta-Database Structure """

from sqlalchemy import BigInteger, Column, Index, Integer, String, CHAR, TEXT

from .. import ResultDbBase


class HistoryClusterDiagnosis(ResultDbBase):
    """Meta-Database Structure """
    __tablename__ = "tb_history_cluster_diagnosis"

    diagnosis_id = Column(Integer, primary_key=True, autoincrement=True)
    instance = Column(String(64), nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    cluster_role = Column(CHAR(5), nullable=False)
    diagnosis_method = Column(CHAR(10), nullable=False)
    cluster_feature = Column(TEXT)
    diagnosis_result = Column(String(40), nullable=False)
    status_code = Column(Integer, nullable=False)
    alarm_type = Column(String(16), nullable=False)
    alarm_level = Column(Integer, nullable=False)

    idx_history_cluster_diagnosis = Index(
        "idx_history_cluster_diagnosis",
        instance,
        timestamp,
        cluster_role,
        diagnosis_method,
        status_code
    )
