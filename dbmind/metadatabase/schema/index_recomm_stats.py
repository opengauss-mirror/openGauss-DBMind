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
from sqlalchemy import Column, Integer, String, BigInteger

from .. import ResultDbBase


class IndexRecommendationStats(ResultDbBase):
    __tablename__ = "tb_index_recommendation_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instance = Column(String(64), nullable=False)
    db_name = Column(String(32), nullable=False)
    recommend_index_count = Column(Integer)
    redundant_index_count = Column(Integer)
    invalid_index_count = Column(Integer)
    stmt_count = Column(Integer)
    positive_stmt_count = Column(Integer)
    table_count = Column(Integer)
    stmt_source = Column(String(24), nullable=False)
    occurrence_time = Column(BigInteger)
