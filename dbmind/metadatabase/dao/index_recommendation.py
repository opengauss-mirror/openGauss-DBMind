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

from sqlalchemy import func

from ._common import truncate_table
from ..business_db import get_session
from ..schema import ExistingIndexes
from ..schema import IndexRecommendation
from ..schema import IndexRecommendationStats
from ..schema import IndexRecommendationStmtDetails
from ..schema import IndexRecommendationStmtTemplates


def clear_data():
    truncate_table(ExistingIndexes.__tablename__)
    truncate_table(IndexRecommendation.__tablename__)
    truncate_table(IndexRecommendationStmtDetails.__tablename__)
    truncate_table(IndexRecommendationStmtTemplates.__tablename__)


def insert_recommendation_stat(instance, db_name, stmt_count, positive_stmt_count,
                               table_count, rec_index_count,
                               redundant_index_count, invalid_index_count, stmt_source):
    with get_session() as session:
        session.add(IndexRecommendationStats(
            instance=instance,
            db_name=db_name,
            recommend_index_count=rec_index_count,
            redundant_index_count=redundant_index_count,
            invalid_index_count=invalid_index_count,
            stmt_count=stmt_count,
            positive_stmt_count=positive_stmt_count,
            table_count=table_count,
            stmt_source=stmt_source
        ))


def get_latest_recommendation_stat(instance=None):
    with get_session() as session:
        if instance is not None:
            return session.query(IndexRecommendationStats).filter(
                IndexRecommendationStats.occurrence_time == func.max(
                    IndexRecommendationStats.occurrence_time).select(),
                IndexRecommendationStats.instance == instance
            )
        return session.query(IndexRecommendationStats).filter(
            IndexRecommendationStats.occurrence_time == func.max(
                IndexRecommendationStats.occurrence_time).select())


def get_recommendation_stat(instance=None):
    with get_session() as session:
        if instance is not None:
            return session.query(IndexRecommendationStats).filter(
                IndexRecommendationStats.instance == instance
            )
        return session.query(IndexRecommendationStats)


def get_advised_index(instance=None):
    with get_session() as session:
        if instance is not None:
            return session.query(IndexRecommendation).filter(
                IndexRecommendation.index_type == 1, IndexRecommendation.instance == instance
            )
        return session.query(IndexRecommendation).filter(IndexRecommendation.index_type == 1)


def get_advised_index_details(instance=None):
    with get_session() as session:
        if instance is not None:
            return session.query(IndexRecommendationStmtDetails, IndexRecommendationStmtTemplates,
                                 IndexRecommendation).filter(
                IndexRecommendationStmtDetails.template_id == IndexRecommendationStmtTemplates.id).filter(
                IndexRecommendationStmtDetails.index_id == IndexRecommendation.id).filter(
                IndexRecommendationStmtDetails.correlation_type == 0,
                IndexRecommendationStmtDetails.instance == instance
            )
        return session.query(IndexRecommendationStmtDetails, IndexRecommendationStmtTemplates,
                             IndexRecommendation).filter(
            IndexRecommendationStmtDetails.template_id == IndexRecommendationStmtTemplates.id).filter(
            IndexRecommendationStmtDetails.index_id == IndexRecommendation.id).filter(
            IndexRecommendationStmtDetails.correlation_type == 0)


def get_existing_indexes(instance=None):
    with get_session() as session:
        if instance is None:
            return session.query(ExistingIndexes)
        return session.query(ExistingIndexes).filter(
            ExistingIndexes.instance == instance
        )


def insert_existing_index(instance, db_name, tb_name, columns, index_stmt):
    with get_session() as session:
        session.add(ExistingIndexes(instance=instance,
                                    db_name=db_name,
                                    tb_name=tb_name,
                                    columns=columns,
                                    index_stmt=index_stmt))


def insert_recommendation(instance, db_name, schema_name, tb_name, columns, index_type, index_stmt, optimized=None,
                          stmt_count=None, select_ratio=None, insert_ratio=None, update_ratio=None,
                          delete_ratio=None):
    with get_session() as session:
        session.add(IndexRecommendation(instance=instance,
                                        db_name=db_name,
                                        schema_name=schema_name,
                                        tb_name=tb_name,
                                        columns=columns,
                                        optimized=optimized,
                                        index_type=index_type,
                                        stmt_count=stmt_count,
                                        select_ratio=select_ratio,
                                        insert_ratio=insert_ratio,
                                        update_ratio=update_ratio,
                                        delete_ratio=delete_ratio,
                                        index_stmt=index_stmt))


def get_template_start_id():
    with get_session() as session:
        return session.query(func.min(IndexRecommendationStmtTemplates.id)).first()[0]


def insert_recommendation_stmt_details(template_id, db_name, stmt, optimized, correlation_type, stmt_count):
    with get_session() as session:
        session.add(IndexRecommendationStmtDetails(
            index_id=session.query(func.max(IndexRecommendation.id)).first()[0],
            template_id=template_id,
            db_name=db_name,
            stmt=stmt,
            optimized=optimized,
            correlation_type=correlation_type,
            stmt_count=stmt_count
        ))


def insert_recommendation_stmt_templates(template, db_name):
    with get_session() as session:
        session.add(IndexRecommendationStmtTemplates(
            db_name=db_name,
            template=template
        ))
