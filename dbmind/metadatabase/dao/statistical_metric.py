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
from ._common import truncate_table
from ..business_db import get_session
from ..schema import StatisticalMetric


def insert_record(metric_name, instance, date, avg_val, min_val=0, max_val=0, the_95_quantile=0):
    with get_session() as session:
        session.add(
            StatisticalMetric(
                metric_name=metric_name,
                instance=instance,
                date=date,
                avg=avg_val,
                min=min_val,
                max=max_val,
                the_95_quantile=the_95_quantile,
            )
        )


def truncate():
    truncate_table(StatisticalMetric.__tablename__)


def count_records(instance=None, metric_name=None):
    return select_metric_statistic_avg_records(instance, metric_name).count()    


def select_metric_statistic_records(instance=None, metric_name=None, offset=None, limit=None):
    with get_session() as session:
        result = session.query(StatisticalMetric)
        if instance is not None:
            result = result.filter(StatisticalMetric.instance == instance)
        if metric_name is not None:
            result = result.filter(StatisticalMetric.metric_name == metric_name)
        if offset is not None:
            result = result.offset(offset)
        if limit is not None:
            result = result.limit(limit)
        return result


def select_metric_statistic_avg_records(instance=None, metric_name=None, only_avg=None, only_max=None,
                                        offset=None, limit=None):
    with get_session() as session:
        if only_avg is not None:
            result = session.query(StatisticalMetric.avg)
        elif only_max is not None:
            result = session.query(StatisticalMetric.max)
        else:
            result = session.query(StatisticalMetric)
        if instance is not None:
            result = result.filter(StatisticalMetric.instance == instance)
        if metric_name is not None:
            result = result.filter(StatisticalMetric.metric_name == metric_name)
        if offset is not None:
            result = result.offset(offset)
        if limit is not None:
            result = result.limit(limit)
        return result
