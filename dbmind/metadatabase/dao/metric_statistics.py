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

"""To manipulate the metric statistics table"""

import json

from sqlalchemy import update, asc

from dbmind.metadatabase.ddl import truncate_table
from ..result_db_session import get_session
from ..schema import MetricStatistics


def select_metric_statistics(offset=None, limit=None, **metric_statistics_args):
    """To select the metric statistics info from metric statistics table"""
    with get_session() as session:
        result = session.query(MetricStatistics)

        metric_name = metric_statistics_args.get("metric_name")
        if metric_name is not None:
            result = result.filter(MetricStatistics.metric_name == metric_name)

        method = metric_statistics_args.get("method")
        if method is not None:
            result = result.filter(MetricStatistics.method == method)

        length = metric_statistics_args.get("length")
        if length is not None:
            result = result.filter(MetricStatistics.length == length)

        step = metric_statistics_args.get("step")
        if step is not None:
            result = result.filter(MetricStatistics.step == step)

        result = result.order_by(asc(MetricStatistics.stat_id))

        if offset is not None:
            result = result.offset(offset)

        if limit is not None:
            result = result.limit(limit)

        return result


def update_metric_statistics(stat_id, **metric_statistics_args):
    """To update the metric statistics info in metric statistics table"""
    metric_statistics_filter = dict()
    for column, value in metric_statistics_args.items():
        if value is not None:
            metric_statistics_filter[column] = value

    if not metric_statistics_filter:
        return

    if isinstance(metric_statistics_filter.get("metric_filter"), dict):
        metric_filter = metric_statistics_filter["metric_filter"].copy()
        metric_statistics_filter["metric_filter"] = json.dumps(
            {k: metric_filter[k] for k in sorted(metric_filter.keys())}
        )

    if isinstance(metric_statistics_filter.get("value_list"), list):
        metric_statistics_filter["value_list"] = json.dumps(metric_statistics_filter["value_list"])

    with get_session() as session:
        session.execute(
            update(MetricStatistics)
            .where(MetricStatistics.stat_id == stat_id)
            .values(**metric_statistics_filter)
            .execution_options(synchronize_session="fetch")
        )


def insert_metric_statistics(metric_name, method, length, step,
                             start, metric_filter, value_list):
    """To insert the metric statistics info into metric statistics table"""
    if isinstance(metric_filter, dict):
        metric_filter = json.dumps(
            {k: metric_filter[k] for k in sorted(metric_filter.keys())}
        )

    if isinstance(value_list, list):
        value_list = json.dumps(value_list)

    with get_session() as session:
        session.add(
            MetricStatistics(
                metric_name=metric_name,
                method=method,
                length=length,
                step=step,
                start=start,
                metric_filter=metric_filter,
                value_list=value_list
            )
        )


def delete_metric_statistics(stat_id):
    """To delete the metric statistics info in metric statistics table"""
    with get_session() as session:
        session.query(MetricStatistics).filter(
            MetricStatistics.stat_id == stat_id
        ).delete()


def truncate_statistics():
    """To truncate the metric statistics table"""
    truncate_table(MetricStatistics.__tablename__)
