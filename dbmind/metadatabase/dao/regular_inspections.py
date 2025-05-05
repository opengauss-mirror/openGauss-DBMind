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
import time

from sqlalchemy import desc

from dbmind import global_vars
from dbmind.metadatabase.ddl import truncate_table
from ..result_db_session import get_session
from ..schema import RegularInspection


def insert_regular_inspection(instance, inspection_type, start, end, report=None, state=None, cost_time=None,
                              conclusion=None):
    with get_session() as session:
        session.add(
            RegularInspection(
                instance=instance,
                inspection_type=inspection_type,
                start=start,
                end=end,
                report=report,
                state=state,
                cost_time=cost_time,
                conclusion=conclusion
            )
        )


def truncate_metric_regular_inspections():
    truncate_table(RegularInspection.__tablename__)


def count_metric_regular_inspections(instance=None, inspection_type=None):
    return select_metric_regular_inspections(instance=instance, inspection_type=inspection_type).count()


def select_metric_regular_inspections(instance=None, inspection_type=None, offset=None, start=None, end=None,
                                      limit=None, spec_id=None, show_report=True):
    with get_session() as session:
        if show_report:
            result = session.query(RegularInspection.instance,
                                   RegularInspection.report,
                                   RegularInspection.start,
                                   RegularInspection.end,
                                   RegularInspection.id,
                                   RegularInspection.state,
                                   RegularInspection.cost_time,
                                   RegularInspection.inspection_type)
        else:
            result = session.query(RegularInspection.instance,
                                   RegularInspection.start,
                                   RegularInspection.end,
                                   RegularInspection.id,
                                   RegularInspection.state,
                                   RegularInspection.cost_time,
                                   RegularInspection.inspection_type)
        if inspection_type is not None:
            result = result.filter(RegularInspection.inspection_type == inspection_type)
        if instance is not None:
            result = result.filter(RegularInspection.instance == instance)
        if start is not None:
            result = result.filter(RegularInspection.start >= start)
        if end is not None:
            result = result.filter(RegularInspection.end <= end)
        if spec_id is not None:
            result = result.filter(RegularInspection.id == spec_id)
        result = result.order_by(desc(RegularInspection.id))
        if offset is not None:
            result = result.offset(offset)
        if limit is not None:
            result = result.limit(limit)
        return result


def delete_metric_regular_inspections(instance, spec_id):
    with get_session() as session:
        _ = session.query(RegularInspection).filter(RegularInspection.instance == instance).filter(
            RegularInspection.id == spec_id).delete()
        return "success"


def delete_old_inspection():
    """To prevent the table from over-expanding."""
    real_time_retention = global_vars.dynamic_configs.get_int_or_float('metadatabase_params',
                                                                       'real_time_inspection_retention', fallback=31)
    daily_retention = global_vars.dynamic_configs.get_int_or_float('metadatabase_params', 'daily_inspection_retention',
                                                                   fallback=400)
    weekly_retention = global_vars.dynamic_configs.get_int_or_float('metadatabase_params',
                                                                    'weekly_inspection_retention', fallback=720)
    monthly_retention = global_vars.dynamic_configs.get_int_or_float('metadatabase_params',
                                                                     'monthly_inspection_retention', fallback=720)
    now_time = int(time.time())
    one_day = 24 * 60 * 60 * 1000
    real_time_start_time = now_time - real_time_retention * one_day
    daily_start_time = now_time - daily_retention * one_day
    weekly_start_time = now_time - weekly_retention * one_day
    monthly_start_time = now_time - monthly_retention * one_day

    with get_session() as session:
        session.query(RegularInspection).filter(RegularInspection.inspection_type == 'real_time_check').filter(
            RegularInspection.start <= real_time_start_time).delete()
        session.query(RegularInspection).filter(RegularInspection.inspection_type == 'daily_check').filter(
            RegularInspection.start <= daily_start_time).delete()
        session.query(RegularInspection).filter(RegularInspection.inspection_type == 'weekly_check').filter(
            RegularInspection.start <= weekly_start_time).delete()
        session.query(RegularInspection).filter(RegularInspection.inspection_type == 'monthly_check').filter(
            RegularInspection.start <= monthly_start_time).delete()
