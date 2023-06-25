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

from dbmind.metadatabase.ddl import truncate_table
from ..result_db_session import get_session
from ..schema import RegularInspection


def insert_regular_inspection(instance, inspection_type, start, end, report=None, state=None, cost_time=None, conclusion=None):
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


def select_metric_regular_inspections(instance=None, inspection_type=None, offset=None, start=None, end=None, limit=None, spec_id=None, show_report=True):
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


def delete_metric_regular_inspections(spec_id):
    with get_session() as session:
        result = session.query(RegularInspection).filter(RegularInspection.id == spec_id).delete()
        return "success"

def delete_old_inspection():
    """To prevent the table from over-expanding."""
    thirty_days = 31 * 24 * 60 * 60 * 1000  # transmit to 'ms'
    retention_start_time = int(time.time()) - thirty_days
    with get_session() as session:
        session.query(RegularInspection).filter(
            RegularInspection.start <= retention_start_time
        ).delete()
