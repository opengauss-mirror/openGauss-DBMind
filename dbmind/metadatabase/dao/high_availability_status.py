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

from ..result_db_session import get_session
from ..schema import HighAvailabilityStatus


def insert_high_availability_status(instance=None, timestamp=None, interface_type=None, interface_info=None):
    """to insert high availability status"""
    with get_session() as session:
        session.add(
            HighAvailabilityStatus(
                instance=instance,
                timestamp=timestamp,
                interface_type=interface_type,
                interface_info=interface_info
            )
        )


def delete_timeout_high_availability_status(oldest_occurrence_time):
    """to delete timeout high availability status"""
    with get_session() as session:
        session.query(HighAvailabilityStatus).filter(
            HighAvailabilityStatus.timestamp <= oldest_occurrence_time
        ).delete()
