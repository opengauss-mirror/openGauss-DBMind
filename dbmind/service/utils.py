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

from dbmind.common.types import Sequence
from dbmind.common.utils.checking import split_ip_port
from dbmind.constants import DISTINGUISHING_INSTANCE_LABEL, EXPORTER_INSTANCE_LABEL


class SequenceUtils:
    @staticmethod
    def from_server(s: Sequence):
        distinguishing = s.labels.get(DISTINGUISHING_INSTANCE_LABEL)
        if distinguishing:
            return distinguishing
        # If the metric does not come from reprocessing-exporter,
        # then return the exporter IP directly.
        return SequenceUtils.exporter_ip(s)

    @staticmethod
    def exporter_address(s: Sequence):
        return s.labels.get(EXPORTER_INSTANCE_LABEL)

    @staticmethod
    def exporter_ip(s: Sequence):
        address = SequenceUtils.exporter_address(s)
        if address:
            return split_ip_port(address)[0].strip()


