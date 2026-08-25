# Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
from datetime import datetime, timedelta

from dbmind.common.utils import cached_property

from .tsdb_client import TsdbClient


class DummyClient(TsdbClient):
    def __init__(self):
        """Functions as a dummy client, called when user chooses to ignore TSDB."""
        pass

    def check_connection(self, params: dict = None) -> bool:
        return True

    def get_current_metric_value(
            self, metric_name: str, label_config: dict = None,
            min_value: float = None, max_value: float = None, params: dict = None
    ):
        return []

    def get_metric_range_data(
            self,
            metric_name: str,
            label_config: dict = None,
            start_time: datetime = (datetime.now() - timedelta(minutes=10)),
            end_time: datetime = datetime.now(),
            chunk_size: timedelta = None,
            step: str = None,
            min_value: float = None,
            max_value: float = None,
            params: dict = None
    ):
        return []

    def custom_query(self, query: str, timeout=None, params: dict = None):
        return []

    def timestamp(self):
        return int(time.time() * 1000)

    @cached_property
    def scrape_interval(self):
        return 15

    @property
    def all_metrics(self):
        return []

    @property
    def name(self):
        return "prometheus"
