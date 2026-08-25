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
import datetime
from unittest import mock

import pytest

from dbmind.common.tsdb.prometheus_client import PrometheusClient


@pytest.fixture(autouse=True)
def get_mock_client(monkeypatch):
    monkeypatch.setattr(PrometheusClient, '_post',
                        mock.MagicMock(return_value=mock.MagicMock(status_code=204, data={"result": "context"})))
    monkeypatch.setattr(PrometheusClient, '_get',
                        mock.MagicMock(return_value=mock.MagicMock(status_code=200, data={"result": "context"})))


def test_prometheus_query(monkeypatch):
    client = PrometheusClient(url='http://127.0.0.1')
    assert client.get_current_metric_value(metric_name="test", min_value=1, max_value=10) == []
    assert client.get_metric_range_data(metric_name="test", min_value=1, max_value=10) == []
    assert client.get_metric_range_data(metric_name="test", min_value=1, max_value=10, step="1000") == []
    from_datetime = datetime.datetime.fromtimestamp(1692087897)
    to_datetime = datetime.datetime.fromtimestamp(1692174298)
    assert client.delete_metric_data(
        metric_name="test", from_datetime=from_datetime, to_datetime=to_datetime, flush=True) is None
    assert client.custom_query(query='test') == []
    assert client.custom_query_range(query='test',
                                     start_time=datetime.datetime.now() - datetime.timedelta(minutes=10),
                                     end_time=datetime.datetime.now(),
                                     step="1000") == []
    assert client.timestamp() == 0
    assert client.name == "prometheus"
