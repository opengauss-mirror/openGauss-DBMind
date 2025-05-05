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
from unittest import mock

import pytest

from dbmind.common.tsdb.influxdb_client import InfluxdbClient

_res = {
    "results": [
        {
            "xxx_id": 0,
            "series": [
                {
                    "name": "metric1",
                    "tags": {
                        "__name__": "opengauss_exporter_fixed_info",
                        "dbname": "db1",
                        "from_instance": "instance1",
                        "job": "opengauss_exporter"
                    },
                    "columns": ["time", "value"],
                    "values": [
                        [1714292448775, 0]
                    ]
                }
            ]
        }
    ]
}


class Response:
    def __init__(self, status_code, headers=None, body=None, res=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.body = body or ''
        self.res = res

    def json(self):
        return self.res

    def ok(self):
        return self.status_code == 200


def mock_get():
    return Response(200, res=_res)


@pytest.fixture(autouse=True)
def get_mock_client(monkeypatch):
    monkeypatch.setattr(InfluxdbClient, '_get',
                        mock.MagicMock(return_value=mock_get()))


def test_influxdb_query(monkeypatch):
    client = InfluxdbClient(url='http://127.0.0.1')
    assert client.get_current_metric_value(metric_name="metric1")[0].labels['from_instance'] == 'instance1'
    assert client.get_metric_range_data(metric_name="metric1")[0].labels['dbname'] == 'db1'
    assert client.custom_query(query='test')[0].values == (0.0, )
    assert client.name == 'influxdb'
