# Copyright (c) 2026 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
# See the Mulan PSL v2 for more details.

import pytest

from dbmind.common.tsdb.prometheus_client import PrometheusClient, label_to_query


class MockResponse:
    status_code = 200

    @staticmethod
    def json():
        return {'data': {'result': []}}


def test_label_to_query_escapes_promql_label_values():
    query = label_to_query(
        {'instance': 'db"1\\prod'},
        {'role': 'primary|standby'}
    )

    assert query == '{instance="db\\"1\\\\prod",role=~"primary|standby"}'


def test_label_to_query_rejects_injected_label_name():
    with pytest.raises(ValueError):
        label_to_query({'instance"} or up{': 'db1'})


def test_get_current_metric_value_rejects_invalid_metric_name():
    client = PrometheusClient('http://localhost:9090')

    with pytest.raises(ValueError):
        client.get_current_metric_value('up} or process_cpu_seconds_total{')


def test_get_metric_range_data_escapes_promql_label_values(monkeypatch):
    captured = {}
    client = PrometheusClient('http://localhost:9090')

    def mock_get(url, **kwargs):
        captured.update(kwargs['params'])
        return MockResponse()

    monkeypatch.setattr(client, '_get', mock_get)

    client.get_metric_range_data(
        'up',
        label_config={'instance': 'db"1} or process_cpu_seconds_total{'},
        params={'labels_like': {'role': 'primary"|standby'}},
        step='30s'
    )

    assert captured['query'] == 'up{instance="db\\"1} or process_cpu_seconds_total{",role=~"primary\\"|standby"}'


def test_get_metric_range_data_rejects_invalid_metric_name():
    client = PrometheusClient('http://localhost:9090')

    with pytest.raises(ValueError):
        client.get_metric_range_data('up} or process_cpu_seconds_total{')


def test_delete_metric_data_rejects_invalid_metric_name():
    client = PrometheusClient('http://localhost:9090')

    with pytest.raises(ValueError):
        client.delete_metric_data('up} or process_cpu_seconds_total{', 1, 2)


def test_delete_metric_data_rejects_injected_label_name():
    client = PrometheusClient('http://localhost:9090')

    with pytest.raises(ValueError):
        client.delete_metric_data('up', 1, 2, labels={'instance"} or up{': 'db1'})
