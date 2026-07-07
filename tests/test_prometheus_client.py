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


def test_delete_metric_data_rejects_invalid_metric_name():
    client = PrometheusClient('http://localhost:9090')

    with pytest.raises(ValueError):
        client.delete_metric_data('up} or process_cpu_seconds_total{', 1, 2)
