# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
import threading
import time
from unittest import mock
from collections import defaultdict

import psycopg2
import requests

from dbmind.common.rpc import RPCClient
from dbmind.common.tsdb.prometheus_client import PrometheusClient
from dbmind.common.types.sequence import Sequence
from dbmind.common.utils import exporter as exporter_utils
from dbmind.components import opengauss_exporter
from dbmind.components.opengauss_exporter.core import controller as oe_controller
from dbmind.components.opengauss_exporter.core.controller import app
from dbmind.components.opengauss_exporter.core.main import ExporterMain as OpenGaussExporterMain
from dbmind.components.opengauss_exporter.core.main import parse_argv as og_parse_argv
from dbmind.components.reprocessing_exporter.core import controller as re_controller
from dbmind.components.reprocessing_exporter.core.main import ExporterMain
from dbmind.components.reprocessing_exporter.core.main import parse_argv
from dbmind.components.reprocessing_exporter.core.service import MetricConfig


from .test_rpc import rpc_client_testing

assert rpc_client_testing


def test_reprocessing_exporter(monkeypatch):
    monkeypatch.setattr(PrometheusClient, 'custom_query', mock.MagicMock(
        return_value=[Sequence((1, 2, 3), (100, 200, 300),
                               name='os_cpu_usage',
                               labels={'from_instance': '127.0.0.1'})]
    ))
    get_prometheus_status = mock.MagicMock(return_value=(True, 'http', None))

    monkeypatch.setattr(PrometheusClient, 'check_connection', mock.Mock(return_value=True))
    monkeypatch.setattr(re_controller, 'run', mock.MagicMock())
    monkeypatch.setattr(exporter_utils, 'get_prometheus_status', get_prometheus_status)
    ExporterMain(parse_argv(['127.0.0.1', '1234', '--disable-https'])).run()
    get_prometheus_status.assert_called_once()
    re_controller.run.assert_called_once()

    # fix issue #I60QZJ
    def mock_query(self):
        labels = {k: 'v' for k in self._label_map}
        return [Sequence(
            name=self.name,
            labels=labels,
            timestamps=(1,),
            values=(1,)
        )]

    monkeypatch.setattr(MetricConfig, 'query', mock_query)

    assert re_controller.query_all_metrics().startswith(b'# HELP')


def test_http_and_rpc_service(monkeypatch, rpc_client_testing):
    expected = [[True], ['aaa']]
    dict_expected = [{'a': True, 'b': 'aaa'}]

    mock_psycopg2_connect = mock.MagicMock()
    mock_conn = mock_psycopg2_connect.return_value

    def mock_cursor_func(**kwargs):
        retval = mock.MagicMock()
        final_return_set = retval.__enter__.return_value
        if 'cursor_factory' in kwargs:
            final_return_set.fetchall.return_value = dict_expected
        else:
            final_return_set.fetchall.return_value = expected
        return retval

    mock_conn.cursor = mock_cursor_func

    monkeypatch.setattr(psycopg2, 'connect', mock_psycopg2_connect)
    monkeypatch.setattr(psycopg2.extensions, 'parse_dsn', lambda arg: defaultdict(lambda: 'aaa'))
    monkeypatch.setattr(opengauss_exporter.core.opengauss_driver.DriverBundle, 'is_monitor_admin', lambda s: True)
    monkeypatch.setattr(opengauss_exporter.core.opengauss_driver.DriverBundle, 'is_standby', lambda s: False)

    exporter = OpenGaussExporterMain(
        og_parse_argv(['--url', 'postgresql://a:b@127.0.0.1:1234/testdb', '--disable-https',
                       '--web.listen-port', '65520']))
    thr = threading.Thread(
        target=exporter.run, args=(),
        name='FakeExporter'
    )
    thr.start()

    while not app.started:
        time.sleep(0.1)

    # test for agent RPC:
    client = RPCClient('http://127.0.0.1:65520/rpc', 'a', 'b')
    res = client.call('query_in_postgres', 'select version();')
    assert res == dict_expected

    # test for metric collecting.
    res = requests.get('http://127.0.0.1:65520/metrics')
    assert res.text.count('# TYPE') > 0
    oe_controller.app.shutdown()
    thr.join()
