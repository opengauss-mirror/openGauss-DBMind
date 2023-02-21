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
import configparser
import glob
import logging
import os
import sys
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest

from dbmind import global_vars
from dbmind.constants import DYNAMIC_CONFIG
from dbmind.metadatabase import create_dynamic_config_schema
from dbmind.cmd.config_utils import DynamicConfig
from dbmind.cmd.edbmind import get_worker_instance
from dbmind.common.tsdb.tsdb_client_factory import TsdbClientFactory
from dbmind.common.types import Sequence

metadatabase_name = 'test_metadatabase.db'
prom_addr = os.environ.get('PROMETHEUS_ADDR', 'hostname:9090')
prom_host, prom_port = prom_addr.split(':')

configs = configparser.ConfigParser()
configs.add_section('TSDB')
configs.set('TSDB', 'name', 'prometheus')
configs.set('TSDB', 'host', prom_host)
configs.set('TSDB', 'port', prom_port)
configs.add_section('SELF-DIAGNOSIS')
configs.add_section('SELF-HEALING')

configs.add_section('METADATABASE')
configs.set('METADATABASE', 'dbtype', 'sqlite')
configs.set('METADATABASE', 'host', '')
configs.set('METADATABASE', 'port', '')
configs.set('METADATABASE', 'username', '')
configs.set('METADATABASE', 'password', '')
configs.set('METADATABASE', 'database', metadatabase_name)
configs.set('SELF-DIAGNOSIS', 'diagnosis_time_window', '300')
configs.set('SELF-HEALING', 'enable_self_healing', 'False')

create_dynamic_config_schema()
DynamicConfig.set(
    'self_monitoring', 'detection_interval', '600'
)
DynamicConfig.set(
    'self_monitoring', 'last_detection_time', '600'
)
DynamicConfig.set(
    'self_monitoring', 'forecasting_future_time', '86400'
)

DynamicConfig.set(
    'self_monitoring', 'result_storage_retention', '600'
)
DynamicConfig.set(
    'self_optimization', 'max_index_storage', '100'
)
DynamicConfig.set(
    'self_optimization', 'max_reserved_period', '100'
)
DynamicConfig.set(
    'self_optimization', 'max_template_num', '10'
)
DynamicConfig.set(
    'self_optimization', 'optimization_interval', '86400'
)
DynamicConfig.set(
    'self_optimization', 'kill_slow_query', 'true'
)

global_vars.must_filter_labels = {}
if os.path.exists(metadatabase_name):
    os.unlink(metadatabase_name)

global_vars.configs = configs
global_vars.dynamic_configs = DynamicConfig
global_vars.worker = get_worker_instance('local', -1)
TsdbClientFactory.set_client_info(
    global_vars.configs.get('TSDB', 'name'),
    global_vars.configs.get('TSDB', 'host'),
    global_vars.configs.get('TSDB', 'port'),
)


def clean_up():
    global_vars.worker.terminate(False)
    try:
        if os.path.exists(metadatabase_name):
            os.unlink(metadatabase_name)
    except Exception:
        pass

    try:
        if os.path.exists(DYNAMIC_CONFIG):
            os.unlink(DYNAMIC_CONFIG)
    except Exception:
        pass

    for logfile in glob.glob('*log'):
        try:
            os.unlink(logfile)
        except Exception:
            pass


def mock_get_current_metric_value(metric_name, label_config=None, *args, **kwargs):
    s = Sequence(name=metric_name, labels=label_config,
                 timestamps=[int(time.time() * 1000)], values=[1])
    d = defaultdict(lambda: 'faked_field')
    d.update(s.labels)
    setattr(s, 'labels', d)
    return [s]


def mock_get_metric_sequence(metric_name, start_time, end_time):
    from dbmind.service import dai

    class MockFetcher(dai.LazyFetcher):
        def _fetch_sequence(self, *args, **kwargs):
            hosts = ('xx.xx.xx.100:1234', 'xx.xx.xx.101:5678', 'xx.xx.xx.102:1111')
            rv = list()

            self.step = self.step or 5

            random_scope = 0.5
            timestamps = list(
                range(int(self.start_time.timestamp()), int(self.end_time.timestamp()) + self.step,
                      self.step))
            values = timestamps.copy()
            # set random values
            for _ in range(0, min(len(timestamps) // 5, 5)):
                idx = random.randint(0, len(timestamps) - 1)
                current_value = values[idx]
                values[idx] = random.randint(
                    int(current_value * (1 - random_scope)),
                    int(current_value * (1 + random_scope))
                )

            for host in hosts:
                s = Sequence(timestamps=timestamps,
                             values=values,
                             name=self.metric_name,
                             labels={'from_instance': host},
                             step=self.step)
                d = defaultdict(lambda: 'faked_field')
                d.update(s.labels)
                setattr(s, 'labels', d)
                rv.append(s)
            return rv

    return MockFetcher(metric_name, start_time, end_time)


def mock_get_latest_metric_sequence(metric_name, minutes):
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    if metric_name == 'pg_sql_statement_history_exc_time':
        return mock_get_slow_query_sequences()

    return mock_get_metric_sequence(metric_name, start_time, end_time)


counter = -1


def mock_get_slow_query_sequences():
    global counter

    from . import mock_slow_queries

    fetchers = [mock_slow_queries.SimpleQueryMockedFetcher('pg_sql_statement_history'),
                mock_slow_queries.ComplexQueryMockedFetcher('pg_sql_statement_history'),
                mock_slow_queries.LockedQueryMockedFetcher('pg_sql_statement_history'),
                mock_slow_queries.SimpleQueryMockedFetcher('pg_sql_statement_history'),
                mock_slow_queries.CommitQueryMockedFetcher('pg_sql_statement_history'),
                mock_slow_queries.SystemQueryMockedFetcher('pg_sql_statement_history'),
                mock_slow_queries.SimpleQueryMockedFetcher('pg_sql_statement_history'),
                mock_slow_queries.ShieldQueryMockedFetcher('pg_sql_statement_history')]

    counter += 1
    return fetchers[counter % len(fetchers)]


def mock_save(*args, **kwargs):
    logging.debug('mock save: args: %s, kwargs: %s.', args, kwargs)


@pytest.fixture(autouse=True)
def mock_dai(monkeypatch):
    from dbmind.service import dai  # Must be here

    if prom_host == 'hostname':
        mock_client = mock.MagicMock()
        mock_client_instance = mock_client.return_value
        mock_client_instance.get_current_metric_value = mock_get_current_metric_value
        monkeypatch.setattr(TsdbClientFactory, 'get_tsdb_client', mock_client)

        # Use faked data source since not found PROMETHEUS_ADDR environment variable.
        monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
        monkeypatch.setattr(dai, 'get_latest_metric_sequence', mock_get_latest_metric_sequence)

    dai.get_all_last_monitoring_alarm_logs = MagicMock(
        return_value=()
    )

    dai.save_history_alarms = mock_save
    dai.save_slow_queries = mock_save
    dai.save_forecast_sequence = mock_save
    dai.save_future_alarms = mock_save

    return dai


@pytest.fixture(autouse=True)
def run_before_and_after_tests(caplog):
    print("Starting to run all tests.")

    yield

    # Determine whether there are ERROR logs.
    errors = [record for record in caplog.get_records('call')
              if record.levelno >= logging.ERROR]
    for error in errors:
        print(error.getMessage(), file=sys.stderr)
    assert not errors

    clean_up()
    print('Tear down all tests.')
