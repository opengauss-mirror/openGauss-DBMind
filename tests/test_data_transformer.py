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
"""
test_data_transformer
"""
import datetime
import logging
import re
from collections import defaultdict
from unittest import mock

import pytest

from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types import Sequence
from dbmind.constants import PORT_SUFFIX
from dbmind.service.web import data_transformer
from dbmind.service.web.context_manager import ACCESS_CONTEXT_NAME


@pytest.fixture(autouse=True)
def mock_get_access_context(monkeypatch):
    fake_context = {ACCESS_CONTEXT_NAME.TSDB_FROM_SERVERS_REGEX: f'127.0.0.1{PORT_SUFFIX}|127.0.0.2{PORT_SUFFIX}|'
                                                                 f'127.0.0.1|127.0.0.2',
                    ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: ['127.0.0.1:19996', '127.0.0.2:19996'],
                    ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ['127.0.0.1', '127.0.0.2'],
                    ACCESS_CONTEXT_NAME.AGENT_INSTANCE_IP_WITH_PORT: '127.0.0.1:19996'}
    monkeypatch.setattr(data_transformer, 'get_access_context',
                        mock.Mock(side_effect=lambda x: fake_context[x]))


def mock_get_metric_sequence(metric_name, start_time, end_time, step, **kwargs):
    """
    mock the result of get_metric_sequence
    """
    from dbmind.service import dai

    class MockFetcher(dai.LazyFetcher):
        """
        mock the class of dai.LazyFetcher
        """

        def _read_buffer(self):
            return self._fetch_sequence(int(self.start_time.timestamp() * 1000), int(self.end_time.timestamp() * 1000),
                                        self.step)

        def _fetch_sequence(self, *args, **kwargs):
            # create a sequence per minute
            sequences = list()
            timestamps = [int(datetime.datetime.now().timestamp() * 1000),
                          int(datetime.datetime.now().timestamp() * 1000) + 1000]

            self.step = self.step or 5
            if self.metric_name == 'full_sql_online_instance_id':
                time_diff = int(self.end_time.timestamp() - self.start_time.timestamp())
                sequence = Sequence(timestamps=[time_diff],
                                    values=[int(datetime.datetime.now().timestamp() * 1000)],
                                    name=self.metric_name,
                                    labels={'from_instance': time_diff,
                                            'debug_query_id': time_diff, 'transaction_id': time_diff},
                                    step=self.step)
                default_dict = defaultdict(lambda: 'faked_field')
                default_dict.update(sequence.labels)
                setattr(sequence, 'labels', default_dict)
                sequences.append(sequence)
            elif metric_name == 'os_mem_usage':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[0.8, 0.9],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1', 'instance': '127.0.0.1', 'user': 'bob',
                                             'position': 'beijing'},
                                     step=self.step)
                sequence2 = Sequence(timestamps=timestamps,
                                     values=[0.5, 0.6],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.2', 'instance': '127.0.0.2', 'user': 'tom',
                                             'position': 'shanghai'},
                                     step=self.step)
                sequence3 = Sequence(timestamps=timestamps,
                                     values=[0.5, 0.6],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.2', 'instance': '127.0.0.2', 'user': 'jerry',
                                             'position': 'beijing'},
                                     step=self.step)
                sequences.extend([sequence1, sequence2, sequence3])
            # mock filter function
            if self.labels:
                for key, value in self.labels.items():
                    sequences = [sequence for sequence in sequences if sequence.labels.get(key) == value]
            # mock regex filter function
            if self.labels_like:
                for key, value in self.labels_like.items():
                    sequences = [sequence for sequence in sequences if re.match(value, sequence.labels.get(key))]
            return sequences

    return MockFetcher(metric_name, start_time, end_time)


def mock_get_latest_metric_value(metric_name, **kwargs):
    """
    mock the result of get_metric_sequence
    """
    from dbmind.service import dai

    class MockFetcher(dai.LazyFetcher):
        """
        mock the class of dai.LazyFetcher
        """

        def _read_buffer(self):
            self.end_time = datetime.datetime.now()
            self.start_time = datetime.datetime.now() - datetime.timedelta(minutes=3)
            return self._fetch_sequence(int(self.start_time.timestamp() * 1000), int(self.end_time.timestamp() * 1000),
                                        self.step)

        def _fetch_sequence(self, *args, **kwargs):
            # create a sequence per minute
            sequences = list()
            timestamps = [int(datetime.datetime.now().timestamp() * 1000),
                          int(datetime.datetime.now().timestamp() * 1000) + 1000]

            self.step = self.step or 5
            if metric_name == 'os_mem_usage':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[0.8, 0.9],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1', 'instance': '127.0.0.1', 'user': 'bob',
                                             'position': 'beijing'},
                                     step=self.step)
                sequences.extend([sequence1])
            elif metric_name == 'pg_db_blks_read':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[20, 150],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db1'},
                                     step=self.step)
                sequences.extend([sequence1])
            elif metric_name == 'pg_db_blks_hit':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[80, 50],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db1'},
                                     step=self.step)
                sequences.extend([sequence1])
            elif metric_name == 'pg_index_idx_blks_read':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[100, 200],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db1'},
                                     step=self.step)
                sequences.extend([sequence1])
            elif metric_name == 'pg_index_idx_blks_hit':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[100, 200],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db1'},
                                     step=self.step)
                sequences.extend([sequence1])
            elif metric_name == 'pg_db_xact_rollback':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[150, 200],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db1'},
                                     step=self.step)
                sequences.extend([sequence1])
            elif metric_name == 'pg_db_xact_commit':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[50, 50],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db1'},
                                     step=self.step)
                sequences.extend([sequence1])
            elif metric_name == 'pg_database_size_bytes':
                sequence1 = Sequence(timestamps=timestamps,
                                     values=[50, 50],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db1'},
                                     step=self.step)
                sequence2 = Sequence(timestamps=timestamps,
                                     values=[50, 50],
                                     name=self.metric_name,
                                     labels={'from_instance': '127.0.0.1:19996', 'instance': '127.0.0.1:9090',
                                             'datname': 'db2'},
                                     step=self.step)
                sequences.extend([sequence1, sequence2])

            # mock filter function
            if self.labels:
                for key, value in self.labels.items():
                    sequences = [sequence for sequence in sequences if sequence.labels.get(key) == value]
            # mock regex filter function
            if self.labels_like:
                for key, value in self.labels_like.items():
                    sequences = [sequence for sequence in sequences if re.match(value, sequence.labels.get(key))]
            return sequences

    return MockFetcher(metric_name)


def mock_delete_metric_sequence(metric_name, instance, from_timestamp=None,
                                to_timestamp=None, regex=False, labels=None, regex_labels=None, flush=False, tz=None):
    logging.debug('mock delete sequence: metric_name: %s, instance: %s, from_datetime: %s, '
                  'to_datetime: %s, regex: %s, labels: %s, regex_labels: %s, flush: %s, tz: %s.',
                  metric_name, instance, from_timestamp, to_timestamp, regex, labels, regex_labels, flush, tz)


@pytest.fixture(autouse=True)
def mock_dai(monkeypatch):
    """
    mock a instance of dai
    """
    from dbmind.service import dai  # Must be here

    mock_client = mock.MagicMock()
    mock_client_instance = mock_client.return_value
    mock_client_instance.scrape_interval = 15
    monkeypatch.setattr(TsdbClientFactory, 'get_tsdb_client', mock_client)

    # Use faked data source since not found PROMETHEUS_ADDR environment variable.
    monkeypatch.setattr(dai, 'get_metric_sequence', mock_get_metric_sequence)
    monkeypatch.setattr(dai, 'get_latest_metric_value', mock_get_latest_metric_value)
    monkeypatch.setattr(dai, 'delete_metric_sequence', mock_delete_metric_sequence)

    return dai


def test_get_full_sql_statement():
    end_timestamp = int(datetime.datetime.now().timestamp() * 1000)
    start_timestamp = end_timestamp - 15 * 1000

    result_15s_limit_10 = data_transformer.get_full_sql_statement('full_sql_online_instance_id',
                                                                  from_timestamp=start_timestamp,
                                                                  to_timestamp=end_timestamp,
                                                                  min_value=start_timestamp,
                                                                  max_value=end_timestamp,
                                                                  fetch_all=True,
                                                                  limit=10)
    assert len(result_15s_limit_10) == 1

    start_timestamp = end_timestamp - 45 * 1000
    result_45s_limit_10 = data_transformer.get_full_sql_statement('full_sql_online_instance_id',
                                                                  from_timestamp=start_timestamp,
                                                                  to_timestamp=end_timestamp,
                                                                  min_value=start_timestamp,
                                                                  max_value=end_timestamp,
                                                                  fetch_all=True,
                                                                  limit=10)
    assert len(result_45s_limit_10) == 2

    start_timestamp = end_timestamp - 40 * 60 * 1000
    result_40min_limit_5 = data_transformer.get_full_sql_statement('full_sql_online_instance_id',
                                                                   min_value=start_timestamp,
                                                                   max_value=end_timestamp,
                                                                   fetch_all=True,
                                                                   limit=5)
    assert len(result_40min_limit_5) == 5

    result_40min = data_transformer.get_full_sql_statement('full_sql_online_instance_id',
                                                           from_timestamp=start_timestamp,
                                                           to_timestamp=end_timestamp,
                                                           min_value=start_timestamp,
                                                           max_value=end_timestamp,
                                                           fetch_all=True)
    assert len(result_40min) == 40


def test_details_parser():
    details1 = "b'\x01\x00\x00\x00\x02B\x00\x00\x00\x02A8\x00\x00\x00\x02\x00\x00\x00\x01\x13\x00" \
               "BufHashTableSearch\x00d\x00\x00\x00\x00\x00\x00\x00\x04\x0b\x00flush data\x00\x1e" \
               "\x00\x00\x00\x00\x00\x00\x00'"
    details_parser1 = data_transformer.DetailsParser(details1[2:-1].encode('utf-8'))
    expected_wait_events1 = [(1, 'BufHashTableSearch', 100), (4, 'flush data', 30)]
    for area_type, area in details_parser1.parse_areas():
        if area_type == 65:
            assert expected_wait_events1 == sorted(details_parser1.parse_wait_events(area), key=lambda x: -x[-1])
    details2 = "b'\x01\x98\x01\x00\x00b\x00\x00\x00\x02AX\x00\x00\x00\x03\x00\x00\x00\x01\x13\x00" \
               "BufHashTableSearch\x009\x01\x00\x00\x00\x00\x00\x00\x04\x0b\x00flush data\x00$\x00" \
               "\x00\x00\x00\x00\x00\x00\x04\x15\x00HashAgg - build hash\x00\xfc\x00\x00\x00\x00\x00\x00\x00'"
    details_parser2 = data_transformer.DetailsParser(details2[2:-1].encode('utf-8'))
    expected_wait_events2 = [(1, 'BufHashTableSearch', 313), (4, 'HashAgg - build hash', 252), (4, 'flush data', 36)]
    for area_type, area in details_parser2.parse_areas():
        if area_type == 65:
            assert expected_wait_events2 == sorted(details_parser2.parse_wait_events(area), key=lambda x: -x[-1])


def test_get_metric_sequence_internal():
    fetcher = data_transformer.get_metric_sequence_internal("os_mem_usage")
    assert isinstance(fetcher.fetchone(), Sequence)
    assert isinstance(fetcher.fetchall(), list)
    assert len(fetcher.fetchall()) == 3


def test_get_metric_sequence():
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance='127.0.0.1', fetch_all=False)
    assert len(sequence) == 1
    assert sequence[0]['labels']['from_instance'] == '127.0.0.1'
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance='127.0.0.2', fetch_all=False)
    assert sequence[0]['labels']['from_instance'] == '127.0.0.2'
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance='127.0.0.2', fetch_all=True)
    assert len(sequence) == 2
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance='127.0.0.2', labels='{"user": "jerry"}',
                                                    fetch_all=True)
    assert len(sequence) == 1
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance='127.0.0.2',
                                                    regex_labels='{"user": "jer.*"}', fetch_all=True)
    assert len(sequence) == 1
    assert sequence[0]['labels']['user'] == 'jerry'
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance=None, fetch_all=True)
    assert len(sequence) == 3
    sequence = data_transformer.get_metric_sequence('tps', instance='127.0.0.1', regex=True)
    assert len(sequence) == 1
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance='127.0.0.2', labels='"user": "jerry"',
                                                    fetch_all=True)
    assert len(sequence) == 2


def test_get_latest_metric_sequence():
    sequence = data_transformer.get_latest_metric_sequence('os_mem_usage', instance='127.0.0.1', latest_minutes=3,
                                                           fetch_all=False)
    assert len(sequence) == 1
    assert sequence[0]['labels']['from_instance'] == '127.0.0.1'
    sequence = data_transformer.get_latest_metric_sequence('os_mem_usage', instance='127.0.0.2', latest_minutes=3,
                                                           fetch_all=False)
    assert sequence[0]['labels']['from_instance'] == '127.0.0.2'
    sequence = data_transformer.get_latest_metric_sequence('os_mem_usage', instance='127.0.0.2', latest_minutes=3,
                                                           fetch_all=True)
    assert len(sequence) == 2
    sequence = data_transformer.get_latest_metric_sequence('os_mem_usage', instance='127.0.0.2', latest_minutes=3,
                                                           labels='{"user": "jerry"}', fetch_all=True)
    assert len(sequence) == 1
    sequence = data_transformer.get_latest_metric_sequence('os_mem_usage', instance='127.0.0.2', latest_minutes=3,
                                                           regex_labels='{"user": "jer.*"}', fetch_all=True)
    assert len(sequence) == 1
    assert sequence[0]['labels']['user'] == 'jerry'
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance=None, fetch_all=True)
    assert len(sequence) == 3
    sequence = data_transformer.get_metric_sequence('tps', instance='127.0.0.1', regex=True)
    assert len(sequence) == 1
    sequence = data_transformer.get_metric_sequence('os_mem_usage', instance='127.0.0.2', labels='"user": "jerry"',
                                                    fetch_all=True)
    assert len(sequence) == 2


def test_get_metric_value():
    fetcher = data_transformer.get_metric_value('os_mem_usage')
    assert len(fetcher.fetchall()) == 1


def test_delete_metric_sequence():
    end_timestamp = int(datetime.datetime.now().timestamp() * 1000)
    from_timestamp = end_timestamp - 3 * 60 * 1000
    data_transformer.delete_metric_sequence('os_mem_usage', '127.0.0.1', from_timestamp,
                                            end_timestamp, regex=False, labels='{"user: "jerry""}',
                                            regex_labels=None, flush=False, tz='UTC+8:00')


def test_stat_buffer_hit():
    assert data_transformer.stat_buffer_hit() == {'127.0.0.1:19996': {'db1': 0.7999992000007999}}


def test_stat_idx_hit():
    assert data_transformer.stat_idx_hit() == {'127.0.0.1:19996': {'db1': 0.499999750000125}}


def test_stat_xact_successful():
    assert data_transformer.stat_xact_successful() == {'127.0.0.1:19996': {'db1': 0.2499998750000625}}


def test_stat_group_by_instance():
    to_agg_tbl = {'127.0.0.1': {'t1': 1, 't2': 5}, '127.0.0.2': {'t1': 5, 't2': 10}}
    assert data_transformer.stat_group_by_instance(to_agg_tbl) == {'127.0.0.1': 3.0, '127.0.0.2': 7.5}


def test_get_database_list():
    assert set(data_transformer.get_database_list()) == {'db1', 'db2'}
