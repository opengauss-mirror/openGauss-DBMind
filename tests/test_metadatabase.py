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

import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, session

from dbmind.common.types import ALARM_TYPES, ALARM_LEVEL
from dbmind.constants import DYNAMIC_CONFIG
from dbmind.metadatabase import result_db_session, ResultDbBase
from dbmind.metadatabase.ddl import create_dynamic_config_schema
from dbmind.metadatabase.dao.alarms import *
from dbmind.metadatabase.dao.dynamic_config import dynamic_config_get, dynamic_config_set
from dbmind.metadatabase.dao.slow_queries import *


@pytest.fixture(scope='module', autouse=True)
def initialize_metadb():
    dbname = 'test_metadatabase1.db'
    os.path.exists(dbname) and os.remove(dbname)
    os.path.exists(DYNAMIC_CONFIG) and os.remove(DYNAMIC_CONFIG)

    engine = create_engine('sqlite:///' + dbname)
    session_maker = sessionmaker(autoflush=False, bind=engine)

    result_db_session.session_clz.update(
        engine=engine,
        session_maker=session_maker,
        db_type='sqlite'
    )

    ResultDbBase.metadata.create_all(engine)

    yield

    # Clean up
    session.close_all_sessions()
    os.path.exists(dbname) and os.remove(dbname)
    os.path.exists(DYNAMIC_CONFIG) and os.remove(DYNAMIC_CONFIG)


def test_slow_queries():
    start_time = time.time() * 1000
    insert_slow_query('127.0.0.1:1234', 'schema', 'db0', 'query0', 10, 10, root_cause='a')
    insert_slow_query('127.0.0.1:1234', 'schema', 'db0', 'query1', 11, 11, root_cause='b')
    insert_slow_query('127.0.0.1:1234', 'schema', 'db0', 'query1', 11, 12, root_cause='c')

    for i, hash_pair in enumerate(((10, 10), (11, 11), (11, 12))):
        result = select_slow_query_id_by_hashcode(*hash_pair)
        s_id = list(result)[0][0]
        assert s_id == i + 1
        insert_slow_query_journal(s_id, start_time, duration_time=1000, instance='127.0.0.1:1234')
        insert_slow_query_journal(s_id, start_time + 1000, duration_time=2000, instance='127.0.0.1:1234')

    count = 0
    for query in select_slow_queries(instance='127.0.0.1:1234',
                                     target_list=('query', 'db_name', 'schema_name', 'insert_at')):
        count += 1
        assert query.schema_name == 'schema'
        assert query.db_name == 'db0'
        assert query.insert_at <= start_time + 3000 + 1000
    assert count == count_slow_queries()

    field_names = ('query', 'db_name', 'root_cause')
    result = select_slow_queries(instance='127.0.0.1:1234',
                                 target_list=field_names,
                                 start_time=start_time,
                                 end_time=start_time + 1000)
    assert result.count() == count

    result = list(select_slow_queries(target_list=('insert_at',),
                                      start_time=start_time,
                                      end_time=start_time + 1000))
    assert result[0]['insert_at'] // 1000 == start_time // 1000

    truncate_slow_queries()
    assert count_slow_queries() == 0


def test_history_alarms():
    truncate_history_alarm()
    function = get_batch_insert_history_alarms_functions
    for _ in range(2):
        function().add(
            Alarm(
                instance='127.0.0.1',
                metric_name='os_cpu_usage',
                metric_filter={},
                alarm_type=ALARM_TYPES.SYSTEM,
                alarm_level=ALARM_LEVEL.INFO,
                start_timestamp=int(time.time() * 1000),
                end_timestamp=int(time.time() * 1000) + 3000,
                alarm_content='CPU exceeds.',
                extra="{'node_id': 1, 'msg': 'test'}",
                anomaly_type="a"
            )
        ).commit()
    assert count_history_alarms() == 2

    alarm_args = {'instance': '127.0.0.2', 'metric_name': 'os_disk_usage'}
    for alarm_id in range(1, 3):
        update_history_alarm(alarm_id, **alarm_args)
    alarm_ids = list()
    result = select_history_alarm()
    field_names = result.statement.columns.keys()
    assert field_names == ['history_alarm_id', 'instance', 'metric_name', 'metric_filter', 'alarm_type',
                           'alarm_level', 'start_at', 'end_at', 'alarm_content', 'extra_info', 'anomaly_type',
                           'alarm_cause']

    last_occurrence_time = float('inf')
    for alarm in result:  # ordered by descend alarm's start time
        assert alarm.start_at < last_occurrence_time  # due to descend
        assert alarm.extra_info == "{'node_id': 1, 'msg': 'test'}"
        assert alarm.instance == '127.0.0.2'
        assert alarm.metric_name == 'os_disk_usage'
        last_occurrence_time = alarm.start_at
        alarm_ids.append(alarm.history_alarm_id)

    delete_timeout_history_alarms(oldest_occurrence_time=int(time.time() * 1000 + 3000))
    assert count_history_alarms() == 0


def test_dynamic_config_db():
    create_dynamic_config_schema()
    assert dynamic_config_get('slow_query_threshold', 'index_number_threshold') == '6'
    dynamic_config_set('slow_query_threshold', 'index_number_threshold', '1')
    assert dynamic_config_get('slow_query_threshold', 'index_number_threshold') == '1'
