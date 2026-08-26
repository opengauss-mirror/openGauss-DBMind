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

from dbmind.app import timed_app
from dbmind.constants import DYNAMIC_CONFIG
from dbmind.metadatabase import result_db_session, ResultDbBase


@pytest.fixture(scope='module', autouse=True)
def initialize_metadb():
    dbname = 'test_metadatabase.db'
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


def test_anomaly_detection():
    timed_app.anomaly_detection()
