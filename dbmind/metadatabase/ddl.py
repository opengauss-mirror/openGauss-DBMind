# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from dbmind.common.exceptions import DuplicateTableError, SQLExecutionError
from dbmind.constants import DYNAMIC_CONFIG

from . import load_all_schema_models, ResultDbBase, DynamicConfigDbBase
from .schema.config_dynamic_params import DynamicParams
from .result_db_session import get_session
from .result_db_session import session_clz
from .result_db_session import update_session_clz_from_configs
from .utils import create_dsn


def create_metadatabase_schema(check_first=True):
    update_session_clz_from_configs()
    load_all_schema_models()
    try:
        ResultDbBase.metadata.create_all(
            session_clz.get('engine'),
            checkfirst=check_first
        )
    except Exception as e:
        if 'DuplicateTable' in str(e):
            raise DuplicateTableError(e)
        raise SQLExecutionError(e)


def destroy_metadatabase():
    update_session_clz_from_configs()
    load_all_schema_models()
    try:
        ResultDbBase.metadata.drop_all(
            session_clz.get('engine')
        )
    except Exception as e:
        if 'DuplicateTable' in str(e):
            raise DuplicateTableError(e)
        raise SQLExecutionError(e)


def create_dynamic_config_schema():
    load_all_schema_models()
    engine = create_engine(create_dsn('sqlite', DYNAMIC_CONFIG))
    DynamicConfigDbBase.metadata.create_all(engine)

    # Batch insert default values into config tables.
    with sessionmaker(engine, autocommit=True, autoflush=True)() as session:
        try:
            session.bulk_save_objects(table.default_values())
        except sqlalchemy.exc.IntegrityError as e:
            # May be duplicate, ignore it.
            raise DuplicateTableError(e)


def truncate_table(table_name):
    with get_session() as session:
        if session_clz.get('db_type') == 'sqlite':
            sql_prefix = 'DELETE FROM '
        else:
            sql_prefix = 'TRUNCATE TABLE '
        session.execute(text(sql_prefix + table_name))
        session.commit()
