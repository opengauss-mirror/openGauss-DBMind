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

from sqlalchemy.engine.url import URL


DB_TYPES = {'sqlite', 'opengauss', 'opengauss', 'postgresql', 'mysql'}


def create_dsn(
        db_type,
        database,
        host=None,
        port=None,
        username=None,
        password=None,
        schema='public'
):
    """Generate a DSN (Data Source Name) according to the user's given parameters.
    Meanwhile, DBMind will adapt some interfaces to SQLAlchemy, such as openGauss."""
    db_type = db_type.lower().strip()
    if db_type not in DB_TYPES:
        raise ValueError("Not supported database type '%s'." % db_type)
    if db_type in ('opengauss', 'opengauss'):
        db_type = 'postgresql'
        # DBMind has to override the following method.
        # Otherwise, SQLAlchemy will raise an exception about unknown version.
        from sqlalchemy.dialects.postgresql.base import PGDialect
        PGDialect._get_server_version_info = lambda *args: (9, 2)
    if db_type == 'sqlite':
        url = URL.create('sqlite', database=database, query={'check_same_thread': 'False'})
    else:
        url = URL.create(db_type, username=username, password=password, host=host, port=port, database=database)
    if db_type == 'postgresql':
        url = URL.create(db_type, username=username, password=password, host=host, port=port, database=database,
                         query={'options': f'-csearch_path={schema}'})
    return url


