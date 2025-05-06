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

import contextlib
import logging

import psycopg2
from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import SingletonThreadPool

from dbmind import global_vars
from dbmind.common.utils.checking import prepare_ip

from .utils import create_dsn
from ..common.utils import write_to_terminal

session_clz = dict()


def update_session_clz_from_configs(is_terminal):
    db_type = global_vars.configs.get('METADATABASE', 'dbtype')
    database = global_vars.configs.get('METADATABASE', 'database')
    host = global_vars.configs.get('METADATABASE', 'host')
    port = global_vars.configs.get('METADATABASE', 'port')
    username = global_vars.configs.get('METADATABASE', 'username')
    password = global_vars.configs.get('METADATABASE', 'password')

    if db_type in ('opengauss', 'opengauss', 'postgresql'):
        valid_port = port.strip() != '' and port is not None
        valid_host = host.strip() != '' and host is not None
        if not valid_port:
            raise ValueError('Invalid port for metadatabase %s: %s.' % (db_type, port))
        if not valid_host:
            raise ValueError('Invalid host for metadatabase %s: %s.' % (db_type, host))
        if len(port.split(',')) > 1 and (len(host.split(',')) != len(port.split(','))):
            raise ValueError('Invalid host or port for metadatabase %s: %s.' % (db_type, host))

    hosts = list(map(lambda x: x.strip(), host.split(',')))
    ports = list(map(lambda x: x.strip(), port.split(',')))
    if len(ports) == 1:
        ports = len(hosts) * ports
    session_clz.clear()
    search_for_primary(database, db_type, hosts, password, ports, username, is_terminal)


def search_for_primary(database, db_type, hosts, password, ports, username, is_terminal):
    for host, port in zip(hosts, ports):
        if session_clz:
            break
        dsn = create_dsn(db_type, database, host, port, username, password)
        postgres_dsn = create_dsn(db_type, 'postgres', host, port, username, password)
        if db_type == 'sqlite':
            engine = create_engine(dsn, pool_pre_ping=True, poolclass=SingletonThreadPool)
        else:
            engine = create_engine(dsn, pool_pre_ping=True,
                                   pool_size=10, max_overflow=10, pool_recycle=25,
                                   connect_args={'connect_timeout': 5, 'application_name': 'DBMind-Service'})
        session_maker = sessionmaker(bind=engine, autocommit=True)
        if db_type not in ('opengauss', 'opengauss', 'postgresql'):
            session_clz.update(
                host=host,
                port=port,
                postgres_dsn=postgres_dsn,
                dsn=dsn,
                engine=engine,
                session_maker=session_maker,
                db_type=db_type,
                db_name=database
            )
            continue
        try:
            conn = engine.connect()
        except (Exception, psycopg2.Error):
            output_connection_info(msg=f'Can not create a valid connection to {prepare_ip(host)}:{port}',
                                   is_terminal=is_terminal, level='error')
            continue
        try:
            # the following statement only support pg database
            result = conn.execute(text("SELECT pg_is_in_recovery();"))
            for row in result:
                # if the dn is primary, then the value of row['pg_is_in_recovery'] is False, or True
                if not row['pg_is_in_recovery']:
                    session_clz.update(
                        host=host,
                        port=port,
                        postgres_dsn=postgres_dsn,
                        dsn=dsn,
                        engine=engine,
                        session_maker=session_maker,
                        db_type=db_type,
                        db_name=database
                    )
                    output_connection_info(msg=f'Current metadatabase connection is {prepare_ip(host)}:{port}',
                                           is_terminal=is_terminal, level='info')
                else:
                    output_connection_info(msg=f'{prepare_ip(host)}:{port} is not a primary data node',
                                           is_terminal=is_terminal, level='error')
        except Exception:
            output_connection_info(msg=f'Failed to execute query on {prepare_ip(host)}:{port}',
                                   is_terminal=is_terminal, level='error')
        finally:
            try:
                conn.close()
            except UnboundLocalError:
                output_connection_info(msg=f'The connection to {prepare_ip(host)}:{port} '
                                           f'has been closed or has not been created',
                                       is_terminal=is_terminal, level='info')


log_level_map = {
    "level": {
        "INFO": "info",
        "WARN": "info",
        "ERROR": "error"
    },
    "color": {
        "INFO": "green",
        "WARN": "yellow",
        "ERROR": "red"
    }
}


def output_connection_info(msg, is_terminal, level):
    """
    - param msg: The tip context.
    - param is_terminal: Print message in terminal, or in log.
    - param level: The log level: ['info', 'warn', 'error']
    """
    if is_terminal:
        write_to_terminal(message=f'{level.upper()}: {msg}', level=log_level_map.get("level").get(level.upper()),
                          color=log_level_map.get("color").get(level.upper()))
    else:
        if level.upper() == 'WARN':
            logging.warning(msg)
        elif level.upper() == 'ERROR':
            logging.error(msg)
        else:
            logging.info(msg)


@contextlib.contextmanager
def get_session():
    if not session_clz:
        update_session_clz_from_configs(is_terminal=False)

    if not session_clz:
        raise ConnectionError("Can not get a valid connection to metadatabase")

    session = session_clz['session_maker']()
    session.begin()
    try:
        yield session
        session.commit()

    except Exception:
        session.rollback()
        logging.info("Current metadatabase connection is %s:%s",
                     prepare_ip(session_clz['host']), session_clz['port'])
        update_session_clz_from_configs(is_terminal=False)
        if session_clz:
            logging.warning("Switch to another metadatabase connection: %s:%s",
                            prepare_ip(session_clz['host']), session_clz['port'])
        else:
            raise ConnectionError("Can not get a valid connection to metadatabase")
    finally:
        session.close()
