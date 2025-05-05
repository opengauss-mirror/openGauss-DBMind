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

import logging
from threading import Lock

from psycopg2.extensions import make_dsn

from dbmind.common.rpc import RPCServer, RPCFunctionRegister
from dbmind.common.opengauss_driver import Driver

_rpc_register = RPCFunctionRegister()
_rpc_service = _rpc_register.register()
_check_lock = Lock()
_agent_exclusive_driver = Driver()  # only for the agent


@_rpc_service
def query_in_postgres(stmt):
    res = _agent_exclusive_driver.query(stmt, force_connection_db='postgres')
    logging.info('[Agent] query_in_postgres: %s; result: %s.', stmt, res)
    return res


@_rpc_service
def query_in_database(stmt, database, return_tuples=False, fetch_all=False, ignore_error=False):
    res = _agent_exclusive_driver.query(
        stmt,
        force_connection_db=database,
        return_tuples=return_tuples,
        fetch_all=fetch_all,
        ignore_error=ignore_error)
    logging.info('[Agent] query_in_database (%s): %s; result: %s.', database, stmt, res)
    return res


def get_driver_address():
    from . import service

    return service.driver.host, service.driver.port


def create_agent_rpc_service():
    def checker(username, pwd):
        if (
            _agent_exclusive_driver.initialized and
            _agent_exclusive_driver.username == username and
            _agent_exclusive_driver.pwd == pwd
        ):
            return True, None
        else:
            _agent_exclusive_driver.initialized = False

        host, port = get_driver_address()
        try:
            with _check_lock:
                dsn = make_dsn(user=username, password=pwd, dbname="postgres", host=host, port=port)
                _agent_exclusive_driver.initialize(dsn)
                del dsn
                return True, None
        except ConnectionError as e:
            return False, str(e)
        except PermissionError as e:
            return False, str(e)

    return RPCServer(_rpc_register, checker)
