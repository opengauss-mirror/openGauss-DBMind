# Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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

from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictRow

from dbmind.common.opengauss_driver import Driver


class MicroserviceClient:
    def __init__(self, connection_kwargs):
        self.connection_kwargs = connection_kwargs
        self.dsn = make_dsn(**self.connection_kwargs)
        self._driver = Driver()
        try:
            self._driver.initialize(self.dsn)
        except Exception:
            raise ConnectionError("Failed to connect to the database, please check your input connection info "
                                  "and the status of your target database.")

    def call(self, funcname, *args, **kwargs):
        if not args:
            return []

        result = []
        if funcname == 'query_in_postgres':
            try:
                result = self._driver.query(stmt=args[0], force_connection_db='postgres', **kwargs)
            except Exception as e:
                logging.exception(e)
        elif funcname == 'query_in_database':
            try:
                result = self._driver.query(stmt=args[0], force_connection_db=args[1], **kwargs)
            except Exception as e:
                logging.exception(e)
        else:
            raise ValueError(f"Unknown function name {funcname}.")

        return result_to_dict(result)

    def call_with_another_credential(
            self, username, password,
            funcname, *args, **kwargs
    ):
        old_username = self.connection_kwargs.get('user')
        old_password = self.connection_kwargs.get('password')
        old_driver = self._driver
        self.connection_kwargs['user'] = username
        self.connection_kwargs['password'] = password

        new_dsn = make_dsn(**self.connection_kwargs)
        self._driver = Driver()
        self._driver.initialize(new_dsn)

        result = []
        try:
            result = self.call(funcname, *args, **kwargs)
        except Exception as e:
            logging.exception(e)

        self.connection_kwargs['user'] = old_username
        self.connection_kwargs['password'] = old_password
        self._driver = old_driver

        return result


def result_to_dict(query_result):
    """
    The execution result of driver is list of RealDictRow, we need to transit it to dict.
    :param query_result: The direct execution result of driver.
    :return: The execution in dict.
        e.g. [{'col1': 'val1', 'col2': 'val2'}, {'col1': 'val3', 'col2': 'val4'}]
    """
    if not isinstance(query_result, list) or not query_result:
        return []
    if all(res is None for res in query_result):
        return query_result

    return_tuples = None
    for res in query_result:
        if isinstance(res, RealDictRow):
            return_tuples = False
            break
        elif isinstance(res, list) or isinstance(res, tuple):
            if res and isinstance(res[0], RealDictRow):
                return_tuples = False
            else:
                return_tuples = True
            break
    if return_tuples is None:
        raise ValueError('The data structure of execution result is unknown.')

    ret = list()
    if not return_tuples:
        for row in query_result:
            if row is None:
                continue
            if isinstance(row, RealDictRow):
                dict_based_row = dict()
                for k, v in tuple(row.items()):
                    dict_based_row[k] = v
                ret.append(dict_based_row)
            elif isinstance(row, list):
                for r in row:
                    dict_based_row = dict()
                    for k, v in tuple(r.items()):
                        dict_based_row[k] = v
                    ret.append(dict_based_row)
    else:
        for res in query_result:
            if res is None:
                ret.append(None)
            elif isinstance(res, list):
                ret.append([list(i) for i in res])
            elif isinstance(res, tuple):
                ret.append(list(res))
    return ret
