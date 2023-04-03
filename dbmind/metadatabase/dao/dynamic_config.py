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
"""`dynamic_config_set()` and `dynamic_config_get()` startup after get_session().
And the function get_session() can only load SQLite database in the current working directory.
Hence, if we want to use `dynamic_config_set()` or `dynamic_config_get()`, we should change the
working directory to the confpath (the path of configuration files).

    Examples
    ------------
    >>> import os
    >>> os.chdir(confpath)
    >>> dynamic_config_get('foo', 'bar')

If you want to add more dynamic configurations, you should follow the underlying list:

1. Create a python file named config_xxx.py in the module of ```dbmind.metadatabase.schema```;
2. Define an ORM class for your dynamic configurations. You can refer to class ```DynamicConfigDbBase```.
3. Then, all main processes are finished. You can call ```dynamic_config_set()``` and ```dynamic_config_get()```.
functions to modify and read them.
"""
import logging
from collections import defaultdict

from sqlalchemy import update, insert
from sqlalchemy.exc import DBAPIError

from dbmind.cmd.configs.config_constants import IV_TABLE

# Register dynamic config table here.
from ..dynamic_config_db_session import get_session
from ..schema.config_dynamic_params import DynamicParams as Table


def dynamic_config_set(category, name, value):
    if Table is None:
        raise ValueError()

    with get_session() as session:
        # If the table has the given name, then update its value.
        # Otherwise, insert a new row into the table.
        if session.query(Table).filter(Table.category == category,
                                       Table.name == name).count() > 0:
            session.execute(
                update(Table).where(
                    Table.name == name
                ).values(
                    value=value
                ).execution_options(
                    synchronize_session="fetch"
                )
            )
        else:
            session.execute(
                insert(Table).values(category=category, name=name, value=value)
            )


def dynamic_config_get(category, name, fallback=None):
    with get_session() as session:
        try:
            result = tuple(session.query(Table).filter(Table.category == category, Table.name == name))
        except DBAPIError as e:
            logging.exception(e)
            # Return a none value instead of raising an
            # error directly because we have a fault-tolerant mechanism
            # later, giving the fault-tolerant mechanism a chance to try.
            return fallback
        if len(result) == 0:
            return fallback
        return result[0].value


def dynamic_category_configs_get(category):
    params = dict()
    with get_session() as session:
        try:
            result = tuple(session.query(Table).filter(Table.category == category))
        except DBAPIError as e:
            logging.exception(e)
            return None
        for item in result:
            params[item.name] = (item.value, item.annotation)
    return params


def dynamic_configs_list():
    __no_need_for_showing__ = (
        'dbmind_config', IV_TABLE
    )
    rv = defaultdict(list)
    with get_session() as session:
        for tuple_item in session.query(Table):
            if tuple_item.category in __no_need_for_showing__:
                continue

            rv[tuple_item.category].append((tuple_item.name, tuple_item.value, tuple_item.annotation))
    return rv
