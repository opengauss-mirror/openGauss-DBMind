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

from abc import abstractmethod
from typing import List

from dbmind.common.parser.others import parse_mixed_quotes_string

REMOVE_ANSI_QUOTES_SQL = """SET sql_mode = REPLACE(@@sql_mode, 'ANSI_QUOTES,', '');
SET sql_mode = REPLACE(@@sql_mode, ',ANSI_QUOTES', '');
SET sql_mode = REPLACE(@@sql_mode, 'ANSI_QUOTES', '');"""


class BaseExecutor:
    def __init__(self, dbname, user, password, host, port, schema, driver=None):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.schema = schema
        self.driver = driver

    def get_schemas(self):
        return parse_mixed_quotes_string(self.schema)

    def get_schema(self):
        return self.schema

    @abstractmethod
    def execute_sqls(self, sqls) -> List[str]:
        pass

    @abstractmethod
    def session(self):
        pass
