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
import sys
from typing import List
import logging
from contextlib import contextmanager

import psycopg2

sys.path.append('..')

from .common import BaseExecutor, REMOVE_ANSI_QUOTES_SQL


class DriverExecutor(BaseExecutor):
    def __init__(self, *arg):
        super(DriverExecutor, self).__init__(*arg)
        self.conn = None
        self.cur = None
        with self.session():
            pass

    def __init_conn_handle(self):
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     user=self.user,
                                     password=self.password,
                                     host=self.host,
                                     port=self.port,
                                     application_name='DBMind-index-advisor',
                                     )
        self.cur = self.conn.cursor()
        try:
            self.cur.execute('SHOW sql_compatibility;')
            for _tuple in self.cur.fetchall():
                if _tuple[0] == 'M':
                    self.cur.execute(REMOVE_ANSI_QUOTES_SQL)
                    self.conn.commit()
        except Exception as e:
            logging.warning('Found %s while executing SQL statement.', e)

    def __execute(self, sql):
        if self.cur.closed:
            self.__init_conn_handle()
        try:
            self.cur.execute(sql)
            self.conn.commit()
            if self.cur.rowcount == -1:
                return
            return [(self.cur.statusmessage,)] + self.cur.fetchall()
        except psycopg2.ProgrammingError:
            return [('ERROR',)]
        except Exception as e:
            logging.warning('Found %s while executing SQL statement.', e)
            return [('ERROR ' + str(e),)]
        finally:
            self.conn.rollback()

    def execute_sqls(self, sqls) -> List[str]:
        results = []
        sqls = ['set current_schema = %s' % self.get_schema()] + sqls
        for sql in sqls:
            res = self.__execute(sql)
            if res:
                results.extend(res)
        return results

    def __close_conn(self):
        if self.conn and self.cur:
            self.cur.close()
            self.conn.close()

    @contextmanager
    def session(self):
        self.__init_conn_handle()
        yield
        self.__close_conn()
