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

import psycopg2

from dbmind.common.utils import escape_single_quote


class Executor:
    def __init__(self, dbname, user, password, host, port, schema='public'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.schema = schema
        self.init_conn_handle()
        self.is_m_compat = self.is_m_compatibility()

    def init_conn_handle(self):
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     user=self.user,
                                     password=self.password,
                                     host=self.host,
                                     port=self.port,
                                     application_name='DBMind-sql-rewriter'
                                     )

    def _execute(self, sql):
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql)
                self.conn.commit()
                return cur.fetchall()
            except ConnectionError:
                pass
            except Exception as e:
                logging.warning('Database connector raised an exception: %s.', e)
                self.conn.rollback()

    def is_m_compatibility(self):
        sql = "SHOW sql_compatibility;"
        return self._execute(sql)[0][0] == 'M'

    def get_table_columns(self, table_name):
        info_schema_columns = 'information_schema.columns'
        if self.is_m_compat:
            info_schema_columns = 'information_schema.gs_columns'
        sql = f"SELECT column_name, ordinal_position FROM {info_schema_columns} WHERE table_name='{escape_single_quote(table_name)}'" \
              f" AND table_schema = '{escape_single_quote(self.schema)}' AND table_catalog = '{escape_single_quote(self.dbname)}';"
        results = sorted(self._execute(sql), key=lambda x: x[1])
        return [result[0] for result in results]

    def exists_primary_key(self, table_name):
        sql = f"SELECT pg_catalog.count(*)  FROM information_schema.table_constraints WHERE " \
              f"constraint_type in ('PRIMARY KEY', 'UNIQUE') AND table_name = '{escape_single_quote(table_name)}'" \
              f" AND table_schema = '{escape_single_quote(self.schema)}' AND constraint_catalog = '{escape_single_quote(self.dbname)}';"
        return self._execute(sql)[0][0] > 0

    def get_notnull_columns(self, table_name):
        info_schema_columns = 'information_schema.columns'
        if self.is_m_compat:
            info_schema_columns = 'information_schema.gs_columns'
        sql = f"SELECT column_name FROM {info_schema_columns} WHERE table_catalog = '{self.dbname}' " \
              f"AND table_schema = '{escape_single_quote(self.schema)}' AND table_name = '{escape_single_quote(table_name)}'" \
              f"AND is_nullable = 'NO';"
        return [_tuple[0] for _tuple in self._execute(sql)]

    def syntax_check(self, sql):
        if sql.upper().startswith('TRUNCATE TABLE'):
            return True
        if not self._execute('SET current_schema=%s;EXPLAIN %s' % (self.schema, sql)):
            return False
        return True

    def get_num_cn(self):
        """

        Returns: the num of cn

        """
        sql = "select pg_catalog.count(*) from pg_catalog.pgxc_node where node_type='C';"
        result = self._execute(sql)
        return int(result[0][0]) if result and result[0] else None
