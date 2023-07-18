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

from .gen_com import dbmind_assert
from .database_connector import DatabaseConnector


class openGaussDatabaseConnector(DatabaseConnector):
    def __init__(self, args, autocommit=False):
        DatabaseConnector.__init__(self, args, autocommit=autocommit)
        self.args = args

        self.db_name = args.database
        logging.debug("Database connector created: {}".format(self.db_name))

        self.db_system = "openGauss"
        self._connection = None

        self.create_connection()

        self.set_random_seed()

        logging.info("openGuass connector created: {}".format(self.db_name))

    def create_connection(self):
        if self._connection:
            self.close()

        self.db_name = self.args.database

        self._connection = psycopg2.connect(host=self.args.host,
                                            database=self.args.database,
                                            user=self.args.user,
                                            password=self.args.password)
        self._connection.autocommit = self.autocommit
        self._cursor = self._connection.cursor()

    def set_random_seed(self, value=0.17):
        logging.info(f"openGauss: Set random seed `SELECT setseed({value})`")
        self.exec_only(f"SELECT setseed({value})")

    def database_names(self):
        result = self.exec_fetch("select datname from pg_database", False)
        return [x[0] for x in result]

    def indexes_size(self):
        statement = (
            "select sum(pg_indexes_size(table_name::text)) from "
            "(select table_name from information_schema.tables "
            "where table_schema='public') as all_tables"
        )
        result = self.exec_fetch(statement)

        return result[0]

    def _simulate_index(self, index):
        table_name = index.table()
        statement = (
            "select * from hypopg_create_index( "
            f"'create index on {table_name} "
            f"({index.joined_column_names()})')"
        )
        result = self.exec_fetch(statement)

        return result

    def _drop_simulated_index(self, oid):
        statement = f"select * from hypopg_drop_index({oid})"
        result = self.exec_fetch(statement)

        dbmind_assert(result[0] is True,
                      f"Could not drop simulated index with oid = {oid}.")

    def create_index(self, index):
        table_name = index.table()
        statement = (
            f"create index {index.index_idx()} "
            f"on {table_name} ({index.joined_column_names()})"
        )
        self.exec_only(statement)
        size = self.exec_fetch(
            f"select relpages from pg_class c " f"where c.relname = '{index.index_idx()}'"
        )
        size = size[0]
        index.estimated_size = size * 8 * 1024

    def create_indexes(self, indexes, mode="hypo"):
        for index in indexes:
            index_def = index.split("#")
            stmt = f"create index on {index_def[0]} ({index_def[1]})"
            if len(index_def) == 3:
                stmt += f" include ({index_def[2]})"
            if mode == "hypo":
                stmt = f"select * from hypopg_create_index('{stmt}')"
            self.exec_only(stmt)

    def get_ind_cost(self, query, indexes, mode="hypo"):
        self.create_indexes(indexes, mode)

        stmt = f"explain (format json) {query}"
        query_plan = self.exec_fetch(stmt)[0][0]["Plan"]

        total_cost = query_plan["Total Cost"]

        if mode == "hypo":
            self.drop_hypo_indexes()
        else:
            self.drop_indexes()

        return total_cost

    def get_ind_plan(self, query, indexes, mode="hypo"):
        self.create_indexes(indexes, mode)

        stmt = f"explain (format json) {query}"
        query_plan = self.exec_fetch(stmt)[0][0]["Plan"]

        if mode == "hypo":
            self.drop_hypo_indexes()
        else:
            self.drop_indexes()

        return query_plan

    def drop_hypo_indexes(self):
        logging.info("Dropping hypo indexes")
        stmt = "SELECT * FROM hypopg_reset();"
        self.exec_only(stmt)

    def drop_indexes(self):
        logging.info("Dropping indexes")
        stmt = "select indexname from pg_indexes where schemaname='public'"
        indexes = self.exec_fetch(stmt, one=False)
        for index in indexes:
            index_name = index[0]
            if "pkey" not in index_name:
                drop_stmt = "drop index {}".format(index_name)
                logging.debug("Dropping index {}".format(index_name))
                self.exec_only(drop_stmt)

    def exec_query(self, query, timeout=None, cost_evaluation=False):
        if not cost_evaluation:
            self._connection.commit()
        query_text = self._prepare_query(query)
        if timeout:
            set_timeout = f"set statement_timeout={timeout}"
            self.exec_only(set_timeout)
        statement = f"explain (analyze, buffers, format json) {query_text}"
        try:
            plan = self.exec_fetch(statement, one=True)[0][0]["Plan"]
            result = plan["Actual Total Time"], plan
        except Exception as e:
            logging.error(f"{query.nr}, {e}")
            self._connection.rollback()
            result = None, self._get_plan(query)

        self._cursor.execute("set statement_timeout = 0")
        self._cleanup_query(query)

        return result

    def _cleanup_query(self, query):
        for query_statement in query.text.split(";"):
            if "drop view" in query_statement:
                self.exec_only(query_statement)
                self.commit()

    def _get_cost(self, query):
        query_plan = self._get_plan(query)
        total_cost = query_plan["Total Cost"]

        return total_cost

    def _get_plan(self, query):
        # create view and return the next sql.
        query_text = self._prepare_query(query)
        statement = f"explain (format json) {query_text}"
        query_plan = self.exec_fetch(statement)[0][0]["Plan"]
        # drop view
        self._cleanup_query(query)

        return query_plan

    def get_tables(self):
        tables = list()
        sql = "select tablename from pg_tables where schemaname = 'public';"
        rows = self.exec_fetch(sql, one=False)
        for row in rows:
            tables.append(row[0])

        return tables

    def get_cols(self, table):
        cols = list()
        sql = f"select column_name from information_schema.columns where " \
              f"table_schema='public' and table_name='{table}'"

        rows = self.exec_fetch(sql, one=False)
        for row in rows:
            cols.append(row[0])

        return cols
