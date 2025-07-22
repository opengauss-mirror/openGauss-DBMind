# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.

import time
import logging

import traceback


class DatabaseConnector:
    def __init__(self, args, autocommit=False):
        self.config = args
        self.db_name = args.database
        self.autocommit = autocommit

        self.simulated_indexes = 0
        self.cost_estimations = 0
        self.cost_estimation_duration = 0
        self.index_simulation_duration = 0

    def exec_only(self, statement):
        self._cursor.execute(statement)

    def exec_fetch(self, statement, one=True):
        self._cursor.execute(statement)
        if one:
            return self._cursor.fetchone()
        return self._cursor.fetchall()

    def enable_simulation(self):
        raise NotImplementedError

    def commit(self):
        self._connection.commit()

    def close(self):
        self._connection.close()
        logging.debug("Database connector closed: {}".format(self.db_name))

    def rollback(self):
        self._connection.rollback()

    def drop_index(self, index):
        statement = f"drop index {index.index_idx()}"
        self.exec_only(statement)

    def _prepare_query(self, query):
        if "create view" in query.text:
            for query_statement in query.text.split(";"):
                if "create view" in query_statement:
                    try:
                        self.exec_only(query_statement)
                    except Exception as e:
                        logging.error(e)
                        logging.error(traceback.format_exc())
                elif "select" in query_statement or "SELECT" in query_statement:
                    return query_statement
        else:
            return query.text

    def simulate_index(self, index):
        self.simulated_indexes += 1

        start_time = time.time()
        result = self._simulate_index(index)
        end_time = time.time()
        self.index_simulation_duration += end_time - start_time

        return result

    def drop_simulated_index(self, identifier):
        start_time = time.time()
        self._drop_simulated_index(identifier)
        end_time = time.time()
        self.index_simulation_duration += end_time - start_time

    def get_cost(self, query):
        self.cost_estimations += 1

        start_time = time.time()
        cost = self._get_cost(query)
        end_time = time.time()
        self.cost_estimation_duration += end_time - start_time

        return cost

    def get_plan(self, query):
        self.cost_estimations += 1

        start_time = time.time()
        plan = self._get_plan(query)
        end_time = time.time()
        self.cost_estimation_duration += end_time - start_time

        return plan

    def table_exists(self, table_name):
        raise NotImplementedError

    def database_exists(self, database_name):
        raise NotImplementedError

    def drop_database(self, database_name):
        raise NotImplementedError

    def create_statistics(self):
        raise NotImplementedError

    def set_random_seed(self, value):
        raise NotImplementedError

    def _get_cost(self, query):
        raise NotImplementedError

    def _get_plan(self, query):
        raise NotImplementedError

    def _simulate_index(self, index):
        raise NotImplementedError

    def _drop_simulated_index(self, identifier):
        raise NotImplementedError
