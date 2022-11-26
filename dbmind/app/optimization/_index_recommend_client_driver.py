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

from contextlib import contextmanager
from typing import List, Tuple, Any

from dbmind import global_vars
from dbmind.components.index_advisor.executors.common import BaseExecutor


class RpcExecutor(BaseExecutor):

    def execute_sqls(self, sqls) -> List[Tuple[Any]]:
        results = []
        sqls = ['set current_schema = %s' % self.get_schema()] + sqls
        sqls = [sql.strip().strip(';') for sql in sqls]
        if self.driver is not None:
            sql_results = self.driver.query(';'.join(sqls), return_tuples=True, fetch_all=True)
        else:
            sql_results = global_vars.agent_rpc_client.call('query_in_database',
                                                            ';'.join(sqls),
                                                            self.dbname,
                                                            return_tuples=True,
                                                            fetch_all=True)
        for sql, sql_res in zip(sqls[1:], sql_results[1:]):
            sql_type = sql.upper().strip().split()[0]
            if sql_type == 'EXPLAIN':
                if sql_res:
                    results.append((sql_type,))
                else:
                    results.append(('ERROR',))
            if sql_res:
                results.extend(sql_res)
        return results

    @contextmanager
    def session(self):
        yield
