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

import re
import shlex
import sys
import time

import dbmind.common.parser.others
import dbmind.common.parser.sql_parsing
from .. import utils
from ..exceptions import ExecutionError

path = ''
# Measure current total committed transactions that do not include xact_rollback.
cmd = "gsql -U {user} -W {password} -d postgres -p {port} -c " \
      "\"SELECT pg_catalog.sum(xact_commit) FROM pg_catalog.pg_stat_database where datname = '{db}';\""


# This script captures the performance indicators in the user's periodic execution task, and measures the quality
# of the tuning results by measuring the range of changes in the indicators.
def run(remote_server, local_host) -> float:
    wait_seconds = utils.config['benchmark_period']
    if not wait_seconds:
        print("Not configured the parameter 'benchmark_period' in the configuration file.",
              file=sys.stderr)
        exit(1)

    if not cmd.startswith("gsql "):
        print('The input cmd is not allowed.')
        exit(1)
    params = cmd.split('gsql ')[1].split('-')
    param_dict = dict()
    try:
        for param in params:
            if param == '':
                continue
            param_key, param_val = param.split(" ", 1)
            param_dict[param_key] = param_val.strip()
    except Exception:
        print('The input cmd is not allowed.')
        exit(1)
    if set(param_dict.keys()) != {'c', 'U', 'W', 'd', 'p'}:
        print('The input cmd is not allowed.')
        exit(1)
    sql = param_dict.get('c')
    if not sql.startswith("\"SELECT pg_catalog.sum(xact_commit) FROM pg_catalog.pg_stat_database where datname = "):
        print('The input cmd is not allowed.')
        exit(1)
    param_dict['db'] = sql[sql.find('\'') + 1: sql.rfind('\'')]
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_$#]{1,65}$", param_dict['db']):
        print('The input cmd is not allowed.')
        exit(1)
    cmd_processed = f"gsql -U {shlex.quote(param_dict.get('U'))} -W {shlex.quote(param_dict.get('W'))} -d postgres " \
                    f"-p {shlex.quote(param_dict.get('p'))} -c \"SELECT pg_catalog.sum(xact_commit) FROM " \
                    f"pg_catalog.pg_stat_database where datname = '{param_dict.get('db')}';\""

    stdout, stderr = remote_server.exec_command_sync(cmd_processed)
    if len(stderr) > 0:
        raise ExecutionError(stderr)
    prev_txn = int(dbmind.common.parser.others.to_tuples(stdout)[0][0])

    time.sleep(wait_seconds)
    stdout, stderr = remote_server.exec_command_sync(cmd_processed)
    if len(stderr) > 0:
        raise ExecutionError(stderr)
    current_txn = int(dbmind.common.parser.others.to_tuples(stdout)[0][0])

    # Return TPS in this period.
    return (current_txn - prev_txn) / wait_seconds
