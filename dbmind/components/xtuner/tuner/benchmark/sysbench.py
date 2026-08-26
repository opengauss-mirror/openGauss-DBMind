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

import shlex

from ..exceptions import ExecutionError

# WARN: You should first install the sysbench test tool on the system,
# then fill in the following path and cmd.

path = "/path/to/sysbench_install/share/sysbench/tests/include/oltp_legacy/oltp.lua"
cmd = "sysbench --test=%s --db-driver=pgsql " \
      "--pgsql-db={db} --pgsql-user={user} --pgsql-password={password} --pgsql-port={port} --pgsql-host=127.0.0.1 " \
      "--oltp-tables-count=20 --oltp-table-size=1000 --max-time=30 --max-requests=0" \
      " --num-threads=20 --report-interval=3 --forced-shutdown=1 run" % path


def run(remote_server, local_host):
    if not cmd.startswith("sysbench ") or not cmd.endswith("run"):
        print('The input cmd is not allowed.')
        exit(1)
    params = cmd.split('sysbench ')[1].rsplit("run")[0].split('--')
    param_dict = dict()
    try:
        for param in params:
            if param == '':
                continue
            param_key, param_val = param.split('=')
            param_dict[param_key] = param_val.strip()
    except Exception:
        print('The input cmd is not allowed.')
        exit(1)
    if set(param_dict.keys()) != {'max-time', 'report-interval', 'max-requests', 'pgsql-user', 'oltp-tables-count',
                                  'db-driver', 'pgsql-port', 'pgsql-db', 'pgsql-host', 'test', 'pgsql-password',
                                  'num-threads', 'oltp-table-size', 'forced-shutdown'}:
        print('The input cmd is not allowed.')
        exit(1)
    cmd_processed = f"sysbench --test={shlex.quote(param_dict.get('test'))} --db-driver=pgsql " \
                    f"--pgsql-db={shlex.quote(param_dict.get('pgsql-db'))} " \
                    f"--pgsql-user={shlex.quote(param_dict.get('pgsql-user'))} " \
                    f"--pgsql-password={shlex.quote(param_dict.get('pgsql-password'))} " \
                    f"--pgsql-port={shlex.quote(param_dict.get('pgsql-port'))} " \
                    f"--pgsql-host={shlex.quote(param_dict.get('pgsql-host'))} --oltp-tables-count=20 " \
                    f"--oltp-table-size=1000 --max-time=30 --max-requests=0 --num-threads=20 " \
                    f"--report-interval=3 --forced-shutdown=1 run"

    stdout, stderr = remote_server.exec_command_sync(cmd_processed)
    if len(stderr) > 0:
        raise ExecutionError(stderr)
    try:
        return float(stdout.split('queries:')[1].split('(')[1].split('per')[0].strip())
    except Exception as e:
        raise ExecutionError('Failed to parse sysbench result, because %s.' % e)
