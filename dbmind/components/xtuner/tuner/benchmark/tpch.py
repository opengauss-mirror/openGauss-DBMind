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
import sys
import time

# WARN: You need to import data into the database and SQL statements in the following path will be executed.
# The program automatically collects the total execution duration of these SQL statements.
path = '/path/to/tpch/queries'  # modify this path which contains benchmark SQL files.
cmd = "find %s -type f -name '*.sql' -exec gsql -U {user} -W {password} -d {db} -p {port} -f {} > /dev/null \\;"


def run(remote_server, local_host):
    time_start = time.monotonic()
    # Check whether the path is valid.
    stdout, stderr = remote_server.exec_command_sync('ls %s' % shlex.quote(path))
    if len(stderr) > 0:
        print('You should correct the parameter `benchmark_path` that the path contains several executable SQL files '
              'in the configuration file.')
        exit(1)

    if not cmd.startswith("find %s ") or not cmd.endswith("> /dev/null \\;"):
        print('The input cmd is not allowed.')
        exit(1)
    params = cmd.split('find %s ')[1].rsplit("> /dev/null \\;")[0].split('-')
    param_dict = dict()
    try:
        for param in params:
            if param == '':
                continue
            param_key, param_val = param.split()
            param_dict[param_key] = param_val.strip()
    except Exception:
        print('The input cmd is not allowed.')
        exit(1)
    if set(param_dict.keys()) != {'type', 'name', 'exec', 'U', 'W', 'd', 'p', 'f'}:
        print('The input cmd is not allowed.')
        exit(1)
    cmd_processed = f"find {shlex.quote(path)} -type f -name '*.sql' -exec gsql -U {shlex.quote(param_dict.get('U'))} " \
                    f"-W {shlex.quote(param_dict.get('W'))} -d {shlex.quote(param_dict.get('d'))} " \
                    f"-p {shlex.quote(param_dict.get('p'))} -f {{}} > /dev/null \\;"

    stdout, stderr = remote_server.exec_command_sync(cmd_processed)
    if len(stderr) > 0:
        print(stderr, file=sys.stderr)
    cost = time.monotonic() - time_start
    return - cost
