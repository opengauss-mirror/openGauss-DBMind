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
import subprocess
import sys
from contextlib import contextmanager
from typing import List, Tuple
import re
import tempfile

from .common import BaseExecutor, REMOVE_ANSI_QUOTES_SQL

BLANK = ' '


def to_tuples(text):
    """Parse execution result by using gsql
     and convert to tuples."""
    lines = text.splitlines()
    separator_location = -1
    for i, line in enumerate(lines):
        # Find separator line such as '-----+-----+------'.
        if re.match(r'^\s*?[-|+]+\s*$', line):
            separator_location = i
            break

    if separator_location < 0:
        return []

    separator = lines[separator_location]
    left = 0
    right = len(separator)
    locations = list()
    while left < right:
        try:
            location = separator.index('+', left, right)
        except ValueError:
            break
        locations.append(location)
        left = location + 1
    # Record each value start location and end location.
    pairs = list(zip([0] + locations, locations + [right]))
    tuples = []
    row = []
    wrap_flag = False
    # Continue to parse each line.
    for line in lines[separator_location + 1:]:
        # Prevent from parsing bottom lines.
        if len(line.strip()) == 0 or re.match(r'\(\d+ rows?\)', line):
            continue
        # Parse a record to tuple.
        if wrap_flag:
            row[-1] += line[pairs[-1][0] + 1: pairs[-1][1]].strip()
        else:
            for start, end in pairs:
                # Increase 1 to start index to go over vertical bar (|).
                row.append(line[start + 1: end].strip())

        if len(line) == right and re.match(r'.*\s*\+$', line):
            wrap_flag = True
            row[-1] = row[-1].strip('+').strip(BLANK) + BLANK
        else:
            tuples.append(tuple(row))
            row = []
            wrap_flag = False
    return tuples


class GsqlExecutor(BaseExecutor):
    def __init__(self, *args):
        super(GsqlExecutor, self).__init__(*args)
        self.base_cmd = ''
        with self.session():
            self.__check_connect()

    def __init_conn_handle(self):
        self.base_cmd = 'gsql -p ' + str(self.port) + ' -d ' + self.dbname
        if self.host:
            self.base_cmd += ' -h ' + self.host
        if self.user:
            self.base_cmd += ' -U ' + self.user
        if self.password:
            self.base_cmd += ' -W ' + shlex.quote(self.password)

    def __check_connect(self):
        cmd = self.base_cmd + ' -c \"'
        cmd += 'select 1;\"'
        #####################################################################
        #                  !! IMPORTANT SECURITY NOTE !!                    #
        # About using subprocess.Popen(cmd, shell=True)                     #
        # 1.All user input is captured as individual options.               #
        # 2.User input is not passed directly to gsql in the subprocess.    #
        # 3.All options have type validation.                               #
        # 4.The command to be executed by the subprocess is passed to the   #
        #   shell as a string.                                              #
        #####################################################################
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (stdout, stderr) = proc.communicate()
        stdout, stderr = stdout.decode(errors='ignore'), stderr.decode(errors='ignore')
        if 'gsql: FATAL:' in stderr or 'failed to connect' in stderr or \
                'gsql: ERROR' in stderr or 'login denied' in stderr:
            raise ConnectionError("An error occurred while connecting to the database.\n" +
                                  "Details: " + stderr)
        return stdout

    @staticmethod
    def __to_tuples(sql_result: str) -> List[Tuple[str]]:
        is_tuple = False
        results = []
        tmp_tuple_lines = []
        for line in sql_result.strip().split('\n'):
            if re.match(r'^\s*?[-|+]+\s*$', line):
                is_tuple = True
            elif re.match(r'\(\d+ rows?\)', line) and is_tuple:
                is_tuple = False
                results.extend(to_tuples('\n'.join(tmp_tuple_lines)))
                tmp_tuple_lines = []
            if is_tuple:
                tmp_tuple_lines.append(line)
            else:
                results.append((line,))

        return results

    def execute_sqls(self, sqls):
        cmd = self.base_cmd + ' -c \"SHOW sql_compatibility;\"'
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (stdout, stderr) = proc.communicate()
        stdout, stderr = stdout.decode(errors='ignore'), stderr.decode(errors='ignore')
        for line in stdout:
            if 'M' == line.strip():
                sqls = [REMOVE_ANSI_QUOTES_SQL] + sqls
        sqls = ['set current_schema = %s' % self.get_schema()] + sqls

        file1 = tempfile.NamedTemporaryFile(mode='w+', delete=True)
        try:
            for sql in sqls:
                if not sql.strip().endswith(';'):
                    sql += ';'
                file1.file.write(sql + '\n')
            file1.file.flush()
            cmd = self.base_cmd + ' -f ' + file1.name
            try:
                ret = subprocess.check_output(
                    shlex.split(cmd), stderr=subprocess.STDOUT)
                return self.__to_tuples(ret.decode(errors='ignore'))
            except subprocess.CalledProcessError as e:
                print(e.output.decode(errors='ignore'), file=sys.stderr)
        finally:
            file1.close()

    @contextmanager
    def session(self):
        self.__init_conn_handle()
        yield
