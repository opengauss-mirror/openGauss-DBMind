#!/usr/bin/env python3
# coding=utf-8
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
import subprocess
import logging
import shlex

try:
    from dbmind.components.dkr.utils import DEPLOY_MODE_SEARCH
except ImportError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))))
    from dbmind.components.dkr.utils import DEPLOY_MODE_SEARCH


BLANK = ' '


class ExecuteFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_executor(name, args):
        if name == 'gsql':
            return GSqlExecutor(args)
        elif name == 'driver':
            return DriverExecutor(args)


class Executor:
    def __init__(self, args):
        self.dbname = args.database
        self.user = args.user
        self.password = args.password
        self.host = args.host
        self.port = args.port
        self.schema = args.schema
        self.system_tables = []

    def init_conn_handle(self):
        pass

    def execute(self):
        pass

    def close_conn(self):
        pass


class DriverExecutor(Executor):
    def __init__(self, args):
        super(DriverExecutor, self).__init__(args)
        self.conn = None
        self.cur = None
        self.init_conn_handle()

    def init_conn_handle(self):
        import psycopg2
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     user=self.user,
                                     password=self.password,
                                     host=self.host,
                                     port=self.port)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def _common_execute(self, stmt):
        if stmt and not stmt[0] == DEPLOY_MODE_SEARCH:
            logging.info("Executing SQL statement: %s", ''.join(stmt))
        result_list = []
        for sql in stmt:
            try:
                self.cur.execute(sql)
                result = self.cur.fetchall()
            except Exception as e:
                if 'permission denied for schema dbe_perf' in str(e).lower():
                    raise
                result = None
            if result is not None:
                result_list.append(result)
        return result_list

    def execute(self, stmt: list, sql_count=1):
        result_list = self._common_execute(stmt)
        return _parse_stdout_driver(result_list, sql_count)

    def explain(self, stmt: list):
        # return sql explain information.
        result_list = self._common_execute(stmt)
        return '\n'.join([record[0] for record in result_list[0]]) if result_list else ''

    def close_conn(self):
        if self.conn and self.cur:
            self.cur.close()
            self.conn.close()


def _parse_stdout_driver(stdout, sql_count):
    if sql_count != 1:
        # return table information.
        if len(stdout) != sql_count:
            return []
        return stdout
    # return wdr information.
    res = []
    for record in stdout:
        res.extend(record)
    return res


class GSqlExecutor(Executor):
    def __init__(self, args):
        super(GSqlExecutor, self).__init__(args)
        self.gsql_prefix = ''
        self.init_conn_handle()

    def init_conn_handle(self):
        self.gsql_prefix = 'gsql -d %s -p %d' % (self.dbname, self.port)
        if self.host:
            self.gsql_prefix += ' -h ' + self.host
        if self.user:
            self.gsql_prefix += ' -U ' + self.user
        if self.password:
            self.gsql_prefix += ' -W ' + shlex.quote(self.password)

    def _common_execute(self, stmt):
        cmd = self.gsql_prefix + ' -c "%s"' % ''.join(stmt)
        # splice the command line arguments that need to be executed for gsql.
        if stmt and not stmt[0] == DEPLOY_MODE_SEARCH:
            logging.info("Executing SQL statement: %s", ''.join(stmt))
        proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = tuple(map(lambda stream: stream.decode(), proc.communicate()))
        if 'gsql: FATAL' in stderr or 'failed to connect' in stderr or \
                'permission' in stderr.lower() or \
                'gsql: ERROR' in stderr or 'login denied' in stderr:
            raise ConnectionError('An error occurred while connecting to the database.\n'
                                  'Details: ' + stderr)
        return stdout

    def execute(self, stmt: list, sql_count=1):
        result_str = self._common_execute(stmt)
        return _parse_stdout_gsql(result_str, sql_count)

    def explain(self, stmt: list):
        result_str = self._common_execute(stmt)
        return result_str


def _parse_stdout_gsql(result_str, sql_count):
    # return table information.
    if sql_count != 1:
        res = []
        lines = result_str.splitlines()
        for i, line in enumerate(lines):
            if re.match(r'^\s*?[-|+]+\s*$', line):
                res.append(_to_tuples("\n".join(lines[i:])))
        # determine whether the number of returned results matches the number of
        # information to be queried in the INIT TABLE.
        if len(res) != sql_count:
            res = []
        return res
    # return wdr information.
    return _to_tuples(result_str)


def _to_tuples(text):
    lines = text.splitlines()
    separator_location = -1
    for i, line in enumerate(lines):
        # find separator line such as '-----+-----+------'.
        if re.match(r'^\s*?[-|+]+\s*$', line):
            separator_location = i
            break
    if separator_location < 0:
        return []
    separator = lines[separator_location]
    left = 0
    right = len(separator)
    locations = []
    while left < right:
        try:
            location = separator.index('+', left, right)
        except ValueError:
            break
        locations.append(location)
        left = location + 1
    # record each value start location and end location.
    pairs = list(zip([0] + locations, locations + [right]))
    tuples = []
    row = []
    wrap_flag = False
    # continue to parse each line.
    for line in lines[separator_location + 1:]:
        # Prevent from parsing bottom lines.
        if len(line.strip()) == 0:
            continue
        if re.match(r'\(\d+ rows?\)', line):
            break
        # parse a record to tuple.
        if wrap_flag:
            row[-1] = '{}{}'.format(row[-1], line[pairs[-1][0] + 1:pairs[-1][1]].strip())
        else:
            for start, end in pairs:
                # increase 1 to start index to go over vertical bar (|).
                row.append(line[start + 1:end].strip())
        if len(line) == right and re.match(r'.*\s*\+$', line):
            wrap_flag = True
            row[-1] = '{}{}'.format(row[-1].strip('+').strip(BLANK), BLANK)
        else:
            tuples.append(tuple(row))
            row = []
            wrap_flag = False
    return tuples

