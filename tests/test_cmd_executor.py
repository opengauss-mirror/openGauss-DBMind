# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
import os

from dbmind.common import cmd_executor
from dbmind.common.cmd_executor import bytes2text, ExecutorFactory
from dbmind.common.platform import LINUX


def test_local_exec():
    if not LINUX:
        return
    local_exec = cmd_executor.LocalExec()
    assert int(
        local_exec.exec_command_sync("cat /proc/cpuinfo | grep processor | wc -l")[0]
    ) == os.cpu_count()
    assert local_exec.exec_command_sync(
        "cat /proc/self/cmdline | xargs -0"
    )[0] == 'cat /proc/self/cmdline'
    assert local_exec.exec_command_sync('echo $PATH')[0] == os.environ.get('PATH')
    assert local_exec.exec_command_sync('echo a; echo b && echo c | grep c')[0] == 'c'
    assert local_exec.exec_command_sync('echo c; echo c && echo c | grep c')[0] == 'c'
    assert local_exec.exec_command_sync('echo c; echo c && echo c | grep b')[0] == ''
    assert local_exec.exec_command_sync('cd /tmp; pwd')[0] == '/tmp'
    assert local_exec.exec_command_sync('cd /tmp; pwd; cd $HOME; pwd')[0] == os.environ.get('HOME')
    assert local_exec.exec_command_sync('export a=bcd && echo $a')[0] == 'bcd'
    assert 'bash' in local_exec.exec_command_sync('bash --version')[0]
    assert 'bash' not in local_exec.exec_command_sync("bash --version | grep -v 'bash'")[0]


def test_to_cmds():
    cases = (
        (
            'echo a && echo b ; echo c',
            ([['echo', 'a'], ['echo', 'b'], ['echo', 'c']], [False, False, False])
        ),
        (
            'echo a && echo b | echo c',
            ([['echo', 'a'], ['echo', 'b'], ['echo', 'c']], [False, False, True])
        ),
        (
            'echo a',
            ([['echo', 'a']], [False])
        ),
        (
            'echo a;',
            ([['echo', 'a;']], [False])
        ),
        (
            'echo a \\;',
            ([['echo', 'a', ';']], [False])
        ),
        (
            'echo a \\; echo b',
            ([['echo', 'a', ';', 'echo', 'b']], [False])
        ),
        (
            'echo a ; echo b',
            ([['echo', 'a'], ['echo', 'b']], [False, False])
        ),
        (
            'echo a \\ echo b',
            ([['echo', 'a', ' echo', 'b']], [False])
        ),
        (
            'find . -name "*" -exec ls -l {} \\;',
            ([['find', '.', '-name', '*', '-exec', 'ls', '-l', '{}', ';']], [False])
        ),
        (
            'find . -name "*" -exec ls -l {} \\; && echo "hello world"',
            ([['find', '.', '-name', '*', '-exec', 'ls', '-l', '{}', ';'], ['echo', 'hello world']], [False, False])
        )
    )

    for cmdline, expected in cases:
        actual = cmd_executor.to_cmds(cmdline)
        assert len(actual[0]) == len(actual[1])
        assert actual == expected


def test_bytes2text():
    assert (bytes2text(['a', 'b', 'c']) == 'abc')
    assert (bytes2text([b'ab', b'cd', b'e']) == 'abcde')
    assert (bytes2text(b'abc') == 'abc')


def test_executor_factory():
    if not LINUX:
        return
    local_ssh = ExecutorFactory() \
        .set_host('127.0.0.1') \
        .get_executor()
    assert local_ssh.exec_command_sync('echo a; echo b && echo c | grep c')[0] == 'c'
