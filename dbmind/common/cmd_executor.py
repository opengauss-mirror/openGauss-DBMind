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

import getpass
import logging
import shlex
import socket
import subprocess
import threading
import time
import os
import sys

import paramiko

from dbmind.common.utils.checking import check_ssh_version

n_stdin = 0
n_stdout = 1
n_stderr = 2


class shlex_py38(shlex.shlex):
    """
    Use the read_token function of shlex in py38 to replace that in py37 to
    fix the incompatibility of punctuation_chars and whitespace_split in py37.
    """

    def read_token(self):
        quoted = False
        escapedstate = ' '
        while True:
            if self.punctuation_chars and self._pushback_chars:
                nextchar = self._pushback_chars.pop()
            else:
                nextchar = self.instream.read(1)
            if nextchar == '\n':
                self.lineno += 1
            if self.state is None:
                self.token = ''  # past end of file
                break
            elif self.state == ' ':
                if not nextchar:
                    self.state = None  # end of file
                    break
                elif nextchar in self.whitespace:
                    if self.token or (self.posix and quoted):
                        break  # emit current token
                    else:
                        continue
                elif nextchar in self.commenters:
                    self.instream.readline()
                    self.lineno += 1
                elif self.posix and nextchar in self.escape:
                    escapedstate = 'a'
                    self.state = nextchar
                elif nextchar in self.wordchars:
                    self.token = nextchar
                    self.state = 'a'
                elif nextchar in self.punctuation_chars:
                    self.token = nextchar
                    self.state = 'c'
                elif nextchar in self.quotes:
                    if not self.posix:
                        self.token = nextchar
                    self.state = nextchar
                elif self.whitespace_split:
                    self.token = nextchar
                    self.state = 'a'
                else:
                    self.token = nextchar
                    if self.token or (self.posix and quoted):
                        break  # emit current token
                    else:
                        continue
            elif self.state in self.quotes:
                quoted = True
                if not nextchar:  # end of file
                    raise ValueError("No closing quotation")
                if nextchar == self.state:
                    if not self.posix:
                        self.token += nextchar
                        self.state = ' '
                        break
                    else:
                        self.state = 'a'
                elif (self.posix and nextchar in self.escape and self.state
                      in self.escapedquotes):
                    escapedstate = self.state
                    self.state = nextchar
                else:
                    self.token += nextchar
            elif self.state in self.escape:
                if not nextchar:  # end of file
                    raise ValueError("No escaped character")
                # In posix shells, only the quote itself or the escape
                # character may be escaped within quotes.
                if (escapedstate in self.quotes and
                        nextchar != self.state and nextchar != escapedstate):
                    self.token += self.state
                self.token += nextchar
                self.state = escapedstate
            elif self.state in ('a', 'c'):
                if not nextchar:
                    self.state = None  # end of file
                    break
                elif nextchar in self.whitespace:
                    self.state = ' '
                    if self.token or (self.posix and quoted):
                        break  # emit current token
                    else:
                        continue
                elif nextchar in self.commenters:
                    self.instream.readline()
                    self.lineno += 1
                    if self.posix:
                        self.state = ' '
                        if self.token or (self.posix and quoted):
                            break  # emit current token
                        else:
                            continue
                elif self.state == 'c':
                    if nextchar in self.punctuation_chars:
                        self.token += nextchar
                    else:
                        if nextchar not in self.whitespace:
                            self._pushback_chars.append(nextchar)
                        self.state = ' '
                        break
                elif self.posix and nextchar in self.quotes:
                    self.state = nextchar
                elif self.posix and nextchar in self.escape:
                    escapedstate = 'a'
                    self.state = nextchar
                elif (nextchar in self.wordchars or nextchar in self.quotes
                      or (self.whitespace_split and
                          nextchar not in self.punctuation_chars)):
                    self.token += nextchar
                else:
                    if self.punctuation_chars:
                        self._pushback_chars.append(nextchar)
                    else:
                        self.pushback.appendleft(nextchar)
                    self.state = ' '
                    if self.token or (self.posix and quoted):
                        break  # emit current token
                    else:
                        continue
        result = self.token
        self.token = ''
        if self.posix and not quoted and result == '':
            result = None
        return result


def bytes2text(bs):
    """
    Converts bytes (or array-like of bytes) to text.

    :param bs: Bytes or array-like of bytes.
    :return: Converted text.
    """
    if type(bs) in (list, tuple) and len(bs) > 0:
        if isinstance(bs[0], bytes):
            return b''.join(bs).decode(errors='ignore').strip()
        if isinstance(bs[0], str):
            return ''.join(bs).strip()
        else:
            raise TypeError
    elif isinstance(bs, bytes):
        return bs.decode(errors='ignore').strip()
    else:
        return ''


class ExecutorFactory:
    def __init__(self):
        """
        A factory class is used to produce executors.
        Here are two types of executors.
        One is implemented through Popen (generally used for local command execution)
        and the other is implemented through SSH (generally used for remote command execution).
        """
        self.host = None
        self.pwd = None
        self.port = 22
        self.me = getpass.getuser()  # Current executing user.
        self.user = self.me  # Default user is current user.

    def set_host(self, host):
        self.host = host
        return self

    def set_user(self, user):
        self.user = user
        return self

    def set_pwd(self, pwd):
        self.pwd = pwd
        return self

    def set_port(self, port):
        self.port = port
        return self

    def get_executor(self):
        if self._is_remote() or self.user != self.me:
            if None in (self.user, self.pwd, self.port, self.host):
                raise AssertionError

            return SSH(host=self.host,
                       user=self.user,
                       pwd=self.pwd,
                       port=self.port)
        else:
            return LocalExec()

    def _is_remote(self):
        if not self.host:
            return False  # Not setting host is treated as local.

        hostname = socket.gethostname()
        _, _, ip_address_list = socket.gethostbyname_ex(hostname)
        if self.host in ('127.0.0.1', 'localhost') or self.host in ip_address_list:
            return False
        else:
            return True


class Executor:
    """Executor is an abstract class."""

    class Wrapper:
        """inner abstract class for asynchronous execution."""

        def __init__(self, stream):
            self.stream = stream

        def read(self):
            pass

    def exec_command_sync(self, command, *args, **kwargs):
        pass


class SSH(Executor):
    def __init__(self, host, user, pwd, port=22, max_retry_times=5):
        """
        Use the paramiko library to establish an SSH connection with the remote server.
        You can run one or more commands.
        In addition, the `gsql` password information is not exposed.

        :param host: String type.
        :param user: String type.
        :param pwd: String type.
        :param port: Int type.
        :param max_retry_times: Int type. Maximum number of retries if the connection fails.
        """
        check_ssh_version()
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port
        self.max_retry_times = max_retry_times
        self.retry_cnt = 0
        self.client = SSH._connect_ssh(host, user, pwd, port)

        # Init a thread local variable to save the exit status.
        self._exit_status = threading.local()
        self._exit_status.value = 0

    @staticmethod
    def _connect_ssh(host, user, pwd, port):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, port, user, pwd)
        return client

    @property
    def exit_status(self):
        return self._exit_status.value

    def _exec_command(self, command, **kwargs):
        if self.client is None:
            self.client = SSH._connect_ssh(self.host, self.user,
                                           self.pwd, self.port)
        try:
            if type(command) in (list, tuple):
                chan = self.client.get_transport().open_session()
                chan.invoke_shell()

                buff_size = 32768
                timeout = kwargs.get('timeout', None)
                stdout = list()
                stderr = list()
                cmds = list(command)
                # In interactive mode,
                # we cannot determine whether the process exits. We need to exit the shell manually.
                cmds.append('exit $?')
                for line in cmds:
                    chan.send(line + '\n')
                    while not chan.send_ready():  # Wait until the sending is complete.
                        time.sleep(0.1)

                # Wait until all commands are executed.
                start_time = time.monotonic()
                while not chan.exit_status_ready():
                    if chan.recv_ready():
                        stdout.append(chan.recv(buff_size))
                    if chan.recv_stderr_ready():
                        stderr.append(chan.recv_stderr(buff_size))
                    if timeout and (time.monotonic() - start_time) > timeout:
                        break
                    time.sleep(0.1)

                chan.close()
                self._exit_status.value = chan.recv_exit_status()
                result_tup = (bytes2text(stdout), bytes2text(stderr))
            else:
                blocking_fd = kwargs.pop('fd', n_stdout)

                # Some environments miss the path of /path/to/bin and /path/to/sbin,
                # so we have to attach a common path to the environment PATH.
                bin_paths = '/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin'
                path_prefix = 'PATH=$PATH:%s && ' % bin_paths
                command = path_prefix + command

                chan = self.client.exec_command(command=command, **kwargs)
                while not chan[blocking_fd].channel.exit_status_ready():  # Blocking here.
                    time.sleep(0.1)

                self._exit_status.value = chan[blocking_fd].channel.recv_exit_status()
                result_tup = (bytes2text(chan[n_stdout].read()), bytes2text(chan[n_stderr].read()))

            self.retry_cnt = 0
            return result_tup
        except paramiko.SSHException as e:
            # reconnect
            self.client.close()
            self.client = SSH._connect_ssh(self.host, self.user,
                                           self.pwd, self.port)

            # Retry until the upper limit is reached.
            if self.retry_cnt >= self.max_retry_times:
                raise ConnectionError("Can not connect to remote host.")

            logging.warning("SSH: %s, so try to reconnect.", e)
            self.retry_cnt += 1
            return self._exec_command(command)

    def exec_command_sync(self, command, *args, **kwargs):
        """
        You can run one or more commands.

        :param command: Type: tuple, list or string.
        :param kwargs: blocking_fd means blocking and waiting for which standard streams.
        :return: Execution result.
        """
        blocking_fd = kwargs.pop('blocking_fd', n_stdout)
        if not isinstance(blocking_fd, int) or blocking_fd > n_stderr or blocking_fd < n_stdin:
            raise ValueError

        return self._exec_command(command, fd=blocking_fd, **kwargs)

    def close(self):
        if self.client:
            self.client.close()
            self.client = None


class LocalExec(Executor):
    _exit_status = None

    def __init__(self):
        """
        Use the subprocess. Popen library to open a pipe.
        You can run one or more commands.
        In addition, the `gsql` password information is not exposed.
        """
        # Init a thread local variable to save the exit status.
        LocalExec._exit_status = threading.local()
        LocalExec._exit_status.value = 0

    @staticmethod
    def exec_command_sync(command, *args, **kwargs):
        if type(command) in (list, tuple):
            stdout = list()
            stderr = list()
            cwd = None
            for line in command:
                # Have to use the `cwd` argument.
                # Otherwise, we can not change the current directory.
                if line.strip().startswith('cd '):
                    cwd = line.strip()[len('cd'):]
                    continue

                proc = subprocess.Popen(shlex.split(line),
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        shell=False,
                                        cwd=cwd)
                outs, errs = proc.communicate(timeout=kwargs.get('timeout', None))
                LocalExec._exit_status.value = proc.returncode  # Get the last one.
                if outs:
                    stdout.append(outs)
                if errs:
                    stderr.append(errs)
            return [bytes2text(stdout), bytes2text(stderr)]
        else:
            returncode, outs, errs = multiple_cmd_exec(command, timeout=kwargs.get('timeout', None))
            LocalExec._exit_status.value = returncode
            return [bytes2text(stream) for stream in [outs, errs]]

    @property
    def exit_status(self):
        return LocalExec._exit_status.value


def dequote(text):
    """Strip quotation marks."""
    return shlex.split(text)[0]


def to_cmds(cmdline):
    separators = {'|', '||', '&&', ';'}
    escaped = '\\'

    def get_separators(s):
        if sys.version_info < (3, 8):
            lex = shlex_py38(s, punctuation_chars=True)
        else:
            lex = shlex.shlex(s, punctuation_chars=True)
        lex.whitespace_split = True
        tokens = list(lex)
        real_tokens = []
        separator_indexes = []
        escape_count = 0
        for token in tokens:
            if token == escaped:
                escape_count += 1
                continue
            real_tokens.append(token)
            if token in separators:
                if escape_count == 0 and len(real_tokens) > 0:
                    separator_indexes.append(len(real_tokens) - 1)
            if escape_count > 0:
                escape_count -= 1
        # append left escaped characters
        for _ in range(escape_count):
            real_tokens.append(escaped)
        while separator_indexes and separator_indexes[-1] == len(real_tokens) - 1:
            separator_indexes.pop()
            real_tokens.pop()
        if len(separator_indexes) == 0:
            real_tokens = shlex.split(s)
        return real_tokens, separator_indexes

    cmd_words, seps = get_separators(cmdline)
    if len(seps) == 0:
        return [cmd_words], [False]
    cmds = []
    require_stdin = [False]  # disable to use stdin for the first command
    cmd_start = 0
    while seps:
        sep_index = seps.pop(0)
        cmds.append(list(map(dequote, cmd_words[cmd_start:sep_index])))
        require_stdin.append(cmd_words[sep_index] == '|')
        cmd_start = sep_index + 1
    last_one = list(map(dequote, cmd_words[cmd_start:]))
    if len(last_one) > 0:
        cmds.append(last_one)
    # Frankly, it is redundant to truncate require_stdin. Coding here is to
    # avoid untested boundary cases.
    return cmds, require_stdin[:len(cmds)]


def multiple_cmd_exec(cmdline, **communicate_kwargs):
    """This function only returns the execution result of the last
    command. And only support the basic scenarios.

    Notice: this function is only a simple wrapper of popen,
     which doesn't support complicated scenarios. e.g.,
    `echo $?`, `dirname $(pwd)`, `PATH=$PATH; echo $PATH`, `echo abc >&2`
    """
    cmds, require_stdin = to_cmds(cmdline)
    if not communicate_kwargs.get('input'):
        stdin = None
    else:
        stdin = subprocess.PIPE
    process_list = []
    # Support some basic scenarios, for example,
    # cd and export commands.
    cwd = os.getcwd()
    env = os.environ.copy()
    for index, cmd in enumerate(cmds):
        # Try to render a simple scenario
        # which contains $ and refer to and environment variable.
        dollar_index = -1
        for i, word in enumerate(cmd):
            if word[0] == '$' and word.count("$") == 1 and env.get(word[1:]):
                dollar_index = i
                break

        if 0 < dollar_index < len(cmd) and cmd[dollar_index - 1] != '\\':
            cmd[dollar_index] = env.get(cmd[dollar_index][1:], '')

        # Handle specific commands.
        if cmd[0] == 'cd':
            cwd = cmd[1]
            continue
        if cmd[0] == 'export' and '=' in cmd[1]:
            k, v = cmd[1].split('=')
            env[k] = v
            continue

        if index == 0:
            _p = subprocess.Popen(
                cmd, stdin=stdin, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, shell=False
            )
            if communicate_kwargs.get('input'):
                _p.stdin.write(communicate_kwargs.pop('input'))
                _p.stdin.close()  # avoid hanging
        else:
            if require_stdin[index] and len(process_list):
                prev_process = process_list[-1]
                stdin = prev_process.stdout
            else:
                stdin = None
            _p = subprocess.Popen(
                cmd, stdin=stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                env=env,
                cwd=cwd
            )
        process_list.append(_p)

    last_process = process_list[-1]
    # If the stdin pipe has been broken or closed, but we didn't close it explicitly,
    # the subprocess will throw a
    # ValueError which notifies this stdin is a closed file.
    if last_process.stdin and last_process.stdin.closed:
        last_process.stdin = None

    try:
        outs, errs = last_process.communicate(**communicate_kwargs)
    finally:
        for _p in process_list:
            _p.terminate()
    return last_process.returncode, outs, errs
