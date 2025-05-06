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
import signal
import socket
import subprocess
import threading
import time
import os
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait

import pexpect
from pexpect import TIMEOUT, EOF
if os.sys.platform == 'win32':
    pass
else:
    from pexpect.pxssh import pxssh

from dbmind.common.utils.base import SecurityChecker
from dbmind.common.utils import write_to_terminal


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
                       username=self.user,
                       password=self.pwd,
                       port=self.port)
        else:
            return LocalExec()

    def _is_remote(self):
        if not self.host:
            return False  # Not setting host is treated as local.

        hostname = socket.gethostname()
        try:
            v4_addrs = socket.getaddrinfo(hostname, None, family=socket.AF_INET)
        except socket.gaierror:
            v4_addrs = []

        try:
            v6_addrs = socket.getaddrinfo(hostname, None, family=socket.AF_INET6)
        except socket.gaierror:
            v6_addrs = []

        addr_infos = v4_addrs + v6_addrs
        ip_address_list = [addr_info[4][0] for addr_info in addr_infos]
        if self.host in ('127.0.0.1', 'localhost', '::1') or self.host in ip_address_list:
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
    def __init__(self, host, username, password, port=22, max_retry_times=5, timeout=10):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.max_retry_times = max_retry_times
        self.timeout = timeout
        self.retry_cnt = 0
        self.executor = SSH.connect(host, username, password, port=self.port)
        self.check_version()

    @staticmethod
    def connect(host, username, password, port=22):
        ssh_options = {
            'PreferredAuthentications': 'password',
            'PubkeyAuthentication': 'no',
            'StrictHostKeyChecking': 'no',
            'UserKnownHostsFile': '/dev/null'
        }
        executor = pxssh(
            echo=False,
            options=ssh_options,
            env={'LD_LIBRARY_PATH': ''}
        )
        try:
            executor.login(
                host,
                port=port,
                username=username,
                password=password,
                sync_multiplier=8
            )
            return executor

        except Exception as e:
            if executor:
                executor.close()

            raise e

    def exec_command_sync(self, command, *args, redirection=" 1>>stdout 2>>stderr", **kwargs):
        if not self.executor or self.executor.closed:
            self.executor = self.connect(self.host, self.username, self.password)

        if not hasattr(self.executor, 'sendline'):
            raise ConnectionError("Slow connection.")

        try:
            if isinstance(command, (list, tuple, set)):
                for cmd in command:
                    self._exec_single_command(cmd, redirection=redirection)
            else:
                self._exec_single_command(command, redirection=redirection)

            self.retry_cnt = 0
            self.exit_status = 0

            if redirection == " 1>>stdout 2>>stderr":
                return self.get_stdout(), self.get_stderr()
            else:
                return "", ""

        except pexpect.ExceptionPexpect:
            self.executor.close()
            self.executor = self.connect(self.host, self.username, self.password)
            if self.retry_cnt >= self.max_retry_times:
                self.exit_status = -1
                raise ConnectionError("Can not connect to remote host.")

            self.retry_cnt += 1
            self.exec_command_sync(command)

    def get_stdout(self):
        self._exec_single_command("cat stdout")
        stdout = self.executor.before.decode()
        num = 0
        while not stdout and num < 5:
            self.executor.prompt(timeout=0.1)
            stdout = self.executor.before.decode()
            num += 1

        if "cat stdout\r\n" in stdout:
            idx = stdout.find("cat stdout\r\n")
            stdout = stdout[idx + 12:]

        self._exec_single_command("rm -f stdout")

        return stdout

    def get_stderr(self):
        self._exec_single_command("cat stderr")
        stderr = self.executor.before.decode()
        num = 0
        while not stderr and num < 5:
            self.executor.prompt(timeout=0.1)
            stderr = self.executor.before.decode()
            num += 1

        if "cat stderr\r\n" in stderr:
            idx = stderr.find("cat stderr\r\n")
            stderr = stderr[idx + 12:]

        self._exec_single_command("rm -f stderr")

        return stderr

    def _exec_single_command(self, command, redirection=""):
        self.executor.sendline(command + redirection)
        self.executor.prompt(timeout=self.timeout)

    def check_version(self):
        conn = pexpect.spawn(f"ssh -1 {self.host}")
        try:
            i = conn.expect(
                ['Protocol major versions differ',
                 'SSH protocol v.1 is no longer supported',
                 TIMEOUT],
                timeout=self.timeout
            )
            if i == 0 or i == 1:
                write_to_terminal('The ssh version 2 is qualified.')
            else:
                raise ValueError('The ssh version is lower than v2.x.')

        except pexpect.ExceptionPexpect:
            raise ValueError('The ssh version is lower than v2.x.')

        finally:
            conn.close()

    def close(self):
        self.executor.close()


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
                try:
                    outs, errs = proc.communicate(timeout=kwargs.get('timeout', None))
                except Exception as e:
                    proc.kill()
                    raise e

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


class SubprocessExecutor:
    def __init__(self, stdin, stdout, stderr, env, cwd, timeout=None):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.env = env
        self.cwd = cwd
        self.timeout = timeout
        self.timed_out = False

    def timeout_callback(self, process):
        self.timed_out = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except Exception as e:
            logging.exception(f"Failed to kill process group: {e}")

    def run(self, command, pipe_input, close_fds=True,
            preexec_fn=None, start_new_session=False):
        proc = subprocess.Popen(
            command,
            stdin=self.stdin,
            stdout=self.stdout,
            stderr=self.stderr,
            env=self.env,
            cwd=self.cwd,
            close_fds=close_fds,
            preexec_fn=preexec_fn,
            start_new_session=start_new_session
        )
        timer = threading.Timer(self.timeout, self.timeout_callback, [proc])
        timer.start()
        try:
            output, errors = proc.communicate(
                input=pipe_input,
                timeout=self.timeout
            )
        except subprocess.TimeoutExpired:
            self.timed_out = True
        finally:
            timer.cancel()
            if proc.stdin:
                proc.stdin.close()

            if proc.stdout:
                proc.stdout.close()

            if proc.stderr:
                proc.stderr.close()

            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait()
            except OSError:
                pass

        if self.timed_out:
            raise subprocess.TimeoutExpired(command, self.timeout)

        return proc.returncode, output, errors


def multiple_cmd_exec(cmdline, **communicate_kwargs):
    """This function only returns the execution result of the last
    command. And only support the basic scenarios.

    Notice: this function is only a simple wrapper of Popen,
     which doesn't support complicated scenarios. e.g.,
    `echo $?`, `dirname $(pwd)`, `PATH=$PATH; echo $PATH`, `echo abc >&2`
    """
    cmds, require_stdin = to_cmds(cmdline)
    if not communicate_kwargs.get('input'):
        stdin = None
    else:
        stdin = subprocess.PIPE

    # Support some basic scenarios, for example,
    # cd and export commands.
    cwd = os.getcwd()
    env = os.environ.copy()
    timeout = communicate_kwargs.get("timeout")

    end_time = time.monotonic() + timeout if isinstance(timeout, (int, float)) else None
    for index, cmd in enumerate(cmds):
        # Try to render a simple scenario
        # which contains $ and refer to and environment variable.
        dollar_index = -1
        for i, word in enumerate(cmd):
            if word[0] == '$' and word.count("$") == 1 and env.get(word[1:]):
                p_word = env.get(word[1:]).replace("\\", "\\\\").replace('"', '\\"\\"')
                SecurityChecker.check_injection_char(p_word)
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
            if communicate_kwargs.get('input'):
                the_input = communicate_kwargs.get('input')
            else:
                the_input = b""

        elif require_stdin[index]:
            stdin = subprocess.PIPE
            the_input = last_output
        else:
            stdin = None
            the_input = b""

        remaining_time = None if end_time is None else end_time - time.monotonic()
        subprocess_executor = SubprocessExecutor(
            stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=cwd,
            timeout=remaining_time
        )
        exitcode, last_output, errs = subprocess_executor.run(
            cmd, the_input, start_new_session=True
        )

    return exitcode, last_output, errs


class SFTP:
    def __init__(self, host, username, password, port=22, timeout=1):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.retry_cnt = 0
        self.executor = None
        self.remote_executor = SSH(host, username, password, port=port)

    def connect(self):
        second_regex_array = [
            "sftp>",
            "(?i)are you sure you want to continue connecting",
            r"(?i)(?:password:)|(?:passphrase for key)",
            "(?i)permission denied",
            "(?i)terminal type",
        ]
        first_regex_array = second_regex_array + ["(?i)connection closed by remote host", EOF]

        self.executor = pexpect.spawn(f"sftp -2 -P {self.port} {self.username}@{self.host}")
        try:  # First phase
            i = self.executor.expect(first_regex_array, timeout=self.timeout)
            if i == 1:  # New certificate -- always accept it.
                self.executor.sendline("yes")
                i = self.executor.expect(second_regex_array, timeout=self.timeout)
            if i == 2:  # password or passphrase
                self.executor.sendline(self.password)
                i = self.executor.expect(second_regex_array, timeout=self.timeout)
            if i == 4:
                self.executor.sendline("ansi")
                i = self.executor.expect(second_regex_array, timeout=self.timeout)
            if i == 6:
                self.executor.close()
                raise ConnectionError('could not establish connection to host')

            if i == 0:  # as expected
                pass
            elif i == 1:  # Second phase
                self.executor.close()
                raise ConnectionError('Weird error. Got "are you sure" prompt twice.')
            elif i == 2:  # password prompt again means incorrect password。
                self.executor.close()
                raise ConnectionError('user or password error, connection refused')
            elif i == 3:  # permission denied -- password was bad.
                self.executor.close()
                raise ConnectionError('user or password error, connection refused')
            elif i == 4:  # terminal type again? WTF?
                self.executor.close()
                raise ConnectionError('Weird error. Got "terminal type" prompt twice.')
            elif i == 5:  # Connection closed by remote host
                self.executor.close()
                raise ConnectionError('connection closed')
            else:  # Unexpected
                self.executor.close()
                raise ConnectionError('unexpected login response')

        except Exception as e:
            if self.executor or not self.executor.closed:
                self.executor.close()
            raise e

    def upload_file(self, local_file, remote_file):
        file = os.path.split(local_file)[1]
        if not os.path.isfile(local_file):
            raise FileNotFoundError(f"File {local_file} is not found.")
        try:
            self.executor.sendline(f"put '{local_file}' '{remote_file}'")
            i = self.executor.expect(["100%", "No such file or directory"], timeout=self.timeout)
            if i == 0:
                write_to_terminal(f"Successfully uploaded local {local_file} to {self.host}{remote_file}.")
            elif i == 1:
                raise FileNotFoundError(f"No such file or directory: {local_file}.")

        except Exception as e:
            write_to_terminal(
                f"WARNING: Transportation of {local_file} to {self.host}{remote_file} failed, "
                f"check if '{file}' is running in processes: {e}"
            )

    def upload_dir(self, local_dir, remote_dir):
        self.mkdir(remote_dir)
        for entry in os.scandir(local_dir):
            local_item = os.path.join(local_dir, entry.name)
            remote_item = os.path.join(remote_dir, entry.name)
            if entry.is_dir():
                self.upload_dir(local_item, remote_item)
            elif entry.is_file():
                self.upload_file(local_item, remote_item)

    def exists(self, remote_path):
        self.executor.sendline(f"ls {remote_path}")
        i = self.executor.expect(["not found", TIMEOUT], timeout=0.1)
        self.executor.expect(["sftp>", TIMEOUT], timeout=0.1)
        if i == 0:
            return False
        return True

    def mkdir(self, path):
        if self.exists(path):
            return

        self.mkdir(os.path.dirname(path))
        self.executor.sendline(f"mkdir {path}")
        i = self.executor.expect(["Permission denied", TIMEOUT], timeout=0.1)
        if i == 0:
            raise pexpect.ExceptionPexpect(
                "Invalid username, password, address or unauthorized path for "
                r"{}@{}/{}".format(self.username, self.host, path)
            )

    def exec_sftp_command(self, command):
        self.executor.sendline(command)
        i = self.executor.expect(["sftp>", TIMEOUT], timeout=self.timeout)
        if i == 1:
            raise TIMEOUT(f"The execution has exceeded the timeout limit {self.timeout}s")

    def close(self):
        self.executor.close()
        self.remote_executor.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


thr_local_variables = threading.local()


def sftp_put(local_file, remote_file):
    client = thr_local_variables.client
    if not os.path.isfile(local_file):
        raise FileNotFoundError(f'File {local_file} is not found.')

    client.upload_file(local_file, remote_file)


def close_client():
    client = thr_local_variables.client
    client.close()
    time.sleep(1)


def initializer(host, username, passwd, port):
    client = SFTP(host, username, passwd, port=port)
    client.connect()
    thr_local_variables.client = client


def transfer_pool(host, username, passwd, upload_list, port=22, workers=4, method='process'):
    if method == "process":
        pool = ProcessPoolExecutor
    elif method == "thread":
        pool = ThreadPoolExecutor
    else:
        raise AttributeError("Attribute method must be 'process' or 'thread'.")

    with pool(
        max_workers=workers,
        initializer=initializer,
        initargs=(host, username, passwd, port)
    ) as executor:
        futures = [executor.submit(sftp_put, *(local_file, remote_file))
                   for local_file, remote_file in upload_list]

        wait(futures)
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.warning(str(e))

        futures = [executor.submit(close_client) for _ in range(workers)]

        wait(futures)
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.warning(str(e))
