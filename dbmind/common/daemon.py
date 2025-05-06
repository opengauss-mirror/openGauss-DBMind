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

import abc
import atexit
import logging
import os
import signal
import sys
import time
import threading
import subprocess

from dbmind.common.utils import get_env
from dbmind.common.utils import dbmind_assert
import dbmind.common.process
from .platform import WIN32

write_info = sys.stdout.write
write_error = sys.stderr.write

STAT_TOTAL_NUM = 52
PARENET_PROCESS_INDEX = 3
RESIDUAL_PROCESS_PARENT_PID = 1


def get_children_pid_file(pid_file):
    return pid_file.replace('pid', 'children.pid')


def read_dbmind_pid_file(filepath):
    """Return the running process's pid from file.
    If the acquisition fails, return 0.

    Note
    ~~~~~~~~

    The func only can read the pid file for DBMind due to specific/fine-grained verification.
    """
    try:
        if not os.path.exists(filepath):
            return 0
        with open(filepath, mode='r') as fp:
            pid = int(fp.readline().strip())
        proc = dbmind.common.process.Process(pid)

        if proc.alive and os.path.samefile(os.path.dirname(filepath), proc.cwd):
            return pid
        else:
            return 0
    except PermissionError:
        return 0
    except ValueError:
        return 0
    except FileNotFoundError:
        return 0


class Daemon:
    """A generic daemon class for DBMind."""

    class STATUS:
        PENDING = 0
        RUNNING = 1

    def __init__(self, pid_file, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        # Setting the path of the PID file
        # is an empty string allows us to disable the PID check.
        self.pid_file = os.path.realpath(pid_file) if pid_file else ''
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.status = Daemon.STATUS.PENDING

    def daemonize(self):
        # This environment variable DBMIND_USE_DAEMON is often used when the startup 
        # process cannot exit, such as in the docker. 
        # If the startup process exits, the service will switch 
        # into the backend and other high-level applications 
        # maybe cannot know whether the service exits. 
        use_daemon = get_env('DBMIND_USE_DAEMON', default_value='1') == '1'
        if not WIN32 and use_daemon:
            """UNIX-like OS has the double-fork magic."""
            try:
                if os.fork() > 0:
                    sys.exit(0)  # the first parent exits.
            except OSError as e:
                write_error('[Daemon Process]: cannot fork the first process: %s.\n' % e.strerror)
                sys.exit(1)
            # modify env
            os.chdir('/')
            os.setsid()
            os.umask(0o0077)
            try:
                if os.fork() > 0:
                    sys.exit(0)
            except OSError as e:
                write_error('[Daemon Process]: cannot fork the second process: %s.\n' % e.strerror)
                sys.exit(1)

            # redirect standard fd
            sys.stdout.flush()
            sys.stderr.flush()
            os.dup2(sys.stdin.fileno(), open(self.stdin, 'r').fileno())
            os.dup2(sys.stdout.fileno(), open(self.stdout, 'r').fileno())
            os.dup2(sys.stderr.fileno(), open(self.stderr, 'r').fileno())

            atexit.register(
                lambda: os.path.exists(self.pid_file) and os.remove(self.pid_file)
            )
            atexit.register(self.clean)

        # Write daemon pid file.
        if self.pid_file:
            with open(self.pid_file, 'w+') as fp:
                fp.write('%d\n' % os.getpid())

    def start(self):
        """Start the daemon process"""
        # Verify that the pidfile is valid and check if the daemon already runs.
        pid = read_dbmind_pid_file(self.pid_file)
        if pid > 0:
            write_error('[Daemon Process]: process (%d) already exists.\n' % pid)
            sys.exit(1)

        self.daemonize()
        self.status = Daemon.STATUS.RUNNING
        write_info('The process is starting.\n')
        deamon_thread = threading.Thread(target=self.check_child_process)
        deamon_thread.daemon = True
        deamon_thread.start()
        self.run()

    def check_child_process(self):
        child_pid_file = get_children_pid_file(self.pid_file)
        if os.path.exists(child_pid_file):
            start_time = os.path.getmtime(child_pid_file)
        else:
            start_time = None
        while True:
            time.sleep(5)
            if not os.path.exists(child_pid_file):
                continue
            updated_time = os.path.getmtime(child_pid_file)
            if start_time == updated_time:
                continue
            with open(child_pid_file, mode='r') as fp:
                pid_list = fp.readline().split(',')
            for pid in pid_list:
                try:
                    output = subprocess.check_output(['ps', '-p', str(pid)], shell=False)
                    if str(pid) not in output.decode('utf-8'):
                        os.kill(os.getpid(), signal.SIGKILL)
                    output = subprocess.check_output(['ps', '-o', 'stat=', '-p', str(pid)], shell=False)
                    if 'Z' in output.decode('utf-8') or 'T' in output.decode('utf-8'):
                        os.kill(os.getpid(), signal.SIGKILL)
                except subprocess.CalledProcessError:
                    os.kill(os.getpid(), signal.SIGKILL)

    def stop(self, level='low'):
        level_mapper = {'low': signal.SIGTERM, 'mid': signal.SIGQUIT, 'high': signal.SIGKILL}

        """Stop the daemon process"""
        pid = read_dbmind_pid_file(self.pid_file)
        if pid <= 0:
            write_error('[Daemon Process]: process not running.\n')
            return

        def kill_process_group(sig):
            if sig in (signal.SIGTERM, signal.SIGQUIT):
                os.kill(pid, sig)
            elif sig == signal.SIGKILL:
                os.kill(pid, sig)
                clean_dbmind_process(get_children_pid_file(self.pid_file), only_residual=False)
            else:
                dbmind_assert(False)

        # If the pid is valid, try to kill the daemon process.
        try:
            send_count = 0
            while True:
                # retry to kill
                write_error('Waiting for process to exit...\n')
                kill_process_group(level_mapper[level])
                send_count += 1
                time.sleep(1)
                # if quitting is timeout, signal will upgrade.
                if level == 'mid' and send_count >= 5:
                    level = 'high'

        except OSError as e:
            if 'No such process' in e.strerror and os.path.exists(self.pid_file):
                os.remove(self.pid_file)

    @abc.abstractmethod
    def clean(self):
        """Cleanup before exit"""

    @abc.abstractmethod
    def run(self):
        """Subclass should override the run() method."""


def clean_dbmind_process(filepath, only_residual=False):
    try:
        if not os.path.exists(filepath):
            return
        with open(filepath, mode='r') as fp:
            pid_list = fp.readline().split(',')
        if not pid_list:
            return
        for pid in pid_list:
            if not only_residual:
                os.kill(int(pid), signal.SIGKILL)
            elif check_parent_child_process(pid, RESIDUAL_PROCESS_PARENT_PID):
                os.kill(int(pid), signal.SIGKILL)
    except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError) as e:
        logging.warning('An exception occurred during clean residual dbmind process: %s', str(e))


def check_parent_child_process(pid, parent_pid):
    try:
        with open('/proc/{}/stat'.format(pid), "r") as f:
            data = f.readline().split()
        if len(data) < STAT_TOTAL_NUM:
            return False
        # the process name may contain space
        index_offset = len(data) - STAT_TOTAL_NUM
        if not data[PARENET_PROCESS_INDEX + index_offset].isdigit():
            return False
        if int(data[PARENET_PROCESS_INDEX + index_offset]) == parent_pid:
            return True
        return False
    except (FileNotFoundError, PermissionError):
        return False
