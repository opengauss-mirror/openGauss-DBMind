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
import logging
import os
import select
import sys
import multiprocessing
import signal
import json

from dbmind.common.utils import dbmind_assert
from dbmind.common.utils.base import WHITE_FMT, RED_FMT, GREEN_FMT, YELLOW_FMT
from dbmind.common.platform import LINUX


def set_proc_title(name: str):
    new_name = name.encode('ascii', 'replace')

    try:
        import ctypes
        libc = ctypes.CDLL('libc.so.6')
        progname = ctypes.c_char_p.in_dll(libc, '__progname_full')
        with open('/proc/self/cmdline') as fp:
            old_progname_len = len(fp.readline())
        if old_progname_len > len(new_name):
            # padding blank chars
            new_name += b' ' * (old_progname_len - len(new_name))

        # for `ps` command:
        # Environment variables are already copied to Python app zone.
        # We can get environment variables by `os.environ` module,
        # so we can ignore the destroying from the following action.
        libc.strcpy(progname, ctypes.c_char_p(new_name))
        # for `top` command and `/proc/self/comm`:
        buff = ctypes.create_string_buffer(len(new_name) + 1)
        buff.value = new_name
        libc.prctl(15, ctypes.byref(buff), 0, 0, 0)
    except Exception as e:
        logging.debug('An error (%s) occured while setting the process name.', e)


def keep_inputting_until_correct(prompt, options):
    input_char = ''
    while input_char not in options:
        input_char = input(prompt).upper()
    return input_char


def write_to_terminal(
        message,
        level='info',
        color=None
):
    levels = ('info', 'error')
    colors = ('white', 'red', 'green', 'yellow', None)
    dbmind_assert(color in colors and level in levels)

    if not isinstance(message, str):
        message = str(message)

    # coloring.
    if color == 'white':
        out_message = WHITE_FMT.format(message)
    elif color == 'red':
        out_message = RED_FMT.format(message)
    elif color == 'green':
        out_message = GREEN_FMT.format(message)
    elif color == 'yellow':
        out_message = YELLOW_FMT.format(message)
    else:
        out_message = message

    # choosing a streaming.
    try:
        if level == 'error':
            sys.stderr.write(out_message)
            sys.stderr.write(os.linesep)
            sys.stderr.flush()
        else:
            sys.stdout.write(out_message)
            sys.stdout.write(os.linesep)
            sys.stdout.flush()
    except BrokenPipeError:
        pass
    except OSError as os_error:
        if "Input/output error" not in str(os_error):
            raise os_error


def read_input_from_pipe():
    """
    Read stdin input if there is "echo 'str1 str2' | python xx.py", return the input string.
    """
    if not LINUX:
        return ""

    input_str = ""
    r_handle, _, _ = select.select([sys.stdin], [], [], 0)
    if not r_handle:
        return ""

    for item in r_handle:
        if item == sys.stdin and not sys.stdin.line_buffering:
            input_str = sys.stdin.read().strip()
    return input_str


def parse_json_from_stdin():
    json_str = read_input_from_pipe()
    if json_str:
        return json.loads(json_str)

    return dict()


def raise_fatal_and_exit(
        message,
        exitcode=1,
        use_logging=True,
        only_print_at_main_process=False
):
    if use_logging:
        logging.fatal(message, exc_info=True)
    is_main_process = multiprocessing.current_process().name == 'MainProcess'
    if not only_print_at_main_process:
        write_to_terminal(message, level='error', color='red')
    elif is_main_process:
        write_to_terminal(message, level='error', color='red')
    if is_main_process:
        exit(exitcode)
    else:
        os.kill(signal.SIGQUIT, os.getppid())
