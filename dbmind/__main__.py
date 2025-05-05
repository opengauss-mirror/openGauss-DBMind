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

import ctypes
import logging
import os
import sys


def replace_cmdline(new_cmdline):
    try:
        libc = ctypes.CDLL('libc.so.6')
        progname = ctypes.c_char_p.in_dll(libc, '__progname_full')
        with open('/proc/self/cmdline') as fp:
            old_progname_len = len(fp.readline())

        if old_progname_len > len(new_cmdline):
            # padding blank chars
            new_cmdline += b' ' * (old_progname_len - len(new_cmdline))

        libc.strcpy(progname, ctypes.c_char_p(new_cmdline))
        buff = ctypes.create_string_buffer(len(new_cmdline) + 1)
        buff.value = new_cmdline
        libc.prctl(15, ctypes.byref(buff), 0, 0, 0)
    except Exception as e:
        logging.warning('Cannot mask the process title due to %s. There may be a security risk, '
                        'please take notice of it.', e)
        sys.exit(1)


def check_ssl_valid_and_ssl_encrypt_status():
    from dbmind.common.utils.base import get_env
    from dbmind.common.utils.cli import write_to_terminal
    env_key = get_env('PGSSLKEY')
    if not env_key:
        return
    if not os.path.exists(env_key):
        return
    with open(env_key) as fp:
        for line in fp.readlines():
            if 'ENCRYPTED' in line.upper():
                result_msg = "The ssl key file {} exists and is encrypted. Current DBMind version does not support" \
                             " encryption authentication. Use default non-SSL connection.".format(env_key)
                write_to_terminal(result_msg, color='yellow')
                return


# Explicitly mask sub-command's process title.
if len(sys.argv) > 1 and sys.argv[1] == 'set':
    new_name = b'DBMind Setting'
    replace_cmdline(new_name)
elif len(sys.argv) > 1 and 'component' in sys.argv and "--url" in sys.argv:
    args_list = sys.argv.copy()
    idx = args_list.index("--url") + 1
    args_list[idx] = "******"
    new_name = " ".join(args_list).encode()
    replace_cmdline(new_name)

# Discard the first item of the sys.path because
# some submodules probably have the same name as
# dependencies.
path0 = os.path.realpath(sys.path[0])
i = 0
while i < len(sys.path):
    if path0 == os.path.realpath(sys.path[i]):
        sys.path.pop(i)
        i -= 1
    i += 1

try:
    from dbmind.cmd import main
except ImportError:
    import sys
    import os

    curr_path = os.path.dirname(os.path.realpath(__file__))
    root_path = os.path.dirname(curr_path)
    sys.path.append(root_path)
    from dbmind.cmd import main

if len(sys.argv) > 1 and (
        'opengauss_exporter' in sys.argv or
        'start' in sys.argv or 'restart' in sys.argv or
        '--initialize' in sys.argv):
    check_ssl_valid_and_ssl_encrypt_status()

main()
