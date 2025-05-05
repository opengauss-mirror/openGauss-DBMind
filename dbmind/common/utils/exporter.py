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

import argparse
import logging
import os
import re
import shlex
import socket
from logging.handlers import RotatingFileHandler

import requests

from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.security import check_password_strength, is_private_key_encrypted, CertCheckerHandler, EncryptedText
from dbmind.common.utils.checking import IPV6_PATTERN, prepare_ip, uniform_ip

from .cli import parse_json_from_stdin, set_proc_title, write_to_terminal


def is_exporter_alive(host, port):
    return is_port_used(host, port)


def is_port_used(host, port):
    if IPV6_PATTERN.match(host):
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    else:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    uniformed_ip = uniform_ip(host.strip())
    if uniformed_ip in ['0.0.0.0', '::']:
        try:
            s.bind((uniformed_ip, port))
            return False
        except socket.error as e:
            if 'Address already in use' in str(e):
                return True
            else:
                return False

        finally:
            s.close()

    try:
        resp = s.connect_ex((host, port))
        if resp == 0:
            return True
        else:
            return False

    except socket.error:
        return False
    finally:
        s.close()


def get_prometheus_status(host, port, user, password, ssl_context=None):
    """Check for Prometheus's status.

    :param host: string type
    :param port: int type
    :param user: username for basic authorization
    :param password: password for basic authorization
    :param ssl_context: ssl context at request time
    :return: triplet (status, scheme, error message)
    """
    if not is_port_used(host, port):
        return False, None, None
    for scheme in ('http', 'https'):
        url = f"{scheme}://{prepare_ip(host)}:{port}/api/v1/query?query=up"
        try:
            # here we only try once
            with create_requests_session(username=user, password=password,
                                         ssl_context=ssl_context, max_retry=1) as session:
                response = session.get(
                    url,
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 200:
                    return True, scheme, None
                elif response.status_code == 401:
                    return False, scheme, 'an unauthorized connection'

        except requests.exceptions.ConnectionError:
            return False, None, 'a connection refused'

        except Exception as e:
            logging.exception(e)
            return False, scheme, 'an unexpected error: %s' % e.__class__.__name__
    return False, None, 'a connection refused'


class KVPairAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        d = dict()
        try:
            for pair in values.split(','):
                name, value = pair.split('=')
                d[name.strip()] = value.strip()
            setattr(args, self.dest, d)
        except ValueError:
            parser.error('Illegal constant labels: %s.' % values)


class ListPairAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        items = list()
        try:
            for item in values.split(','):
                if item.count('=') == 1:
                    name, value = item.split('=')
                    items.append(value.strip())
                else:
                    items.append(item.strip())
            setattr(args, self.dest, items)
        except ValueError:
            parser.error('Illegal value: %s.' % values)


def warn_logging_and_terminal(message):
    logging.warning(message)
    write_to_terminal(message, level='error')


class ReCreatingRFHandler(RotatingFileHandler):
    def __init__(self, *args, **kwargs):
        self.flag = False
        super().__init__(*args, **kwargs)

    def emit(self, record):
        try:
            if not os.path.exists(self.baseFilename):
                os.makedirs(os.path.dirname(self.baseFilename), mode=0o700, exist_ok=True)
                self.stream = open(self.baseFilename, 'a', encoding=self.encoding)
                os.chmod(self.baseFilename, 0o600)
            super().emit(record)
            self.flag = False
        except Exception as e:
            if not self.flag:
                write_to_terminal(
                    'record log failed, because: {}, please check log path: {}.'.format(str(e), self.baseFilename),
                    level='info', color='yellow')
                self.flag = True


def set_logger(filepath, level):
    level = level.upper()
    log_path = os.path.dirname(filepath)
    if not os.path.exists(log_path):
        os.makedirs(log_path, 0o700)

    formatter = logging.Formatter(
        '[%(asctime)s][%(filename)s:%(lineno)d][%(funcName)s][%(levelname)s][%(thread)d] - %(message)s'
    )
    # Specify the size of logfiles.
    max_bytes = 100 * 1024 * 1024  # 100Mb
    backup_count = 5

    handler = ReCreatingRFHandler(
        filename=filepath,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    default_logger = logging.getLogger()
    default_logger.handlers = []
    default_logger.setLevel(level)
    default_logger.addHandler(handler)

    if os.path.exists(filepath) and os.path.isfile(filepath):
        os.chmod(filepath, 0o600)


def exporter_parse_and_adjust_ssl_args(parser, argv):
    args = parser.parse_args(argv)

    if not hasattr(args, 'json'):
        args.json = parse_json_from_stdin()
    ssl_keyfile_password = None
    if args.disable_https:
        # Clear up redundant arguments.
        args.ssl_keyfile = None
        args.ssl_certfile = None
    else:
        if not (args.ssl_keyfile and args.ssl_certfile):
            parser.error(
                'If you use the Https protocol (default), you need to give the argument values '
                'of --ssl-keyfile and --ssl-certfile. If you want to verify the validity of the '
                'certificate of the communicating end, you need to specify the CA certificate file '
                'through the --ssl-ca-file option. Otherwise, use the --disable-https argument to '
                'disable the Https protocol.'
            )
        if args.ssl_ca_file is not None and args.ssl_certfile is not None and \
                not CertCheckerHandler.is_valid_cert(ca_name=args.ssl_ca_file, crt_name=args.ssl_certfile):
            parser.error(
                "The ssl ca is not valid."
            )

        ssl_keyfile_password_raw = args.json.get('ssl-keyfile-password')
        if ssl_keyfile_password_raw is None:
            parser.error(
                "You should pass ssl-keyfile-password through pipe when deploying the exporter, exit..."
            )
        ssl_keyfile_password = str(EncryptedText(ssl_keyfile_password_raw))
        if not (ssl_keyfile_password and isinstance(ssl_keyfile_password, str)) or not\
                is_private_key_encrypted(args.ssl_keyfile):
            parser.error(
                "You should pass an encrypted key file and the password of the key file"
                " through the json ssl-keyfile-password field, exit..."
            )

        if not check_password_strength(ssl_keyfile_password, is_ssl_keyfile_password=True):
            parser.error(
                "The ssl_keyfile_password is a weak password, please improve the password strength and try again."
            )

    setattr(args, 'keyfile_password', ssl_keyfile_password)

    if hasattr(args, 'tsdb_ssl_keyfile') or hasattr(args, 'tsdb_ssl_certfile') or hasattr(args, 'tsdb_ssl_ca_file'):
        if (
                args.tsdb_ssl_ca_file is not None and
                not CertCheckerHandler.is_valid_cert(ca_name=args.tsdb_ssl_ca_file, crt_name=None)
        ) or (
                args.tsdb_ssl_certfile is not None and
                not CertCheckerHandler.is_valid_cert(ca_name=None, crt_name=args.tsdb_ssl_certfile)
        ):
            parser.error('The tsdb ssl ca is not valid.')
        tsdb_keyfile_password = None
        if args.tsdb_ssl_keyfile or args.tsdb_ssl_certfile:
            if not (args.tsdb_ssl_keyfile and args.tsdb_ssl_certfile):
                parser.error(
                    'If TSDB use the Https protocol, you need to give the argument values '
                    'of --tsdb-ssl-keyfile and --tsdb-ssl-certfile. If you want to verify the validity of the '
                    'certificate of the communicating end, you need to specify the CA certificate file '
                    'through the --tsdb-ssl-ca-file option. Otherwise, it is not necessary to provide '
                    'any certificate parameter.'
                )
            tsdb_keyfile_password = args.json.get('tsdb-keyfile-password')
            if not (tsdb_keyfile_password and isinstance(tsdb_keyfile_password, str)) or not\
                    is_private_key_encrypted(args.tsdb_ssl_keyfile):
                parser.error(
                    "You should pass an encrypted key file and the password of the key file"
                    " through the json tsdb-keyfile-password field, exit..."
                )

            if not check_password_strength(tsdb_keyfile_password, is_ssl_keyfile_password=True):
                parser.error(
                    "The tsdb_ssl_keyfile_password is a weak password,"
                    " please improve the password strength and try again."
                )

        setattr(args, 'tsdb_keyfile_password', tsdb_keyfile_password)

    return args


def wipe_off_sensitive_information_from_proc_title(old, new, wipe_argument=False):
    """
    Removes the password from the process title.

    :param old: old string that needs to be removed.
    :param new: use this new string to replace the old one.
    :param wipe_argument: specifies whether to wipe off argument value.
    :return None
    """
    with open('/proc/self/cmdline') as fp:
        cmdline = fp.readline().replace('\x00', ' ')
    wiped_cmdline = cmdline
    if wipe_argument:
        for index, word in enumerate(shlex.split(cmdline)):
            if word == old:
                wiped_cmdline = cmdline.replace(shlex.split(cmdline)[index + 1], new)
    else:
        wiped_cmdline = cmdline.replace(old, new)
    set_proc_title(wiped_cmdline)


def wipe_off_dsn_password(db_connection_string):
    """
    Removes the password from the database connection string
    @param db_connection_string: database connection string
    @return: the database connection string with the password removed
    """
    result = re.findall(r'.*://.*:(.+)@.*:.*/.*', db_connection_string)
    if len(result) == 0:
        result = re.findall(r'password=(.*)\s', db_connection_string)
        if len(result) == 0:
            return '*********'

    password = result[0]
    if len(password) == 0:
        return '*********'

    return db_connection_string.replace(password, '******')
