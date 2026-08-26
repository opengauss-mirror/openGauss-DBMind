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
import os
import sys

from dbmind import constants
from dbmind.common.daemon import Daemon
from dbmind.common.utils import exporter
from dbmind.common.utils import write_to_terminal
from dbmind.common.types.ssl import SSLContext
from dbmind.common.utils.checking import (
    warn_ssl_certificate, CheckPort, CheckIP, path_type,
    positive_int_type, prepare_ip
)
from dbmind.common.utils.exporter import (
    is_exporter_alive, set_logger, exporter_parse_and_adjust_ssl_args,
    wipe_off_sensitive_information_from_proc_title
)
from dbmind.constants import __version__
from . import controller
from . import dao
from . import service

CURR_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..')
)
DEFAULT_YAML = 'reprocessing_exporter.yml'

DEFAULT_LOGFILE = 'reprocessing_exporter.log'
exporter_info_dict = {
    'ip': '',
    'port': '',
    'logfile': ''
}


def parse_argv(argv):
    parser = argparse.ArgumentParser(
        description='Reprocessing Exporter: A re-processing module for metrics stored in the Prometheus server.',
        allow_abbrev=False
    )
    parser.add_argument('prometheus_host', help='from which host to pull data')
    parser.add_argument('prometheus_port', type=positive_int_type,
                        help='the port to connect to the Prometheus host')
    parser.add_argument('--prometheus-auth-user',
                        help='use this user for basic authorization to connect to the Prometheus server')
    parser.add_argument('--prometheus-auth-password',
                        help='use this password for basic authorization to connect to the Prometheus server')
    parser.add_argument('--disable-https', action='store_true',
                        help='disable Https scheme')
    parser.add_argument('--ssl-keyfile', type=path_type, help='set the path of ssl key file')
    parser.add_argument('--ssl-certfile', type=path_type, help='set the path of ssl certificate file')
    parser.add_argument('--ssl-ca-file', type=path_type, help='set the path of ssl ca file')
    parser.add_argument('--tsdb-ssl-keyfile', type=path_type, help='set the path of tsdb ssl key file')
    parser.add_argument('--tsdb-ssl-certfile', type=path_type, help='set the path of tsdb ssl certificate file')
    parser.add_argument('--tsdb-ssl-ca-file', type=path_type, help='set the path of tsdb ssl ca file')
    parser.add_argument('--web.listen-address', default='127.0.0.1', action=CheckIP,
                        help='address on which to expose metrics and web interface')
    parser.add_argument('--web.listen-port', type=int, default=8181, action=CheckPort,
                        help='listen port to expose metrics and web interface')
    parser.add_argument('--collector.config', '--config', type=path_type, default=os.path.join(CURR_DIR, DEFAULT_YAML),
                        help='according to the content of the yaml file for metric collection')
    parser.add_argument('--log.filepath', type=os.path.realpath,
                        default=os.path.join(os.getcwd(), DEFAULT_LOGFILE),
                        help='the path to log')
    parser.add_argument('--log.level', default='info', choices=('debug', 'info', 'warn', 'error', 'fatal'),
                        help='only log messages with the given severity or above.'
                             ' Valid levels: [debug, info, warn, error, fatal]')
    parser.add_argument('-v', '--version', action='version', version=__version__)

    args = exporter_parse_and_adjust_ssl_args(parser, argv)

    return args


class ExporterMain(Daemon):
    def clean(self):
        pass

    def __init__(self, args):
        self.args = args
        cur_path = os.path.realpath(os.path.dirname(__file__))
        proj_path = cur_path[:cur_path.rfind('dbmind')]
        self.pid_file = os.path.join(proj_path, constants.REPROCESSING_PIDFILE_NAME)
        super().__init__(self.pid_file)

    def change_file_permissions(self):
        if (
                self.args.ssl_keyfile and
                os.path.exists(self.args.ssl_keyfile) and
                os.path.isfile(self.args.ssl_keyfile)
        ):
            os.chmod(self.args.ssl_keyfile, 0o400)
        if (
                self.args.ssl_certfile and
                os.path.exists(self.args.ssl_certfile) and
                os.path.isfile(self.args.ssl_certfile)
        ):
            os.chmod(self.args.ssl_certfile, 0o400)
        if (
                self.args.ssl_ca_file and
                os.path.exists(self.args.ssl_ca_file) and
                os.path.isfile(self.args.ssl_ca_file)
        ):
            os.chmod(self.args.ssl_ca_file, 0o400)
        # TSDB-related certificate
        if (
                self.args.tsdb_ssl_keyfile and
                os.path.exists(self.args.tsdb_ssl_keyfile) and
                os.path.isfile(self.args.tsdb_ssl_keyfile)
        ):
            os.chmod(self.args.tsdb_ssl_keyfile, 0o400)
        if (
                self.args.tsdb_ssl_certfile and
                os.path.exists(self.args.tsdb_ssl_certfile) and
                os.path.isfile(self.args.tsdb_ssl_certfile)
        ):
            os.chmod(self.args.tsdb_ssl_certfile, 0o400)
        if (
                self.args.tsdb_ssl_ca_file and
                os.path.exists(self.args.tsdb_ssl_ca_file) and
                os.path.isfile(self.args.tsdb_ssl_ca_file)
        ):
            os.chmod(self.args.tsdb_ssl_ca_file, 0o400)
        if self.args.__dict__['log.filepath'] and os.path.exists(self.args.__dict__['log.filepath']) and os.path.isfile(
            self.args.__dict__['log.filepath']
        ):
            os.chmod(self.args.__dict__['log.filepath'], 0o600)
        if os.path.exists(CURR_DIR):
            os.chmod(CURR_DIR, 0o700)
        if os.path.exists(os.path.join(CURR_DIR, DEFAULT_YAML)):
            os.chmod(os.path.join(CURR_DIR, DEFAULT_YAML), 0o600)
        if os.path.exists(self.args.__dict__['collector.config']) and os.path.isfile(
                self.args.__dict__['collector.config']):
            os.chmod(self.args.__dict__['collector.config'], 0o600)
        if os.path.exists(self.pid_file):
            os.chmod(self.pid_file, 0o600)

    def run(self):
        # Wipe off sensitive string and path of ssl certificate relative path.
        try:
            auth_user = self.args.prometheus_auth_user
            auth_password = self.args.prometheus_auth_password
            if auth_user:
                wipe_off_sensitive_information_from_proc_title(auth_user, '******')
            if auth_password:
                wipe_off_sensitive_information_from_proc_title(auth_password, '******')

            ssl_certfile = self.args.ssl_certfile
            ssl_keyfile = self.args.ssl_keyfile
            ssl_ca_file = self.args.ssl_ca_file
            if ssl_certfile:
                wipe_off_sensitive_information_from_proc_title('--ssl-certfile', '******', wipe_argument=True)
            if ssl_keyfile:
                wipe_off_sensitive_information_from_proc_title('--ssl-keyfile', '******', wipe_argument=True)
            if ssl_ca_file:
                wipe_off_sensitive_information_from_proc_title('--ssl-ca-file', '******', wipe_argument=True)

            tsdb_ssl_certfile = self.args.tsdb_ssl_certfile
            tsdb_ssl_keyfile = self.args.tsdb_ssl_keyfile
            tsdb_ssl_ca_file = self.args.tsdb_ssl_ca_file
            if tsdb_ssl_certfile:
                wipe_off_sensitive_information_from_proc_title('--tsdb-ssl-certfile', '******', wipe_argument=True)
            if tsdb_ssl_keyfile:
                wipe_off_sensitive_information_from_proc_title('--tsdb-ssl-keyfile', '******', wipe_argument=True)
            if tsdb_ssl_ca_file:
                wipe_off_sensitive_information_from_proc_title('--tsdb-ssl-ca-file', '******', wipe_argument=True)

        except FileNotFoundError:
            write_to_terminal('Failed to wipe off sensitive information, exiting...')
            sys.exit(1)

        if self.args.tsdb_ssl_certfile and self.args.tsdb_ssl_keyfile:
            ssl_context = SSLContext(self.args.tsdb_ssl_certfile,
                                     self.args.tsdb_ssl_keyfile,
                                     self.args.tsdb_keyfile_password,
                                     self.args.tsdb_ssl_ca_file)
        else:
            ssl_context = None

        alive, scheme, msg = exporter.get_prometheus_status(
            self.args.prometheus_host, self.args.prometheus_port,
            self.args.prometheus_auth_user,
            self.args.prometheus_auth_password,
            ssl_context=ssl_context
        )
        if not alive:
            write_to_terminal('Failed to connect to the Prometheus server due to %s, exiting...' % msg, color='red')
            sys.exit(1)

        setattr(
            self.args,
            'prometheus_url',
            f'{scheme}://{prepare_ip(self.args.prometheus_host)}:{self.args.prometheus_port}'
        )

        set_logger(self.args.__dict__['log.filepath'],
                   self.args.__dict__['log.level'])
        self.change_file_permissions()
        exporter_info_dict['logfile'] = self.args.__dict__['log.filepath']
        exporter_info_dict['ip'] = self.args.__dict__['web.listen_address']
        exporter_info_dict['port'] = self.args.__dict__['web.listen_port']
        dao.set_prometheus_client(
            url=self.args.__dict__['prometheus_url'],
            username=self.args.prometheus_auth_user,
            password=self.args.prometheus_auth_password,
            ssl_context=ssl_context
        )
        service.register_prometheus_metrics(
            rule_filepath=self.args.__dict__['collector.config']
        )

        warn_ssl_certificate(self.args.ssl_certfile, self.args.ssl_keyfile, self.args.ssl_ca_file)
        warn_ssl_certificate(self.args.tsdb_ssl_certfile, self.args.tsdb_ssl_keyfile, self.args.tsdb_ssl_ca_file)

        controller.run(
            host=self.args.__dict__['web.listen_address'],
            port=self.args.__dict__['web.listen_port'],
            ssl_keyfile=self.args.ssl_keyfile,
            ssl_certfile=self.args.ssl_certfile,
            ssl_keyfile_password=self.args.keyfile_password,
            ssl_ca_file=self.args.ssl_ca_file
        )


def main(argv):
    args = parse_argv(argv)
    if is_exporter_alive(args.__dict__['web.listen_address'],
                         args.__dict__['web.listen_port'],
                         ):
        write_to_terminal('Service has been started or the address already in use, exiting...', color='red')
        return
    ExporterMain(args).start()
