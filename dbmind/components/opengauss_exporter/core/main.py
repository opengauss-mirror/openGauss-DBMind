# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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

import yaml
from psycopg2.extensions import parse_dsn, make_dsn

from dbmind.common.daemon import Daemon
from dbmind.common.platform import LINUX
from dbmind.common.utils import write_to_terminal
from dbmind.common.utils.checking import (
    warn_ssl_certificate,
    CheckPort,
    CheckIP,
    CheckDSN,
    path_type,
    positive_int_type,
    not_negative_int_type,
    prepare_ip,
    check_ip_valid,
    check_port_valid
)
from dbmind.common.utils.exporter import (
    KVPairAction,
    ListPairAction,
    wipe_off_sensitive_information_from_proc_title,
    wipe_off_dsn_password,
    is_exporter_alive,
    set_logger,
    exporter_parse_and_adjust_ssl_args
)
from dbmind.constants import __version__

from . import controller
from . import service
from .agent import create_agent_rpc_service

ROOT_DIR_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
YAML_DIR_PATH = os.path.join(ROOT_DIR_PATH, 'yamls')

COMING_FROM_EACH_DATABASE = 'coming_from_each_database.yml'
DEFAULT_YAML = 'default.yml'
PG_SETTINGS_YAML = 'pg_settings.yml'
STATEMENTS_YAML = 'statements.yml'
MAX_NODE_NUMBER = 8

DEFAULT_LOGFILE = 'dbmind_opengauss_exporter.log'
exporter_info_dict = {
    'constant_labels_instance': '',
    'ip': '',
    'port': '',
    'logfile': ''
}


def file_exists(file):
    return file and os.path.exists(file) and os.path.isfile(file)


def parse_argv(argv):
    parser = argparse.ArgumentParser(
        description='openGauss Exporter (DBMind): Monitoring or controlling for openGauss.',
        allow_abbrev=False
    )
    parser.add_argument('--url', '--dsn', required=True,
                        help='openGauss database target url. '
                             'It is recommended to connect to the postgres '
                             'database through this URL, so that the exporter '
                             'can actively discover and monitor other databases.',
                        action=CheckDSN)
    parser.add_argument('--config-file', '--config', type=path_type, default=os.path.join(YAML_DIR_PATH, DEFAULT_YAML),
                        help='path to config file.')
    parser.add_argument('--include-databases', action=ListPairAction, default='',
                        help='only scrape metrics from the given database list.'
                             ' a list of database name (format is label=dbname or dbname) separated by comma(,).')
    parser.add_argument('--exclude-databases', action=ListPairAction, default='',
                        help='scrape metrics from the all auto-discovered databases'
                             ' excluding the list of database.'
                             ' a list of database name (format is label=dbname or dbname) separated by comma(,).')
    parser.add_argument('--constant-labels', default='', action=KVPairAction,
                        help='a list of label=value separated by comma(,).')
    parser.add_argument('--scrape-interval-seconds', type=not_negative_int_type, default=0,
                        help='specify the scrape interval in seconds to reduce redundant results. '
                             'If set 0, it means automatically calculate.')
    parser.add_argument('--web.listen-address', default='127.0.0.1', action=CheckIP,
                        help='address on which to expose metrics and web interface')
    parser.add_argument('--web.listen-port', type=int, default=9187, action=CheckPort,
                        help='listen port to expose metrics and web interface')
    parser.add_argument('--disable-cache', action='store_true',
                        help='force not using cache.')
    parser.add_argument('--disable-settings-metrics', action='store_true',
                        help='not collect pg_settings.yml metrics.')
    parser.add_argument('--disable-statement-history-metrics', action='store_true',
                        help='not collect statement-history metrics (including slow queries).')
    parser.add_argument('--disable-https', action='store_true',
                        help='disable Https scheme')
    parser.add_argument('--disable-agent', action='store_true',
                        help='by default, this exporter also assumes the role of DBMind-Agent, that is, executing '
                             'database operation and maintenance actions issued by the DBMind service. With this '
                             'argument, users can disable the agent functionality, thereby prohibiting the DBMind '
                             'service from making changes to the database.')
    parser.add_argument('--ssl-keyfile', type=path_type, help='set the path of ssl key file')
    parser.add_argument('--ssl-certfile', type=path_type, help='set the path of ssl certificate file')
    parser.add_argument('--ssl-ca-file', type=path_type, help='set the path of ssl ca file')
    parser.add_argument('--parallel', default=5, type=positive_int_type,
                        help='number of parallels for metrics scrape.')
    parser.add_argument('--connection-pool-size', default=0, type=not_negative_int_type,
                        help='size of connection pool for each database. Set zero to disable connection pool.')
    parser.add_argument('--log.filepath', type=os.path.realpath,
                        default=os.path.join(os.getcwd(), DEFAULT_LOGFILE),
                        help='the path to log')
    parser.add_argument('--log.level', default='info', choices=('debug', 'info', 'warn', 'error', 'fatal'),
                        help='only log messages with the given severity or above.'
                             ' Valid levels: [debug, info, warn, error, fatal]')
    parser.add_argument('-v', '--version', action='version', version=__version__)

    args = parser.parse_args(argv)

    both_set_database = set(args.include_databases).intersection(args.exclude_databases)
    if both_set_database:
        parser.error('Not allowed to set the same database %s '
                     'in the argument --include-databases and '
                     '--exclude-database at the same time.' % both_set_database)

    return exporter_parse_and_adjust_ssl_args(parser, argv)


class ExporterMain(Daemon):
    def clean(self):
        pass

    def __init__(self, args):
        self.multi_connection = False
        conn_info = parse_dsn(args.url)
        if "password" not in conn_info:
            db_password = args.json.get('db-password')
            if not (db_password and isinstance(db_password, str)):
                raise ValueError("You should pass the password of database through the json db-password field, exit...")
            conn_info["password"] = db_password
            args.url = make_dsn(**conn_info)
        conn_info.clear()

        self.args = args
        cur_path = os.path.realpath(os.path.dirname(__file__))
        proj_path = cur_path[:cur_path.rfind('dbmind')]
        if (
            args.constant_labels and
            isinstance(args.constant_labels, dict) and
            'instance' in args.constant_labels.keys()
        ):
            constant_labels_instance = args.constant_labels['instance']
            self.pid_file = os.path.join(proj_path, 'opengauss_exporter_{}.pid'.format(constant_labels_instance))
            exporter_info_dict['constant_labels_instance'] = constant_labels_instance
        else:
            self.pid_file = os.path.join(proj_path, 'opengauss_exporter.pid')
        super().__init__(self.pid_file)

    def multi_connection_validation(self):
        # Check whether URL in the multi-node deployment mode are valid
        parsed_dsn = parse_dsn(self.args.url)
        host, port = parsed_dsn.get('host', '').split(','), parsed_dsn.get('port', '').split(',')
        if len(host) != len(port):
            raise ValueError('You should provide accurate URL information.')
        for item in host:
            if not check_ip_valid(item):
                raise ValueError('You should provide accurate URL information.')
        for item in port:
            if not check_port_valid(item):
                raise ValueError('You should provide accurate URL information.')
        if len(host) > MAX_NODE_NUMBER:
            raise ValueError('The number of addresses in the URL exceeds the limit.')
        if len(host) > 1:
            self.multi_connection = True

    def change_file_permissions(self):
        if file_exists(self.args.ssl_keyfile):
            os.chmod(self.args.ssl_keyfile, 0o400)
        if file_exists(self.args.ssl_certfile):
            os.chmod(self.args.ssl_certfile, 0o400)
        if file_exists(self.args.ssl_ca_file):
            os.chmod(self.args.ssl_ca_file, 0o400)
        if file_exists(self.args.__dict__['log.filepath']):
            os.chmod(self.args.__dict__['log.filepath'], 0o600)
        if file_exists(self.args.config_file):
            os.chmod(self.args.config_file, 0o600)
        if os.path.exists(self.pid_file):
            os.chmod(self.pid_file, 0o600)
        if os.path.exists(YAML_DIR_PATH):
            os.chmod(YAML_DIR_PATH, 0o700)
        if os.path.exists(os.path.join(YAML_DIR_PATH, PG_SETTINGS_YAML)):
            os.chmod(os.path.join(YAML_DIR_PATH, PG_SETTINGS_YAML), 0o600)
        if os.path.exists(os.path.join(YAML_DIR_PATH, DEFAULT_YAML)):
            os.chmod(os.path.join(YAML_DIR_PATH, DEFAULT_YAML), 0o600)
        if os.path.exists(os.path.join(YAML_DIR_PATH, COMING_FROM_EACH_DATABASE)):
            os.chmod(os.path.join(YAML_DIR_PATH, COMING_FROM_EACH_DATABASE), 0o600)
        if os.path.exists(os.path.join(YAML_DIR_PATH, STATEMENTS_YAML)):
            os.chmod(os.path.join(YAML_DIR_PATH, STATEMENTS_YAML), 0o600)

    def run(self):
        # Wipe off password of url for the process title and path of ssl certificate relative path.
        try:
            url = self.args.url
            wipe_off_sensitive_information_from_proc_title(url, wipe_off_dsn_password(url))
            ssl_certfile = self.args.ssl_certfile
            ssl_keyfile = self.args.ssl_keyfile
            ssl_ca_file = self.args.ssl_ca_file
            if ssl_certfile:
                wipe_off_sensitive_information_from_proc_title('--ssl-certfile', '******', wipe_argument=True)
            if ssl_keyfile:
                wipe_off_sensitive_information_from_proc_title('--ssl-keyfile', '******', wipe_argument=True)
            if ssl_ca_file:
                wipe_off_sensitive_information_from_proc_title('--ssl-ca-file', '******', wipe_argument=True)
        except FileNotFoundError:
            write_to_terminal('Failed to wipe off sensitive information, exiting...')
            if LINUX:
                sys.exit(1)

        set_logger(self.args.__dict__['log.filepath'],
                   self.args.__dict__['log.level'])
        self.change_file_permissions()
        self.multi_connection_validation()
        exporter_info_dict['logfile'] = self.args.__dict__['log.filepath']
        exporter_info_dict['ip'] = self.args.__dict__['web.listen_address']
        exporter_info_dict['port'] = self.args.__dict__['web.listen_port']
        try:
            service.config_collecting_params(
                url=self.args.url,
                include_databases=self.args.include_databases,
                exclude_databases=self.args.exclude_databases,
                parallel=self.args.parallel,
                connection_pool_size=self.args.connection_pool_size,
                disable_cache=self.args.disable_cache,
                constant_labels=self.args.constant_labels,
                scrape_interval_seconds=self.args.scrape_interval_seconds,
            )
        except ConnectionError:
            # We can not throw the exception details due to the default security policy.
            write_to_terminal('Failed to connect to the database using the url, exiting...', color='red')
            sys.exit(1)
        except Exception as e:
            write_to_terminal(
                'Failed to connect to the database using the url due to an exception %s, exiting...'
                % e.__class__.__name__,
                color='red'
            )
            raise e

        if not self.args.disable_settings_metrics:
            with open(os.path.join(YAML_DIR_PATH, PG_SETTINGS_YAML), errors='ignore') as fp:
                service.register_metrics(
                    yaml.safe_load(fp),
                    force_connection_db='postgres'
                )

        if not self.args.disable_statement_history_metrics:
            with open(os.path.join(YAML_DIR_PATH, STATEMENTS_YAML), errors='ignore') as fp:
                service.register_metrics(
                    yaml.safe_load(fp),
                    force_connection_db='postgres'
                )

        # Metrics in the following config file need to
        # be scraped from each discovered database.
        with open(os.path.join(YAML_DIR_PATH, COMING_FROM_EACH_DATABASE), errors='ignore') as fp:
            service.register_metrics(yaml.safe_load(fp))

        # It is enough to connect to the given database and scrape metrics for the following config.
        with open(self.args.config_file, errors='ignore') as fp:
            service.register_metrics(
                yaml.safe_load(fp),
                force_connection_db=service.driver.main_dbname
            )

        # Reuse Http service to serve RPC.
        disable_agent = self.args.disable_agent or self.multi_connection
        if not disable_agent:
            rpc = create_agent_rpc_service()
            controller.bind_rpc_service(rpc)

        warn_ssl_certificate(self.args.ssl_certfile, self.args.ssl_keyfile, self.args.ssl_ca_file)

        # All startup works are completed, then mark the global
        # exporter fixed information.
        service.register_exporter_fixed_info()
        url = (f"{'http' if self.args.disable_https else 'https'}://"
               f"{prepare_ip(self.args.__dict__['web.listen_address'])}:"
               f"{self.args.__dict__['web.listen_port']}")
        service.update_exporter_fixed_info('url', url)
        service.update_exporter_fixed_info('rpc', not disable_agent)
        service.update_exporter_fixed_info('dbname', service.driver.main_dbname)
        service.update_exporter_fixed_info('monitoring', service.driver.address)

        controller.run(
            host=self.args.__dict__['web.listen_address'],
            port=self.args.__dict__['web.listen_port'],
            telemetry_path='/metrics',
            ssl_keyfile=self.args.ssl_keyfile,
            ssl_certfile=self.args.ssl_certfile,
            ssl_keyfile_password=self.args.keyfile_password,
            ssl_ca_file=self.args.ssl_ca_file
        )


def main(argv):
    args = parse_argv(argv)
    if is_exporter_alive(args.__dict__['web.listen_address'],
                         args.__dict__['web.listen_port']):
        write_to_terminal('Service has been started or the address already in use, exiting...', color='red')
    else:
        ExporterMain(args).start()
    args.url = ''

