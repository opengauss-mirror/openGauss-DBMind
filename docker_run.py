#!/bin/env python3

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

"""This file allows the user to use the
entire DBMind without many installations."""

import configparser
import os
import sys
import time
from types import SimpleNamespace

import requests
from prometheus_client.parser import text_string_to_metric_families

from dbmind import constants
from dbmind.cmd import setup
from dbmind.cmd.config_utils import ConfigUpdater
from dbmind.common.parser.others import parse_dsn
from dbmind.components.deployment.prometheus_deploy import PROMETHEUS, SSL, EXPORTERS
from dbmind.components.deployment.prometheus_deploy import edit_prometheus_yaml

OPENGAUSS_DSNS = os.getenv('OPENGAUSS_DSNS')
CMD_EXPORTERS = os.getenv('CMD_EXPORTERS')
NODE_EXPORTERS = os.getenv('NODE_EXPORTERS')
METADATABASE = os.getenv('METADATABASE')
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', 15))

# fixed path
PATHS_CONFIG = '/etc'  # can be modified
DBMIND_PATHS_CONFIG = '{}/dbmind'.format(PATHS_CONFIG)
PROMETHEUS_PATHS_CONFIG = '{}/prometheus/prometheus.yml'.format(PATHS_CONFIG)
GF_PATHS_CONFIG = '{}/grafana/grafana.ini'.format(PATHS_CONFIG)

PATHS_DATA = '/data'  # can be modified
DBMIND_PATHS_DATA = '{}/dbmind'.format(PATHS_DATA)
PROMETHEUS_PATHS_DATA = '{}/prometheus'.format(PATHS_DATA)
GF_PATHS_DATA = '{}/grafana'.format(PATHS_DATA)

PATHS_LOG = '/log'  # can be modified
DBMIND_PATHS_LOGS = '{}/dbmind'.format(PATHS_LOG)
DBMIND_EXPORTER_PATHS_LOGS = '{}/exporter'.format(DBMIND_PATHS_LOGS)
PROMETHEUS_PATHS_LOG = '{}/prometheus'.format(PATHS_LOG)
GF_PATHS_LOGS = '{}/grafana'.format(PATHS_LOG)

# dynamically update
g_agent = None


def notify(msg):
    print(msg, file=sys.stdout)


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def execute(cmd):
    if 'opengauss_exporter' in cmd:
        # Avoid printing some sensitive information from command line.
        print('Starting opengauss_exporter...')
    else:
        print('Starting cmd: ' + cmd)
    return os.system(cmd)


def split_with_comma(s):
    if not s:
        return []
    segments = s.split(',')
    return list(map(str.strip, segments))


def strip_scheme(urls):
    rv = []
    for url in urls:
        if url.startswith('https://'):
            die('Not supported https yet for ' + url)
        if url.startswith('http://'):
            url = url[len('http://'):]
        url = url.strip('/')
        if ':' not in url:
            die('Require fixed port in the url ' + url)
        rv.append(url)
    return rv


def look_for_master_exporter(targets, dsn_list):
    """Currently, only supports one agent."""
    NODE_INFO_METRIC = 'pg_node_info_uptime'
    NODE_INFO_IS_SLAVE_LABEL = 'is_slave'
    NODE_INFO_NON_SLAVE_FLAG = 'N'

    for i, target in enumerate(targets):
        url = 'http://{}'.format(target)
        r = requests.get(url + '/metrics')
        if r.status_code != 200:
            continue
        for family in text_string_to_metric_families(r.text):
            if family.name != NODE_INFO_METRIC:
                continue
            for sample in family.samples:
                if sample.labels[NODE_INFO_IS_SLAVE_LABEL] != NODE_INFO_NON_SLAVE_FLAG:
                    # The index is consistent with dsn_list.
                    dsn = parse_dsn(dsn_list[i])
                    return SimpleNamespace(
                        url=url, username=dsn['user'], password=dsn['password']
                    )

    die('Not found a master node from given DSN.')


class PortAllocator:
    START_PORT = 10000

    LABEL_OPENGAUSS_EXPORTER = 'opengauss_exporter'
    LABEL_REPROCESSING_EXPORTER = 'reprocessing_exporter'
    LABEL_CMD_EXPORTER = 'cmd_exporter'

    def __init__(self):
        self.current = PortAllocator.START_PORT
        self.labels = {}

    def allocate(self, label):
        # non-thread-safety
        # Don't need to check if the port is occupied
        # because we allocate the port range from 10000.
        self.current += 1
        if label not in self.labels:
            self.labels[label] = []
        self.labels[label].append(self.current)
        return self.current

    def ports(self, label):
        return self.labels.get(label, [])


port_allocator = PortAllocator()


def start_opengauss_exporters(dsn_list):
    cmd = (
        './gs_dbmind component opengauss_exporter '
        '--url {dsn} '
        '--web.listen-address 0.0.0.0 '  # not exposed
        '--web.listen-port {port} '
        '--disable-http --scrape-interval-seconds {scrape_interval} '
        '--log.filepath {log_path}/og_exporter_{log_suffix}.log '
        '--log.level error'
    )

    targets = []
    for dsn in dsn_list:
        parsed = parse_dsn(dsn)
        listen_port = port_allocator.allocate(
            port_allocator.LABEL_OPENGAUSS_EXPORTER
        )
        execute(
            cmd.format(
                dsn=dsn,
                port=listen_port,
                scrape_interval=SCRAPE_INTERVAL,
                log_path=DBMIND_EXPORTER_PATHS_LOGS,
                log_suffix='%s_%s_%s' % (listen_port, parsed['host'], parsed['port'])
            )
        )
        targets.append('127.0.0.1:{}'.format(listen_port))

    return targets


def start_reprocessing_exporter():
    # fixed port: 8181
    cmd = (
        './gs_dbmind component reprocessing_exporter '
        '127.0.0.1 9090 '
        '--web.listen-address 0.0.0.0 '
        '--web.listen-port 8181 '
        '--disable-https '
        '--log.filepath {log_path}/reprocessing_exporter.log '
        '--log.level error'
    ).format(
        log_path=DBMIND_EXPORTER_PATHS_LOGS
    )
    execute(cmd)


def start_cmd_exporters():
    pass


def start_prometheus(opengauss_exporter_targets):
    deploy_config = configparser.ConfigParser()
    deploy_config.add_section(PROMETHEUS)
    deploy_config.add_section(SSL)
    deploy_config.add_section(EXPORTERS)
    deploy_config.set(PROMETHEUS, 'host', '127.0.0.1')
    deploy_config.set(PROMETHEUS, 'prometheus_port', '9090')
    deploy_config.set(PROMETHEUS, 'reprocessing_exporter_port', '8181')
    deploy_config.set(SSL, 'enable_ssl', 'false')

    def edit(yaml_obj):
        # unified unit: second
        yaml_obj['global']['scrape_interval'] = '%ds' % SCRAPE_INTERVAL
        return yaml_obj

    edit_prometheus_yaml(
        yaml_path=PROMETHEUS_PATHS_CONFIG,
        configs=deploy_config,
        opengauss_exporter_targets=opengauss_exporter_targets,
        node_exporter_targets=strip_scheme(split_with_comma(NODE_EXPORTERS)),
        cmd_exporter_targets=strip_scheme(split_with_comma(CMD_EXPORTERS)),
        additionally_edit=edit
    )

    if not os.path.exists(PROMETHEUS_PATHS_LOG):
        os.makedirs(PROMETHEUS_PATHS_LOG, exist_ok=True)

    PROMETHEUS_BIN = '/bin/prometheus'
    cmd = (
        'nohup {bin} '
        '--config.file={etc} '
        '--storage.tsdb.path={data} '
        '--web.console.libraries=/usr/share/prometheus/console_libraries '
        '--web.console.templates=/usr/share/prometheus/consoles '
        '--storage.tsdb.retention.time=1w '  # one week
        '2>&1 >{log}/prometheus.log &'
    ).format(
        bin=PROMETHEUS_BIN,
        etc=PROMETHEUS_PATHS_CONFIG,
        data=PROMETHEUS_PATHS_DATA,
        log=PROMETHEUS_PATHS_LOG
    )

    execute(cmd)


def start_grafana():
    # load DBMind Grafana templates
    pass


def config_dbmind_service(confpath):
    setup.setup_directory(confpath)
    dbmind_conf = os.path.join(DBMIND_PATHS_CONFIG, constants.CONFILE_NAME)
    with ConfigUpdater(dbmind_conf) as config:
        # TSDB
        config.set('TSDB', 'name', 'prometheus')
        config.set('TSDB', 'host', '127.0.0.1')
        config.set('TSDB', 'port', '9090')

        if METADATABASE:
            dsn = parse_dsn(METADATABASE)
            config.set('METADATABASE', 'dbtype', 'opengauss')
            config.set('METADATABASE', 'host', dsn['host'])
            config.set('METADATABASE', 'port', dsn['port'])
            config.set('METADATABASE', 'username', dsn['user'])
            config.set('METADATABASE', 'password', dsn['password'])
            config.set('METADATABASE', 'database', dsn['dbname'])
        else:
            # default to use sqlite
            config.set('METADATABASE', 'dbtype', 'sqlite')
            config.set('METADATABASE', 'database', '{}/metadatabase.db'.format(DBMIND_PATHS_DATA))

        # AGENT
        config.set('AGENT', 'master_url', g_agent.url)
        config.set('AGENT', 'username', g_agent.username)
        config.set('AGENT', 'password', g_agent.password)

        # WEB-SERVICE
        config.set('WEB-SERVICE', 'host', '0.0.0.0')
        config.set('WEB-SERVICE', 'port', '8080')

        config.set('LOG', 'log_directory', DBMIND_PATHS_LOGS)

    # override existence data for meta-database
    setup.initialize_and_check_config(confpath, interactive=False, quiet=True)


def start_dbmind_service():
    # Ensure that the DBMind config directory exists.
    old_cwd = os.getcwd()
    if not os.path.exists(DBMIND_PATHS_DATA):
        os.makedirs(DBMIND_PATHS_DATA)

    # Not existing and existing but empty directory are both ok.
    if (not os.path.exists(DBMIND_PATHS_CONFIG) 
            or len(os.listdir(DBMIND_PATHS_CONFIG)) == 0):
        config_dbmind_service(DBMIND_PATHS_CONFIG)

    # config_dbmind_service() will change cwd, we change it back.
    os.chdir(old_cwd)
    cmd = './gs_dbmind service start -c {}'.format(DBMIND_PATHS_CONFIG)
    exitcode = execute(cmd)
    if exitcode:
        die('Failed to start DBMind service (exitcode %d). Please check for'
            'configurations.' % exitcode)


def check_all_ports():
    # Check for fixed ports
    # Check for dynamic ports
    pass


def block():
    while True:
        time.sleep(10)


def main_procedure():
    if not OPENGAUSS_DSNS:
        die('Need to set environment variable OPENGAUSS_DSNS.')
    if not NODE_EXPORTERS:
        die('Need to set environment variable NODE_EXPORTERS.')
    if not os.path.exists(PATHS_DATA):
        die('Need to bind mount a volume for ' + PATHS_DATA)
    if not os.path.exists(PATHS_CONFIG):
        die('Need to bind mount a volume for ' + PATHS_CONFIG)
    if not os.path.exists(PATHS_LOG):
        die('Need to bind mount a volume for ' + PATHS_LOG)

    os.putenv('DBMIND_USE_DAEMON', '1')  # allow DBMind to use backend process
    os.putenv('OPENBLAS_NUM_THREADS', '1')  # for openBLAS, numpy

    notify('Starting openGauss exporters using OPENGAUSS_DSNS...')
    dsn_list = split_with_comma(OPENGAUSS_DSNS)
    opengauss_exporter_targets = start_opengauss_exporters(dsn_list)

    global g_agent
    g_agent = look_for_master_exporter(opengauss_exporter_targets, dsn_list)

    notify('Starting cmd exporters...')
    start_cmd_exporters()

    notify('Starting configuring Prometheus...')
    start_prometheus(opengauss_exporter_targets)

    notify('Starting reprocessing exporter...')
    start_reprocessing_exporter()

    notify('Starting Grafana...')
    start_grafana()

    start_dbmind_service()

    check_all_ports()

    # Hang here to prevent docker from exiting.
    notify('All tasks are starting.')
    try:
        block()
    except KeyboardInterrupt:
        die('Terminated.')


if __name__ == '__main__':
    main_procedure()

