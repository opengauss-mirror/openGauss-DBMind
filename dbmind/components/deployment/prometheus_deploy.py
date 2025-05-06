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

import argparse
import getpass
import glob
import os
import platform
import re
import shlex
import socket
import sys
import time
from collections import defaultdict
from psycopg2.extensions import make_dsn
from socket import getaddrinfo, gethostname
from typing import Callable

import requests
import yaml
from pexpect import ExceptionPexpect
from prettytable import PrettyTable
from requests.adapters import HTTPAdapter
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from dbmind.common.cmd_executor import SSH, SFTP, transfer_pool
from dbmind.common.utils import dbmind_assert
from dbmind.common.utils.base import RED_FMT, GREEN_FMT
from dbmind.common.utils.checking import path_type, prepare_ip, split_ip_port
from dbmind.common.utils.cli import keep_inputting_until_correct
from dbmind.constants import __version__
from .utils import ConfigParser
from .utils import (
    unzip,
    checksum_sha256,
    download_file,
    download_sha256,
    check_config_validity,
    convert_full_width_character_to_half_width,
    parse_ip_info_from_string,
    validate_ssh_connection,
    validate_database_connection
)
from ...common.utils.exporter import set_logger

disable_warnings(InsecureRequestWarning)

DOWNLOADING = 'DOWNLOADING'
PROMETHEUS = 'PROMETHEUS'
EXPORTERS = 'EXPORTERS'
SSL = 'SSL'
DATABASE = 'DATABASE'

SKIP_LIST = [DOWNLOADING]

SCRIPT_PATH = os.path.split(os.path.realpath(__file__))[0]
CONFIG_PATH = os.path.join(SCRIPT_PATH, 'deploy.conf')
DBMIND_PATH = os.path.realpath(os.path.join(SCRIPT_PATH, '../../../'))
EXTRACT_PATH = os.path.realpath(os.path.expanduser('~'))
os.makedirs(EXTRACT_PATH, exist_ok=True)

sha256_checksum = {}

CONFIG_OPTIONS = {
    PROMETHEUS + '-host': '127.0.0.1',
    PROMETHEUS + '-ssh_port': '22',
    PROMETHEUS + '-prometheus_port': '9090',
    PROMETHEUS + '-reprocessing_exporter_port': '8181',
    EXPORTERS + '-ssh_port': '22',
    EXPORTERS + '-opengauss_ports_range': '9187-9197',
    EXPORTERS + '-node_exporter_port': '9100',
    EXPORTERS + '-cmd_exporter_port': '9180',
}

PWD = {}

WAITING_CMD = []
BACKEND_CMD = []

CERT_PERMISSION = '400'
LOG_PERMISSION = '600'
DBMIND_PERMISSION = '400'
FILE_PERMISSION = '600'
DIR_PERMISSION = '700'

LOCALHOSTS = set([h[4][0] for h in getaddrinfo(gethostname(), None, family=socket.AF_INET)] + ['127.0.0.1'])
ARCHITECTURES = {
    'x86_64': 'amd64',
    'aarch64': 'arm64',
}

N_TRANSPORTERS = 4


def has_conflict(host, port, ports_occupied, option, section):
    hosts = [host] if isinstance(host, str) else host
    ports_out = f'{port[0]}-{port[-1]}' if isinstance(port, list) and len(port) > 1 else port
    ports = [port] if isinstance(port, str) else port
    for h in hosts:
        k = 'localhost' if h in LOCALHOSTS else h
        for p in ports:
            if p in ports_occupied[k]:
                print(f"{section}-{option}: {ports_out} has conflict with ports of {h}, you need another port.")
                return True

    return False


def add_port(host, port, ports_occupied):
    hosts = [host] if isinstance(host, str) else host
    ports = [port] if isinstance(port, str) else port
    for h in hosts:
        k = 'localhost' if h in LOCALHOSTS else h
        for p in ports:
            ports_occupied[k].add(p)


def config_ports_has_conflict(configs):
    ports_occupied = defaultdict(set)
    host = configs.get(PROMETHEUS, 'host').strip()
    for section in configs.sections():
        for option, _ in configs.items(section):
            value = configs.get(section, option).strip()
            if section in [PROMETHEUS, EXPORTERS] and not value:
                print(
                    f"It seems the config file is unfinished with the empty {section}-{option}."
                    "Maybe you should deploy with '--online' or '--offline' first."
                )
                return True

            if 'ssh_port' in option:
                continue

            elif '_ports' in option:
                ports_range = parse_ip_info_from_string(value)
                if not has_conflict(host, ports_range, ports_occupied, option, section):
                    add_port(host, ports_range, ports_occupied)
                else:
                    return True

            elif '_port' in option:
                port = value.strip()
                if not has_conflict(host, port, ports_occupied, option, section):
                    add_port(host, port, ports_occupied)
                else:
                    return True

            elif 'targets' in option:
                urls = parse_ip_info_from_string(value)
                host = list()
                for url in urls:
                    host_port = url.split('/', 1)[0].strip()
                    h, p = map(str.strip, split_ip_port(host_port))
                    if has_conflict(h, p, ports_occupied, option, section):
                        return True

                    host.append(h)

                for url in urls:
                    host_port = url.split('/', 1)[0].strip()
                    h, p = map(str.strip, split_ip_port(host_port))
                    add_port(h, p, ports_occupied)

    return False


def passwd_input(key, configs):
    validate = validate_ssh_connection
    hosts = []
    if key == PROMETHEUS:
        username = configs.get(PROMETHEUS, 'host_username')
        port = configs.get(PROMETHEUS, 'ssh_port')
        dbmind_assert(
            username.strip() and port.strip(),
            "Empty usrname or port. You should checkout the config file."
        )
        hosts = [f"{username}@{prepare_ip(configs.get(key, 'host'))}:{port}"]
    elif key == EXPORTERS:
        username = configs.get(EXPORTERS, 'host_username')
        port = configs.get(EXPORTERS, 'ssh_port')
        targets = configs.get(EXPORTERS, 'targets')
        dbmind_assert(
            username.strip() and port.strip() and targets.strip(),
            "Empty usrname, port or address. You should checkout the config file."
        )
        hosts = list(set([f"{username}@{prepare_ip(split_ip_port(s)[0])}:{port}"
                          for s in map(str.strip, targets.split(','))]))
    elif key == DATABASE:
        username = configs.get(EXPORTERS, 'database_username')
        targets = configs.get(EXPORTERS, 'targets')
        dbmind_assert(
            username.strip() and targets.strip(),
            "Empty usrname or address. You should checkout the config file."
        )
        hosts = list(set([username + '@' + s for s in map(str.strip, targets.split(','))]))
        validate = validate_database_connection

    valid_pwd = False
    while not valid_pwd:
        PWD[key] = getpass.getpass(f"Input the password for {hosts}: ")
        if all((validate(PWD[key], *re.split('[:/@]', host)) for host in hosts)):
            valid_pwd = True
        else:
            choice = keep_inputting_until_correct(
                f"Invalid username, password or address for {hosts}. "
                "Are you sure you want to retype the above information?(y/n).", ('Y', 'N')
            )
            if choice == 'N':
                valid_pwd = True


def db_exporters_parsing(configs):
    urls = parse_ip_info_from_string(configs.get(EXPORTERS, 'targets'))
    opengauss_ports_range = parse_ip_info_from_string(configs.get(EXPORTERS, 'opengauss_ports_range'))

    exporters = {}
    for url in urls:
        host_port = url.split('/')[0].strip()
        host = split_ip_port(host_port)[0].strip()
        if host not in exporters:
            exporters[host] = defaultdict(list)

        if url in exporters[host]['db_instance']:
            print('WARNING: Found duplicated database-urls in configs.')
            continue

        for port in opengauss_ports_range:
            opengauss_exporter = f"{prepare_ip(host)}:{port}"
            if opengauss_exporter not in exporters[host]['opengauss_exporters']:
                exporters[host]['db_instance'].append(url)
                exporters[host]['opengauss_exporters'].append(opengauss_exporter)
                break

        else:
            raise ValueError('The opengauss exporter ports are used up.')

    return exporters


def set_deploy_config_interactive(configs):
    def user_input(s, o, v, m):
        input_value_ = ''
        while input_value_.strip() == '':
            message = GREEN_FMT.format(o) + ' {} [default: {}]:'.format(m, v)
            message += '\n'
            input_value_ = input(message)
            if input_value_.strip() == '':  # If user inputs nothing, set to default value.
                input_value_ = v

            input_value_ = convert_full_width_character_to_half_width(input_value_)
            valid, invalid_reason = check_config_validity(s, o, input_value_)
            if not valid:
                print("Please retype due to '%s'." % invalid_reason)
                input_value_ = ''

            elif 'ssh_port' in option:
                continue

            elif '_ports' in o:
                ports_range = parse_ip_info_from_string(input_value_)
                if not has_conflict(host, ports_range, ports_occupied, o, s):
                    add_port(host, ports_range, ports_occupied)
                else:
                    input_value_ = ''

            elif '_port' in o:
                port = input_value_.strip()
                if not has_conflict(host, port, ports_occupied, o, s):
                    add_port(host, port, ports_occupied)
                else:
                    input_value_ = ''

            elif 'targets' in o:
                urls = parse_ip_info_from_string(input_value_)
                for url in urls:
                    host_port = url.split('/', 1)[0].strip()
                    h, p = map(str.strip, split_ip_port(host_port))
                    if has_conflict(h, p, ports_occupied, o, s):
                        input_value_ = ''
                        break
                else:
                    for url in urls:
                        host_port = url.split('/', 1)[0].strip()
                        h, p = map(str.strip, split_ip_port(host_port))
                        add_port(h, p, ports_occupied)

        return input_value_.strip()

    host = ''
    ports_occupied = defaultdict(set)
    for section in configs.sections():
        if section in SKIP_LIST:
            continue

        for option, value_msg in configs.items(section):
            if section == SSL and \
                    option != 'enable_ssl' and \
                    configs.get(SSL, 'enable_ssl') != 'True':
                continue

            key = "%s-%s" % (section, option)
            default_value = ''
            if key in CONFIG_OPTIONS:
                default_value = CONFIG_OPTIONS.get(key)
            elif option == 'path':
                default_value = '/home/{}'.format(configs.get(section, 'host_username'))

            _, msg = map(str.strip, value_msg.rsplit('#', 1))
            if option.endswith(('_port', '_username', 'path')):
                input_value = user_input(section, option, default_value, '{} of {}.'.format(msg, host))
            else:
                input_value = user_input(section, option, default_value, msg)

            configs.set(section, option, '%s  # %s' % (input_value, msg))

            if section == PROMETHEUS and option == 'host_username':
                passwd_input(PROMETHEUS, configs)
            elif section == EXPORTERS and option == 'host_username':
                passwd_input(EXPORTERS, configs)
            elif section == PROMETHEUS and option == 'host':
                host = configs.get(PROMETHEUS, 'host')
            elif section == EXPORTERS and option == 'targets':
                host = list(set([split_ip_port(s)[0] for s in
                                 map(str.strip, configs.get(EXPORTERS, 'targets').split(','))]))

    with open(CONFIG_PATH, 'w') as f:  # write the config file
        configs.write(f)


def edit_prometheus_yaml(
        yaml_obj,
        configs,
        node_exporter_targets,
        cmd_exporter_targets,
        opengauss_exporter_targets,
        additionally_edit: Callable = None
):
    yaml_obj['scrape_configs'] = [
        {
            'job_name': 'prometheus',
            'static_configs': [
                {
                    'targets': [
                        f"{prepare_ip(configs.get(PROMETHEUS, 'host'))}:"
                        f"{configs.get(PROMETHEUS, 'prometheus_port')}"
                    ]
                }
            ],
        },
        {
            'job_name': 'reprocessing_exporter',
            'scheme': 'https' if configs.get(SSL, 'enable_ssl') == 'True' else 'http',
            'static_configs': [
                {
                    'targets': [
                        f"{prepare_ip(configs.get(PROMETHEUS, 'host'))}:"
                        f"{configs.get(PROMETHEUS, 'reprocessing_exporter_port')}"
                    ]
                }
            ],
        },
        {
            'job_name': 'node_exporter',
            'scheme': 'http',
            'static_configs': [
                {
                    'targets': []
                }
            ],
        },
        {
            'job_name': 'cmd_exporter',
            'scheme': 'https' if configs.get(SSL, 'enable_ssl') == 'True' else 'http',
            'static_configs': [
                {
                    'targets': []
                }
            ],
        },
        {
            'job_name': 'opengauss_exporter',
            'scheme': 'https' if configs.get(SSL, 'enable_ssl') == 'True' else 'http',
            'static_configs': [
                {
                    'targets': []
                }
            ],
        }
    ]

    for job in yaml_obj['scrape_configs']:
        if job['job_name'] == 'reprocessing_exporter' and job['scheme'] == 'https':
            job['tls_config'] = {
                'ca_file': configs.get(SSL, 'prometheus_ssl_ca_file'),
                'key_file': configs.get(SSL, 'prometheus_ssl_keyfile'),
                'cert_file': configs.get(SSL, 'prometheus_ssl_certfile'),
            }

    for job in yaml_obj['scrape_configs']:
        if job['job_name'] == 'node_exporter':
            for i, config in enumerate(job['static_configs']):
                if 'targets' in config:
                    job['static_configs'][i]['targets'].extend(node_exporter_targets)

        if job['job_name'] == 'cmd_exporter':
            for i, config in enumerate(job['static_configs']):
                if 'targets' in config:
                    job['static_configs'][i]['targets'].extend(cmd_exporter_targets)

            if job['scheme'] == 'https':
                job['tls_config'] = {
                    'ca_file': configs.get(SSL, 'prometheus_ssl_ca_file'),
                    'key_file': configs.get(SSL, 'prometheus_ssl_keyfile'),
                    'cert_file': configs.get(SSL, 'prometheus_ssl_certfile'),
                }

        if job['job_name'] == 'opengauss_exporter':
            for i, config in enumerate(job['static_configs']):
                if 'targets' in config:
                    job['static_configs'][i]['targets'].extend(opengauss_exporter_targets)

            if job['scheme'] == 'https':
                job['tls_config'] = {
                    'ca_file': configs.get(SSL, 'prometheus_ssl_ca_file'),
                    'key_file': configs.get(SSL, 'prometheus_ssl_keyfile'),
                    'cert_file': configs.get(SSL, 'prometheus_ssl_certfile'),
                }

    if additionally_edit:
        yaml_obj = additionally_edit(yaml_obj)

    return yaml_obj


def get_target_generator(exporters):
    def generate(port):
        return [f'{prepare_ip(host)}:{port}' for host in exporters]

    return generate


def deploy(configs, online=False, lite=None):
    """
    To deploy prometheus and exporters to the locations which are given in the config file.
    For once the prometheus and reprocessing exporter will be deployed at the same location.
    For multiple times the node exporters will be deployed according to the number of database hosts.
    For multiple times the opengauss exporters will be deployed according to the number of monitored
    database targets.
    """

    block_dict = {
        "dir": (
            glob.glob(os.path.join(EXTRACT_PATH, configs.get(DOWNLOADING, "prometheus"), "data")) +
            glob.glob(os.path.join(DBMIND_PATH, "**", ".*"), recursive=True) +
            glob.glob(os.path.join(DBMIND_PATH, "docs")) +
            glob.glob(os.path.join(DBMIND_PATH, "tests")) +
            glob.glob(os.path.join(DBMIND_PATH, "**", "__pycache__"), recursive=True)
        ),
        "file": (
            glob.glob(os.path.join(DBMIND_PATH, "**", ".gitignore"), recursive=True) +
            glob.glob(os.path.join(DBMIND_PATH, "**", "*.log"), recursive=True)
        )
    }

    def sftp_upload(ip, username, passwd, port, remote_dir, software):
        def upload_preparation(local, remote):
            sftp.mkdir(remote)
            for entry in os.scandir(local):
                local_path = os.path.join(local, entry.name)
                remote_path = os.path.join(remote, entry.name)
                if entry.is_file() and entry.path not in block_dict["file"]:
                    if ip in LOCALHOSTS and local_path == remote_path:
                        print(f'WARNING: Source: {local_path} and destination: {remote_path}'
                              ' are the same path from the same node.'
                              ' Transportation was skipped to avoid overwriting.')
                        continue
                    upload_list.append((local_path, remote_path))

                elif entry.is_dir() and entry.path not in block_dict["dir"]:
                    upload_preparation(entry.path, remote_path)

        with SFTP(ip, username, passwd, port=int(port)) as sftp:
            upload_list = list()
            sftp.mkdir(remote_dir)
            sftp.remote_executor.exec_command_sync(f"chmod u+xw -R {shlex.quote(remote_dir)}")
            if lite is None:
                upload_preparation(DBMIND_PATH, remote_dir)

            upload_preparation(os.path.join(EXTRACT_PATH, software),
                               os.path.join(remote_dir, software))

            transfer_pool(ip, username, passwd, upload_list, port=int(port), workers=N_TRANSPORTERS)

            dbmind_permission = list()
            dbmind_dir = os.path.join(remote_dir, "dbmind")
            dbmind_permission.append(f'find {shlex.quote(remote_dir)} -type d -exec chmod {DIR_PERMISSION} {{}} \;')
            dbmind_permission.append(f'find {shlex.quote(remote_dir)} -type f -exec chmod {FILE_PERMISSION} {{}} \;')
            dbmind_permission.append(f'find {shlex.quote(dbmind_dir)} -type f -exec chmod {DBMIND_PERMISSION} {{}} \;')
            dbmind_permission.append(f'chmod +x {shlex.quote(os.path.join(remote_dir, "gs_dbmind"))}')
            dbmind_permission.append(f'find {shlex.quote(remote_dir)} -maxdepth 1 -type d -name python*'
                                     f' -exec chmod -R +x {{}}/bin \;')

            sftp.remote_executor.exec_command_sync(dbmind_permission)

    download_path = os.path.join(EXTRACT_PATH, 'downloads')
    if not os.path.exists(download_path):
        os.mkdir(download_path)

    host = configs.get(DOWNLOADING, 'host')
    prometheus_file = configs.get(DOWNLOADING, 'prometheus') + '.tar.gz'
    node_exporter_file = configs.get(DOWNLOADING, 'node_exporter') + '.tar.gz'
    sha256_checksum[prometheus_file] = configs.get(DOWNLOADING, 'prometheus_sha256')
    sha256_checksum[node_exporter_file] = configs.get(DOWNLOADING, 'node_exporter_sha256')

    if online:
        download_sha256(prometheus_file, download_path, host, sha256_checksum)
        configs.set(DOWNLOADING, 'prometheus_sha256', sha256_checksum[prometheus_file])
        download_sha256(node_exporter_file, download_path, host, sha256_checksum)
        configs.set(DOWNLOADING, 'node_exporter_sha256', sha256_checksum[node_exporter_file])
        with open(CONFIG_PATH, 'w') as f:  # rewrite the config file
            configs.write(f)

        if not checksum_sha256(download_path, prometheus_file, sha256_checksum):
            download_file(prometheus_file, download_path, host)
        else:
            print("{} already downloaded and it's integral.".format(prometheus_file))

        if not checksum_sha256(download_path, node_exporter_file, sha256_checksum):
            download_file(node_exporter_file, download_path, host)
        else:
            print("{} already downloaded and it's integral.".format(node_exporter_file))

    prometheus_exists = checksum_sha256(download_path, prometheus_file, sha256_checksum)
    node_exporter_exists = checksum_sha256(download_path, node_exporter_file, sha256_checksum)
    if not (prometheus_exists and node_exporter_exists):
        print("Prometheus or node_exporter 'tar.gz' file doesn't exist.")
        prometheus_ready, node_exporter_ready = False, False
    else:
        prometheus_ready = unzip(download_path, prometheus_file, EXTRACT_PATH)
        node_exporter_ready = unzip(download_path, node_exporter_file, EXTRACT_PATH)

    if prometheus_ready and node_exporter_ready:
        yaml_path = os.path.join(EXTRACT_PATH, configs.get(DOWNLOADING, 'prometheus'), 'prometheus.yml')
        print('Deployment finished, you can find the "prometheus.yml" file at {}'.format(yaml_path))
        exporters = db_exporters_parsing(configs)
        generate_targets = get_target_generator(exporters)
        node_exporter_targets = generate_targets(configs.get(EXPORTERS, 'node_exporter_port'))
        cmd_exporter_targets = generate_targets(configs.get(EXPORTERS, 'cmd_exporter_port'))
        opengauss_exporter_targets = sum([[t for t in exporters[host]['opengauss_exporters']]
                                          for host in exporters], [])

        with open(yaml_path, 'r', encoding='utf-8') as f:
            yaml_obj = yaml.safe_load(f.read())

        new_yaml_obj = edit_prometheus_yaml(
            yaml_obj,
            configs,
            node_exporter_targets=node_exporter_targets,
            cmd_exporter_targets=cmd_exporter_targets,
            opengauss_exporter_targets=opengauss_exporter_targets
        )  # edit the prometheus config file

        with open(yaml_path, 'w') as f:
            print('Initiating Prometheus config file.')
            yaml.dump(new_yaml_obj, f)

        if lite is None or lite == 'all' or lite == 'prometheus':
            sftp_upload(
                configs.get(PROMETHEUS, 'host'),
                configs.get(PROMETHEUS, 'host_username'),
                PWD[PROMETHEUS],
                configs.get(PROMETHEUS, 'ssh_port'),
                configs.get(PROMETHEUS, 'path'),
                configs.get(DOWNLOADING, "prometheus")
            )

        if lite is None or lite == 'all' or lite == 'node_exporter':
            for host in exporters:
                sftp_upload(
                    host,
                    configs.get(EXPORTERS, 'host_username'),
                    PWD[EXPORTERS],
                    configs.get(EXPORTERS, 'ssh_port'),
                    configs.get(EXPORTERS, 'path'),
                    configs.get(DOWNLOADING, "node_exporter")
                )

        print("You can run the Prometheus and exporters by using: 'gs_dbmind component deployment --run'.")
    else:
        if lite == 'node_exporter':
            node_exporter_path = '/tmp/node_exporter'
            if node_exporter_ready:
                local_dir = os.path.join(EXTRACT_PATH, configs.get(DOWNLOADING, "node_exporter"))
                if not os.path.exists(local_dir):
                    os.mkdir(local_dir)
                cmd = 'cp {} {}'.format(shlex.quote(node_exporter_path), shlex.quote(local_dir))
                os.system(cmd)
                exporters = db_exporters_parsing(configs)
                for target in exporters:
                    sftp_upload(
                        target,
                        configs.get(EXPORTERS, 'host_username'),
                        PWD[EXPORTERS],
                        configs.get(EXPORTERS, 'ssh_port'),
                        configs.get(EXPORTERS, 'path'),
                        configs.get(DOWNLOADING, "node_exporter")
                    )
                print('node_exporter successfully deployed...')
                print("You can run the node_exporter by using: 'gs_dbmind component deployment --run'.")
            else:
                print('node_exporter does not exist...')
                print('Deployment unfinished')
        else:
            print('Deployment unfinished')


def generate_tasks(configs):
    """
    Generate the task executors and task cmds, extract the details for status check from configs.
    returns:
    tasks: dict. tasks will classify the cmds by different executors as the executors are the keys
    of tasks. The remote executors differs by different hosts.
    """

    def cert_permission(x):
        tasks[x].append(f'chmod {CERT_PERMISSION} {shlex.quote(configs.get(SSL, "prometheus_ssl_keyfile"))}')
        tasks[x].append(f'chmod {CERT_PERMISSION} {shlex.quote(configs.get(SSL, "prometheus_ssl_ca_file"))}')
        tasks[x].append(f'chmod {CERT_PERMISSION} {shlex.quote(configs.get(SSL, "prometheus_ssl_certfile"))}')
        tasks[x].append(f'chmod {CERT_PERMISSION} {shlex.quote(configs.get(SSL, "exporter_ssl_keyfile"))}')
        tasks[x].append(f'chmod {CERT_PERMISSION} {shlex.quote(configs.get(SSL, "exporter_ssl_ca_file"))}')
        tasks[x].append(f'chmod {CERT_PERMISSION} {shlex.quote(configs.get(SSL, "exporter_ssl_certfile"))}')

    tasks = defaultdict(list)
    if configs.get(SSL, 'enable_ssl') == "True":
        ssl = '--ssl-keyfile {} --ssl-certfile {} --ssl-ca-file {}'.format(
            shlex.quote(configs.get(SSL, 'exporter_ssl_keyfile')),
            shlex.quote(configs.get(SSL, 'exporter_ssl_certfile')),
            shlex.quote(configs.get(SSL, 'exporter_ssl_ca_file')),
        )
    else:
        ssl = '--disable-https'

    host = configs.get(PROMETHEUS, 'host')
    username = configs.get(PROMETHEUS, 'host_username')
    port = configs.get(PROMETHEUS, 'ssh_port')
    path = configs.get(PROMETHEUS, 'path')
    listen_address = configs.get(PROMETHEUS, 'listen_address')
    prometheus_port = configs.get(PROMETHEUS, 'prometheus_port')
    reprocessing_exporter_port = configs.get(PROMETHEUS, 'reprocessing_exporter_port')
    try:
        ssh = SSH(host, username, PWD[PROMETHEUS], port=int(port))
        executor = ssh
    except ExceptionPexpect:
        raise ExceptionPexpect(f"Invalid usrname, passwd or address for {username}@{prepare_ip(host)}:{port}")

    prometheus_path = os.path.join(path, configs.get(DOWNLOADING, 'prometheus'))

    # Authorize to make the file executable
    tasks[executor].append(f'chmod +x {shlex.quote(os.path.join(prometheus_path, "prometheus"))}')
    if configs.get(SSL, 'enable_ssl') == "True":
        cert_permission(executor)

    # prometheus
    prometheus_listen = f"--web.listen-address={shlex.quote(prepare_ip(listen_address))}:{prometheus_port}"
    prometheus_conf = f"--config.file {shlex.quote(os.path.join(prometheus_path, 'prometheus.yml'))}"
    prometheus_web = "--web.enable-admin-api"
    prometheus_retention_time = "--storage.tsdb.retention.time=1w"
    prometheus_log_dir = f">{shlex.quote(os.path.join(path, 'prometheus.log'))}"
    prometheus_cmd = ' '.join([
        os.path.join(prometheus_path, 'prometheus'),
        prometheus_listen,
        prometheus_conf,
        prometheus_web,
        prometheus_retention_time,
        prometheus_log_dir
    ])
    tasks[executor].append([prometheus_cmd, '', f'Prometheus of {host}'])
    WAITING_CMD.append(prometheus_cmd)
    BACKEND_CMD.append(prometheus_cmd)
    # reprocessing exporter
    reprocessing_exporter_web = '{} {}'.format(shlex.quote(str(host)), shlex.quote(str(prometheus_port)))
    reprocessing_exporter_listen = '--web.listen-address {} --web.listen-port {}'.format(
        shlex.quote(listen_address),
        reprocessing_exporter_port
    )
    dbmind_path = shlex.quote(os.path.join(path, 'gs_dbmind'))
    reprocessing_exporter_cmd = ' '.join([
        dbmind_path,
        'component',
        'reprocessing_exporter',
        reprocessing_exporter_web,
        reprocessing_exporter_listen,
        ssl
    ])
    tasks[executor].append([
        reprocessing_exporter_cmd,
        f'reprocessing-exporter of {host} has been started or the address already in use.',
        f'reprocessing-exporter of {host}'
    ])
    tasks[executor].append(f'chmod {LOG_PERMISSION} {shlex.quote(os.path.join(path, "prometheus.log"))}')
    # node_exporters, cmd_exporters and opengauss_exporters
    exporters = db_exporters_parsing(configs)
    username = configs.get(EXPORTERS, 'host_username')
    port = configs.get(EXPORTERS, 'ssh_port')
    path = configs.get(EXPORTERS, 'path')
    listen_address = configs.get(EXPORTERS, 'listen_address').strip()
    node_exporter_port = configs.get(EXPORTERS, 'node_exporter_port')
    cmd_exporter_port = configs.get(EXPORTERS, 'cmd_exporter_port')
    for host in exporters:
        try:
            db_ssh = SSH(host, username, PWD[EXPORTERS], port=int(port))
            executor = db_ssh
        except ExceptionPexpect:
            raise ExceptionPexpect(f"Invalid usrname, passwd or address for {username}@{prepare_ip(host)}:{port}")

        node_exporter_path = os.path.join(path, configs.get(DOWNLOADING, 'node_exporter'))

        # Authorize to make the file executable
        tasks[executor].append(f'chmod +x {shlex.quote(os.path.join(node_exporter_path, "node_exporter"))}')
        if configs.get(SSL, 'enable_ssl') == "True":
            cert_permission(executor)

        # node exporter
        node_exporter_listen = '--web.listen-address=:{}'.format(shlex.quote(str(node_exporter_port)))
        node_exporter_cmd = ' '.join([
            os.path.join(node_exporter_path, 'node_exporter'),
            node_exporter_listen
        ])
        tasks[executor].append([node_exporter_cmd, '', f'node-exporter of {host}'])
        BACKEND_CMD.append(node_exporter_cmd)
        # cmd exporter
        cmd_exporter_listen = '--web.listen-address {} --web.listen-port {}'.format(
            shlex.quote(str(listen_address)) if listen_address == '0.0.0.0' else shlex.quote(host),
            shlex.quote(str(cmd_exporter_port))
        )
        dbmind_path = shlex.quote(os.path.join(path, 'gs_dbmind'))
        cmd_exporter_cmd = ' '.join([
            dbmind_path,
            'component',
            'cmd_exporter',
            cmd_exporter_listen,
            ssl
        ])
        tasks[executor].append([
            cmd_exporter_cmd,
            f'cmd-exporter of {host} has been started or the address already in use.',
            f'cmd-exporter of {host}'
        ])
        # openGauss exporter
        db_username = configs.get(EXPORTERS, 'database_username')
        for i, db_instance in enumerate(exporters[host]['db_instance']):
            opengauss_exporter = exporters[host]['opengauss_exporters'][i]
            opengauss_exporter_listen = '--web.listen-address {} --web.listen-port {}'.format(
                shlex.quote(str(listen_address)) if listen_address == '0.0.0.0' else shlex.quote(host),
                shlex.quote(str(split_ip_port(opengauss_exporter)[1])),
            )
            dsn = make_dsn('postgresql://{}'.format(db_instance), user=db_username, password=PWD[DATABASE])
            opengauss_exporter_cmd = ' '.join([
                dbmind_path,
                'component',
                'opengauss_exporter',
                '--dsn',
                shlex.quote(dsn),
                opengauss_exporter_listen,
                ssl
            ])
            tasks[executor].append([
                opengauss_exporter_cmd,
                f'opengauss-exporter of {opengauss_exporter} has been started or the address already in use.',
                f'opengauss-exporter of {opengauss_exporter}'
            ])

    return tasks


def run(tasks):
    print('Starting the Prometheus and exporters.')
    for executor, cmds in tasks.items():
        for cmd in cmds:
            stdout, stderr = '', ''
            if isinstance(cmd, list):
                print(f'Starting {cmd[2]}')
                if cmd[0] in BACKEND_CMD:
                    _, _ = executor.exec_command_sync("nohup " + cmd[0], redirection=" 2>&1 &")
                else:
                    stdout, stderr = executor.exec_command_sync(cmd[0])
                    if 'address already in use' in stdout:
                        print(cmd[1])
                        continue

                if cmd[0] in WAITING_CMD:
                    print('Waiting for cmd to fully start.')
                    time.sleep(30)

            else:
                stdout, stderr = executor.exec_command_sync(cmd)

            if stderr:
                print(stderr)
            elif stdout:
                print(stdout)

        executor.close()


def generate_checks(configs):
    """
    Generate the urls to checks the condition of Prometheus and the exporters.
    returns:
    checks: list. Checks will gather the information of prometheus and exporters for the status check.
    """

    checks = []
    # prometheus
    host = configs.get(PROMETHEUS, 'host')
    listen_address = configs.get(PROMETHEUS, 'listen_address')
    prometheus_port = configs.get(PROMETHEUS, 'prometheus_port')
    reprocessing_exporter_port = configs.get(PROMETHEUS, 'reprocessing_exporter_port')
    prometheus_url = f"{prepare_ip(host)}:{prometheus_port}/api/v1/query?query=up"
    checks.append({
        'url': prometheus_url,
        'type': 'prometheus-server',
        'address': f"{prepare_ip(host)}:{prometheus_port}",
        'listen': f"{prepare_ip(listen_address)}:{prometheus_port}",
        'target': '-',
        'status': 'Down'
    })
    # reprocessing exporter
    reprocessing_exporter_url = f"{prepare_ip(host)}:{reprocessing_exporter_port}/metrics"
    checks.append({
        'url': reprocessing_exporter_url,
        'type': 'reprocessing-exporter',
        'address': f"{prepare_ip(host)}:{reprocessing_exporter_port}",
        'listen': f"{prepare_ip(listen_address)}:{reprocessing_exporter_port}",
        'target': f"{prepare_ip(host)}:{prometheus_port}",
        'status': 'Down'
    })
    # node_exporters, cmd_exporters and opengauss_exporters
    exporters = db_exporters_parsing(configs)
    listen_address = configs.get(EXPORTERS, 'listen_address').strip()
    node_exporter_port = configs.get(EXPORTERS, 'node_exporter_port')
    cmd_exporter_port = configs.get(EXPORTERS, 'cmd_exporter_port')
    for host in exporters:
        # node exporter
        listen = listen_address if listen_address == '0.0.0.0' else host
        node_exporter_url = f"{prepare_ip(host)}:{node_exporter_port}/metrics"
        checks.append({
            'url': node_exporter_url,
            'type': 'node-exporter',
            'address': f"{prepare_ip(host)}:{node_exporter_port}",
            'listen': f"{prepare_ip(listen)}:{node_exporter_port}",
            'target': host,
            'status': 'Down'
        })
        # cmd exporter
        cmd_exporter_url = f"{prepare_ip(host)}:{cmd_exporter_port}/metrics"
        checks.append({
            'url': cmd_exporter_url,
            'type': 'cmd-exporter',
            'address': f"{prepare_ip(host)}:{cmd_exporter_port}",
            'listen': f"{prepare_ip(listen)}:{cmd_exporter_port}",
            'target': host,
            'status': 'Down'
        })
        # opengauss exporter
        for i, db_instance in enumerate(exporters[host]['db_instance']):
            opengauss_exporter = exporters[host]['opengauss_exporters'][i]
            opengauss_port = split_ip_port(opengauss_exporter)[1]
            opengauss_exporter_url = "{}/metrics".format(opengauss_exporter)
            checks.append({
                'url': opengauss_exporter_url,
                'type': 'opengauss-exporter',
                'address': opengauss_exporter,
                'listen': f"{prepare_ip(listen)}:{opengauss_port}",
                'target': db_instance,
                'status': 'Down'
            })

    return checks


def check(checks, timeout):
    """
    Check the status of prometheus and exporters with the information in checks.
    Check the status_code of request.get(check_item['url'], header) to see if the target
    is running as expected.
    """

    print('Checking the status of Prometheus and exporters.')
    s = requests.Session()  # suitable for both http and https
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))

    for i, check_item in enumerate(checks):
        try:
            response = s.get(
                'http://' + check_item['url'],
                headers={"Content-Type": "application/json"},
                verify=False,
                timeout=timeout
            )
            if response.status_code == 200:
                checks[i]['status'] = GREEN_FMT.format('Up')
            else:
                print(f'The response status code is {response.status_code}, connection failed.')
                checks[i]['status'] = RED_FMT.format('Down')

        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.ConnectionError) and \
                    'Remote end closed connection without response' in str(e):
                checks[i]['status'] = GREEN_FMT.format('Up')
            else:
                try:
                    response = s.get(
                        'https://' + check_item['url'],
                        headers={"Content-Type": "application/json"},
                        verify=True,
                        timeout=timeout
                    )
                    if response.status_code == 200:
                        checks[i]['status'] = GREEN_FMT.format('Up')
                    else:
                        print(f'The response status code is {response.status_code}, connection failed.')
                        checks[i]['status'] = RED_FMT.format('Down')

                except requests.exceptions.RequestException as e:
                    if isinstance(e, requests.exceptions.SSLError) and \
                            'certificate verify failed' in str(e):
                        checks[i]['status'] = GREEN_FMT.format('Up')
                    else:
                        print(e)
                        checks[i]['status'] = RED_FMT.format('Down')

    open_source = ['prometheus-server', 'node-exporter']
    pt = PrettyTable()
    warnings = set()
    pt.field_names = list(checks[0].keys())[1:]
    for check_item in checks:
        pt.add_row(list(check_item.values())[1:])
        if '0.0.0.0' in check_item['listen'] and check_item['type'] in open_source:
            warnings.add(
                f"Because {check_item['type']} is an open source software, "
                "we won't know which NIC the user wants to bind to, thus we"
                " don't change it."
            )

    print(pt)
    for warning in warnings:
        print(warning)


def main(argv):
    """
    We provide 4 attributes to direct the deployment.
    --online and --offline are almost the same. The difference is that the option --online
    will download the prometheus.tar.gz and node_exporter.tar.gz files and get the
    sha256 checksums for the two tar.gz files before the deployment.
    the deployment will demand the user to input a series of configs and the input configs
    will be recorded in the config file.
    --run will run the prometheus and the exporters according to the config file.
    --check will check the status of prometheus and exporters according to the config file.
    """
    global CONFIG_PATH

    parser = argparse.ArgumentParser(description='To deploy Prometheus, node-exporter, '
                                                 'cmd-exporter, openGauss-exporter and '
                                                 'reprocessing-exporter')
    parser.add_argument('--online', action='store_true',
                        help='Download the Prometheus and node_exporters online. '
                             'Deploy Prometheus and exporters to the nodes locally.')
    parser.add_argument('--offline', action='store_true',
                        help='Deploy Prometheus and exporters to the nodes locally.')
    parser.add_argument('--run', action='store_true',
                        help='Run Prometheus and all the exporters.')
    parser.add_argument('--check', action='store_true',
                        help='Check the status of Prometheus and all the exporters.')
    parser.add_argument('-c', '--conf', type=path_type,
                        help='Indicates the location of the config file to skip interactive configuration. '
                             'Default path is %s.' % CONFIG_PATH)
    parser.add_argument('--timeout', default=10, type=int,
                        help='Indicates the time in seconds to wait for response '
                             'from exporters when --check.')
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('--lite', default=None, type=str,
                        help='Only deploy Prometheus or node_exporter to the nodes locally. '
                             'all for deploy Prometheus and node_exporter. '
                             'prometheus for deploy Prometheus. node_exporter for deploy node_exporter.')
    args = vars(parser.parse_args(argv))
    log_file_path = os.path.join(os.getcwd(), 'dbmind_deployment.log')
    set_logger(log_file_path, "info")

    n_actions = sum([bool(action) for action in (args['online'], args['offline'], args['run'], args['check'])])
    if n_actions == 0:
        parser.error('You must specify a action from [--online, --offline, --run, --check] or --help for details.')
    elif n_actions > 1:
        parser.error("You can't specify more than one action from [--online, --offline, --run, --check].")

    if args['conf'] is not None:
        if os.path.exists(args['conf']):
            CONFIG_PATH = args['conf']
        else:
            print("{} doesn't exist.".format(args['conf']))
            return

    if args['lite'] is not None:
        if args['lite'] != 'all' and args['lite'] != 'prometheus' and args['lite'] != 'node_exporter':
            print("--lite must be all or prometheus, or node_exporter.")
            return

    configs = ConfigParser(inline_comment_prefixes=None)
    with open(file=CONFIG_PATH, mode='r') as fp:
        configs.read_file(fp)

    architecture = platform.uname().processor
    if architecture not in ARCHITECTURES:
        print(f"The unsupported CPU architecture: {architecture}, exiting.")
        return

    try:
        if args['online'] or args['offline']:
            os_arch = ARCHITECTURES[architecture]
            prometheus = configs.get(DOWNLOADING, 'prometheus')
            node_exporter = configs.get(DOWNLOADING, 'node_exporter')
            prometheus_arch_in_config = prometheus.rsplit('-', 1)[1]
            if prometheus_arch_in_config != os_arch:
                print(f"{prometheus} should be adapted to '{os_arch}', modifying.")
                prometheus = prometheus[:prometheus.rfind(prometheus_arch_in_config)] + os_arch
                configs.set(DOWNLOADING, 'prometheus', prometheus)
            node_exporter_arch_in_config = node_exporter.rsplit('-', 1)[1]
            if node_exporter_arch_in_config != os_arch:
                print(f"{node_exporter} should be adapted to '{os_arch}', modifying.")
                node_exporter = node_exporter[:node_exporter.rfind(node_exporter_arch_in_config)] + os_arch
                configs.set(DOWNLOADING, 'node_exporter', node_exporter)

            if args['conf'] is None:
                set_deploy_config_interactive(configs)
            else:
                passwd_input(PROMETHEUS, configs)
                passwd_input(EXPORTERS, configs)

            deploy(configs, online=args['online'], lite=args['lite'])

        if args['run'] or args['check']:
            if config_ports_has_conflict(configs):
                sys.exit(0)

            if args['run']:
                passwd_input(PROMETHEUS, configs)
                passwd_input(EXPORTERS, configs)
                passwd_input(DATABASE, configs)
                tasks = generate_tasks(configs)
                checks = generate_checks(configs)
                run(tasks)
                print('Wait for checking, just a moment.')
                time.sleep(10)
                check(checks, args['timeout'])
            elif args['check']:
                checks = generate_checks(configs)
                check(checks, args['timeout'])

    except KeyboardInterrupt:
        print("\nThe procedure was manually terminated.")
