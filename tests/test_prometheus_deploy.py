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

import configparser
import os
import pytest
from collections import defaultdict
from unittest import mock

from dbmind.components.deployment import prometheus_deploy
from dbmind.components.deployment.prometheus_deploy import (
    config_ports_has_conflict,
    db_exporters_parsing,
    edit_prometheus_yaml,
    get_target_generator,
    generate_tasks,
    generate_checks
)
from dbmind.components.deployment.utils import (
    url_generate,
    check_config_validity
)


@pytest.fixture
def deploy_configs():
    configs = configparser.ConfigParser()
    configs.add_section('DOWNLOADING')
    configs.set('DOWNLOADING', 'host', 'https://github.com/prometheus')
    configs.set('DOWNLOADING', 'node_exporter', 'node_exporter-1.3.1.linux-amd64')
    configs.set('DOWNLOADING', 'prometheus', 'prometheus-2.35.0-rc0.linux-amd64')
    configs.set('DOWNLOADING', 'node_exporter_sha256',
                '68f3802c2dd3980667e4ba65ea2e1fb03f4a4ba026cca375f15a0390ff850949')
    configs.set('DOWNLOADING', 'prometheus_sha256',
                '0a5df354665575ee490fbc7653284638e77dd02ef2118e040c1b2b9c666f9071')
    configs.add_section('PROMETHEUS')
    configs.set('PROMETHEUS', 'host', '10.90.56.173')
    configs.set('PROMETHEUS', 'ssh_port', '22')
    configs.set('PROMETHEUS', 'host_username', 'cent')
    configs.set('PROMETHEUS', 'path', '/media/sdb/cent/deploy')
    configs.set('PROMETHEUS', 'listen_address', '0.0.0.0')
    configs.set('PROMETHEUS', 'prometheus_port', '9090')
    configs.set('PROMETHEUS', 'reprocessing_exporter_port', '8181')
    configs.add_section('EXPORTERS')
    configs.set('EXPORTERS', 'targets',
                '10.90.56.172:19995/postgres, 10.90.56.173:19995/postgres')
    configs.set('EXPORTERS', 'ssh_port', '22')
    configs.set('EXPORTERS', 'host_username', 'cent')
    configs.set('EXPORTERS', 'path', '/media/sdb/deploy')
    configs.set('EXPORTERS', 'database_username', 'dbmind')
    configs.set('EXPORTERS', 'listen_address', '0.0.0.0')
    configs.set('EXPORTERS', 'opengauss_ports_range', '9187-9197')
    configs.set('EXPORTERS', 'node_exporter_port', '9100')
    configs.set('EXPORTERS', 'cmd_exporter_port', '9180')
    configs.add_section('SSL')
    configs.set('SSL', 'enable_ssl', 'False')
    configs.set('SSL', 'prometheus_ssl_certfile', '/media/sdb/deploy/prometheus.crt')
    configs.set('SSL', 'prometheus_ssl_keyfile', '/media/sdb/deploy/prometheus.key')
    configs.set('SSL', 'prometheus_ssl_ca_file', '/media/sdb/deploy/prometheus.crt')
    configs.set('SSL', 'exporter_ssl_certfile', '/media/sdb/deploy/prometheus.crt')
    configs.set('SSL', 'exporter_ssl_keyfile', '/media/sdb/deploy/prometheus.key')
    configs.set('SSL', 'exporter_ssl_ca_file', '/media/sdb/deploy/prometheus.crt')
    return configs


@pytest.fixture
def prometheus_yaml_origin():
    yaml_obj = {
        'global': {
            'scrape_interval': '15s',
            'evaluation_interval': '15s'
        },
        'alerting': {
            'alertmanagers': [
                {
                    'static_configs': [
                        {
                            'targets': None
                        }
                    ]
                }
            ]
        },
        'rule_files': None,
        'scrape_configs': [
            {
                'job_name': 'prometheus',
                'static_configs': [
                    {
                        'targets': ['localhost:9090']
                    }
                ]
            }
        ]
    }
    return yaml_obj


@pytest.fixture
def prometheus_yaml_target():
    yaml_target = {
        'alerting': {
            'alertmanagers': [
                {
                    'static_configs': [
                        {
                            'targets': None
                        }
                    ]
                }
            ]
        },
        'global': {
            'evaluation_interval': '15s',
            'scrape_interval': '15s'
        },
        'rule_files': None,
        'scrape_configs': [
            {
                'job_name': 'prometheus',
                'static_configs': [
                    {
                        'targets': [
                            '10.90.56.173:9090'
                        ]
                    }
                ]
            },
            {
                'job_name': 'reprocessing_exporter',
                'scheme': 'http',
                'static_configs': [
                    {
                        'targets': [
                            '10.90.56.173:8181'
                        ]
                    }
                ]
            },
            {
                'job_name': 'node_exporter',
                'scheme': 'http',
                'static_configs': [
                    {
                        'targets': [
                            '10.90.56.172:9100',
                            '10.90.56.173:9100'
                        ]
                    }
                ]
            }, {
                'job_name': 'cmd_exporter',
                'scheme': 'http',
                'static_configs': [
                    {
                        'targets': [
                            '10.90.56.172:9180',
                            '10.90.56.173:9180'
                        ]
                    }
                ]
            },
            {
                'job_name': 'opengauss_exporter',
                'scheme': 'http',
                'static_configs': [
                    {
                        'targets': [
                            '10.90.56.172:9187',
                            '10.90.56.173:9187'
                        ]
                    }
                ]
            }
        ]
    }
    return yaml_target


def test_config_ports_has_conflict(deploy_configs):
    configs = deploy_configs
    assert (not config_ports_has_conflict(configs))

    configs.set('PROMETHEUS', 'reprocessing_exporter_port', '9090')
    assert (config_ports_has_conflict(configs))
    configs.set('PROMETHEUS', 'reprocessing_exporter_port', '8181')

    configs.set('EXPORTERS', 'node_exporter_port', '9090')
    assert (config_ports_has_conflict(configs))
    configs.set('EXPORTERS', 'node_exporter_port', '9100')

    configs.set('EXPORTERS', 'opengauss_ports_range', '9087-9197')
    assert (config_ports_has_conflict(configs))
    configs.set('EXPORTERS', 'opengauss_ports_range', '9187-9197')


def test_db_exporters_parsing(deploy_configs):
    configs = deploy_configs
    exporters = db_exporters_parsing(configs)
    target = {
        '10.90.56.172': defaultdict(
            list,
            {
                'db_instance': ['10.90.56.172:19995/postgres'],
                'opengauss_exporters': ['10.90.56.172:9187']
            }
        ),
        '10.90.56.173': defaultdict(
            list,
            {
                'db_instance': ['10.90.56.173:19995/postgres'],
                'opengauss_exporters': ['10.90.56.173:9187']
            }
        )
    }
    assert (exporters == target)

    configs.set('EXPORTERS', 'targets', '10.90.56.172:19995/postgres, 10.90.56.172:19995/tpcc10')
    configs.set('EXPORTERS', 'opengauss_ports_range', '9195-9197')
    exporters = db_exporters_parsing(configs)
    target = {
        '10.90.56.172': defaultdict(
            list,
            {
                'db_instance': [
                    '10.90.56.172:19995/postgres',
                    '10.90.56.172:19995/tpcc10',
                ],
                'opengauss_exporters': [
                    '10.90.56.172:9195',
                    '10.90.56.172:9196',
                ]
            }
        ),
    }
    assert (exporters == target)
    configs.set('EXPORTERS', 'targets', '10.90.56.172:19995/postgres, 10.90.56.173:19995/postgres')
    configs.set('EXPORTERS', 'opengauss_ports_range', '9187-9197')


def test_edit_prometheus_yaml(deploy_configs, prometheus_yaml_origin, prometheus_yaml_target):
    configs = deploy_configs
    exporters = db_exporters_parsing(configs)
    generate_targets = get_target_generator(exporters)
    node_exporter_targets = generate_targets(configs.get("EXPORTERS", 'node_exporter_port'))
    cmd_exporter_targets = generate_targets(configs.get("EXPORTERS", 'cmd_exporter_port'))
    opengauss_exporter_targets = sum([[t for t in exporters[host]['opengauss_exporters']]
                                      for host in exporters], [])
    new_yaml_obj = edit_prometheus_yaml(
        prometheus_yaml_origin,
        configs,
        node_exporter_targets = node_exporter_targets,
        cmd_exporter_targets = cmd_exporter_targets,
        opengauss_exporter_targets = opengauss_exporter_targets
    )
    assert new_yaml_obj == prometheus_yaml_target



def test_generate_tasks(monkeypatch, deploy_configs):
    target = [
        [
            'chmod +x /media/sdb/cent/deploy/prometheus-2.35.0-rc0.linux-amd64/prometheus',
            'chmod +x /media/sdb/cent/deploy/gs_dbmind',
            [
                '/media/sdb/cent/deploy/prometheus-2.35.0-rc0.linux-amd64/prometheus '
                '--web.listen-address=0.0.0.0:9090 --config.file '
                '/media/sdb/cent/deploy/prometheus-2.35.0-rc0.linux-amd64/prometheus.yml '
                '--web.enable-admin-api '
                '--storage.tsdb.retention.time=1w '
                '>/prometheus.log',
                '',
                'Prometheus of 10.90.56.173'
            ],
            [
                '/media/sdb/cent/deploy/gs_dbmind component reprocessing_exporter 10.90.56.173 9090 '
                '--web.listen-address 0.0.0.0 '
                '--web.listen-port 8181 '
                '--disable-https',
                'reprocessing-exporter of 10.90.56.173 has been started or the address already in use.',
                'reprocessing-exporter of 10.90.56.173'
            ],
            'chmod 600 /prometheus.log',
            'chmod +x /media/sdb/deploy/node_exporter-1.3.1.linux-amd64/node_exporter',
            'chmod +x /media/sdb/deploy/gs_dbmind',
            [
                '/media/sdb/deploy/node_exporter-1.3.1.linux-amd64/node_exporter '
                '--web.listen-address=:9100',
                '',
                'node-exporter of 10.90.56.172'
            ],
            [
                '/media/sdb/deploy/gs_dbmind component cmd_exporter '
                '--web.listen-address 0.0.0.0 '
                '--web.listen-port 9180 '
                '--disable-https',
                'cmd-exporter of 10.90.56.172 has been started or the address already in use.',
                'cmd-exporter of 10.90.56.172'
            ],
            [
                '/media/sdb/deploy/gs_dbmind component opengauss_exporter '
                '--url postgresql://dbmind:3@10.90.56.172:19995/postgres '
                '--web.listen-address 0.0.0.0 '
                '--web.listen-port 9187 '
                '--disable-https',
                'opengauss-exporter of 10.90.56.172:9187 has been started or the address already in use.',
                'opengauss-exporter of 10.90.56.172:9187'
            ],
            'chmod +x /media/sdb/deploy/node_exporter-1.3.1.linux-amd64/node_exporter',
            'chmod +x /media/sdb/deploy/gs_dbmind',
            [
                '/media/sdb/deploy/node_exporter-1.3.1.linux-amd64/node_exporter '
                '--web.listen-address=:9100',
                '',
                'node-exporter of 10.90.56.173'
            ],
            [
                '/media/sdb/deploy/gs_dbmind component cmd_exporter '
                '--web.listen-address 0.0.0.0 '
                '--web.listen-port 9180 '
                '--disable-https',
                'cmd-exporter of 10.90.56.173 has been started or the address already in use.',
                'cmd-exporter of 10.90.56.173'
            ],
            [
                '/media/sdb/deploy/gs_dbmind component opengauss_exporter '
                '--url postgresql://dbmind:3@10.90.56.173:19995/postgres '
                '--web.listen-address 0.0.0.0 '
                '--web.listen-port 9187 '
                '--disable-https',
                'opengauss-exporter of 10.90.56.173:9187 has been started or the address already in use.',
                'opengauss-exporter of 10.90.56.173:9187'
            ]
        ]
    ]
    mock_PWD = {
        "PROMETHEUS": "1",
        "EXPORTERS": "2",
        "DATABASE": "3"
    }
    configs = deploy_configs

    mock_SSH = mock.MagicMock()

    monkeypatch.setattr(os.path, 'join', lambda *args: str.join("/", list(args)))
    monkeypatch.setattr(prometheus_deploy, 'EXTRACT_PATH', '')
    monkeypatch.setattr(prometheus_deploy, 'PWD', mock_PWD)
    monkeypatch.setattr(prometheus_deploy, 'SSH', mock_SSH)
    monkeypatch.setattr(prometheus_deploy.SSH, 'exec_command_sync', mock.MagicMock())
    monkeypatch.setattr(prometheus_deploy, 'PWD', mock_PWD)

    tasks = generate_tasks(configs)
    assert list(tasks.values()) == target


def test_generate_checks(deploy_configs):
    target = [
        {
            'url': '10.90.56.173:9090/api/v1/query?query=up',
            'type': 'prometheus-server',
            'address': '10.90.56.173:9090',
            'listen': '0.0.0.0:9090',
            'target': '-',
            'status': 'Down'
        },
        {
            'url': '10.90.56.173:8181/metrics',
            'type': 'reprocessing-exporter',
            'address': '10.90.56.173:8181',
            'listen': '0.0.0.0:8181',
            'target': '10.90.56.173:9090',
            'status': 'Down'
        },
        {
            'url': '10.90.56.172:9100/metrics',
            'type': 'node-exporter',
            'address': '10.90.56.172:9100',
            'listen': '0.0.0.0:9100',
            'target': '10.90.56.172',
            'status': 'Down'
        },
        {
            'url': '10.90.56.172:9180/metrics',
            'type': 'cmd-exporter',
            'address': '10.90.56.172:9180',
            'listen': '0.0.0.0:9180',
            'target': '10.90.56.172',
            'status': 'Down'
        },
        {
            'url': '10.90.56.172:9187/metrics',
            'type': 'opengauss-exporter',
            'address': '10.90.56.172:9187',
            'listen': '0.0.0.0:9187',
            'target': '10.90.56.172:19995/postgres',
            'status': 'Down'
        },
        {
            'url': '10.90.56.173:9100/metrics',
            'type': 'node-exporter',
            'address': '10.90.56.173:9100',
            'listen': '0.0.0.0:9100',
            'target': '10.90.56.173',
            'status': 'Down'
        },
        {
            'url': '10.90.56.173:9180/metrics',
            'type': 'cmd-exporter',
            'address': '10.90.56.173:9180',
            'listen': '0.0.0.0:9180',
            'target': '10.90.56.173',
            'status': 'Down'
        },
        {
            'url': '10.90.56.173:9187/metrics',
            'type': 'opengauss-exporter',
            'address': '10.90.56.173:9187',
            'listen': '0.0.0.0:9187',
            'target': '10.90.56.173:19995/postgres',
            'status': 'Down'
        }
    ]
    configs = deploy_configs
    checks = generate_checks(configs)
    assert checks == target


def test_url_generate(deploy_configs):
    configs = deploy_configs
    host = configs.get("DOWNLOADING", "host")

    filename = configs.get("DOWNLOADING", "prometheus")
    url = url_generate(filename, host)
    assert (url == host + "/prometheus/releases/download/v2.35.0-rc0/prometheus-2.35.0-rc0.linux-amd64")

    filename = configs.get("DOWNLOADING", "node_exporter")
    url = url_generate(filename, host)
    assert (url == host + "/node_exporter/releases/download/v1.3.1/node_exporter-1.3.1.linux-amd64")

    filename = "prometheus-2.35.0.linux-amd64"
    url = url_generate(filename, host)
    assert (url == host + "/prometheus/releases/download/v2.35.0/prometheus-2.35.0.linux-amd64")

    filename = "prometheus-2.35.0.linux-arm64"
    url = url_generate(filename, host)
    assert (url == host + "/prometheus/releases/download/v2.35.0/prometheus-2.35.0.linux-arm64")

    filename = "prometheus-2.35.4.linux-amd64"
    url = url_generate(filename, host)
    assert (url == host + "/prometheus/releases/download/v2.35.4/prometheus-2.35.4.linux-amd64")


def test_check_config_validity():
    section, option, value = "EXPORTERS", "opengauss_ports_range", "9187"
    assert(check_config_validity(section, option, value) ==
          (False, 'You need to input a range. eg. (start-end)'))

    section, option, value = "EXPORTERS", "opengauss_ports_range", "9197-9187"
    assert(check_config_validity(section, option, value) ==
          (False, 'The start 9197 must be fewer than the end 9187.'))

    section, option, value = "EXPORTERS", "opengauss_ports_range", "1000-1025"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid port for EXPORTERS-opengauss_ports_range: 1000-1025(1024-65535)'))

    section, option, value = "EXPORTERS", "opengauss_ports_range", "65534-65545"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid port for EXPORTERS-opengauss_ports_range: 65534-65545(1024-65535)'))

    section, option, value = "PROMETHEUS", "ssh_port", "9000"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid port for PROMETHEUS-ssh_port: 9000(1-1023)'))

    section, option, value = "PROMETHEUS", "ssh_port", "10.0.0.256"
    assert(check_config_validity(section, option, value) ==
          (False, '10.0.0.256 is not a integer.'))

    section, option, value = "PROMETHEUS", "ssh_port", "ssh"
    assert(check_config_validity(section, option, value) ==
          (False, 'ssh is not a integer.'))

    section, option, value = "PROMETHEUS", "prometheus_port", "1023"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid port for PROMETHEUS-prometheus_port: 1023(1024-65535)'))

    section, option, value = "EXPORTERS", "node_exporter_port", "65536"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid port for EXPORTERS-node_exporter_port: 65536(1024-65535)'))

    section, option, value = "PROMETHEUS", "listen_address", "010.90.56.172"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid IP Address for PROMETHEUS-listen_address: 010.90.56.172'))

    section, option, value = "PROMETHEUS", "host", "10.0.0.256"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid IP Address for PROMETHEUS-host: 10.0.0.256'))

    section, option, value = "PROMETHEUS", "host", "www.baidu.com"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid IP Address for PROMETHEUS-host: www.baidu.com'))

    section, option, value = "SSL", "enable_ssl", "false"
    assert(check_config_validity(section, option, value) ==
          (False, 'enable_ssl must be "True" or "False".'))

    section, option, value = "EXPORTERS", "targets", "10.90.56.172:19995/postgres, "
    assert(check_config_validity(section, option, value) ==
          (False, 'Illegal db instance " ", e.g. ip:port/dbname'))

    section, option, value = "EXPORTERS", "targets", "10.90.56.172:19995:postgres, 10.90.56.173:19995/postgres"
    assert(check_config_validity(section, option, value) ==
          (False, 'Illegal db instance "10.90.56.172:19995:postgres", e.g. ip:port/dbname'))

    section, option, value = "EXPORTERS", "targets", "10.90.56.172:19995/postgres, 10.90.56.173/19995/postgres"
    assert(check_config_validity(section, option, value) ==
          (False, 'Illegal db instance " 10.90.56.173/19995/postgres", e.g. ip:port/dbname'))

    section, option, value = "EXPORTERS", "targets", "10.90.56.172:1023/postgres, 10.90.56.173:19995/postgres"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid port "1023" (1024-65535) for "10.90.56.172:1023/postgres" in EXPORTERS-targets.'))

    section, option, value = "EXPORTERS", "targets", "10.90.56.172:19995/postgres:, 10.90.56.256:19995/postgres"
    assert(check_config_validity(section, option, value) ==
          (False, 'Illegal db instance "10.90.56.172:19995/postgres:", e.g. ip:port/dbname'))

    section, option, value = "EXPORTERS", "targets", "10.90.56.172:19995/ , 10.90.56.173:19995/postgres"
    assert(check_config_validity(section, option, value) ==
          (False, 'Empty dbname " " for "10.90.56.172:19995/ " in EXPORTERS-targets.'))

    section, option, value = "EXPORTERS", "targets", "10.90.56.172:19995/postgres, 10.90.56.256:65536/postgres"
    assert(check_config_validity(section, option, value) ==
          (False, 'Invalid IP " 10.90.56.256" and Invalid port "65536" (1024-65535) '
                  'for " 10.90.56.256:65536/postgres" in EXPORTERS-targets.'))
