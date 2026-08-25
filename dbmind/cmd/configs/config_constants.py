# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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


from configparser import ConfigParser

import dbmind.common
from dbmind.common.rpc import ping_rpc_url
from dbmind.common.utils import write_to_terminal
from dbmind.common.utils.checking import check_port_valid, check_ip_valid
from dbmind.common.parser.others import extract_ip_groups
from dbmind import constants

# header text
DBMIND_CONF_HEADER = """\
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

# Notice:
# 1. (null) explicitly represents empty or null. Meanwhile blank represents undefined.
# 2. DBMind encrypts password parameters. Hence, there is no plain-text password after initialization.
# 3. Users can only configure the plain-text password in this file before initializing
#    (that is, using the --initialize option),
#    and then if users want to modify the password-related information,
#    users need to use the 'set' sub-command to achieve.
# 4. If users use relative path in this file, the current working directory is the directory where this file is located.
"""

# fixed constant flags in the config file
NULL_TYPE = '(null)'  # empty text.
ENCRYPTED_SIGNAL = 'Encrypted->'

# Used by check_config_validity().
CONFIG_OPTIONS = {
    'TSDB-name': ['prometheus', 'influxdb', 'ignore'],
    'METADATABASE-dbtype': ['sqlite', 'opengauss', 'opengauss', 'postgresql'],
    'WORKER-type': ['local', 'dist'],
    'LOG-level': ['DEBUG', 'INFO', 'WARNING', 'ERROR']
}
POSITIVE_INTEGER_CONFIG = ['LOG-maxbytes', 'LOG-backupcount']
BOOLEAN_CONFIG = []


def check_config_validity(section, option, value, silent=False, microservice=False, ignore_tsdb=False):
    config_item = '%s-%s' % (section, option)
    # exceptional cases:
    if config_item in ('METADATABASE-port', 'METADATABASE-host'):
        if value.strip() == '' or value == NULL_TYPE:
            return True, None

    if (config_item == 'AGENT-username' or config_item == 'AGENT-password') and not microservice:
        if value.strip() in ('', NULL_TYPE):
            return False, 'Not set Agent-username or Agent-password'

    if config_item == 'AGENT-master_url' and not silent:
        if value.strip() in ('', NULL_TYPE):
            write_to_terminal(
                'Notice: Without explicitly setting agent configurations, '
                'the automatic detection mechanism is used.',
                color='yellow'
            )
        else:
            for url in value.split(','):
                success = ping_rpc_url(url.strip())
                if not success:
                    write_to_terminal(
                        'WARNING: Failed to test the RPC url %s.' % url,
                        color='yellow'
                    )

    if section == 'TIMED_TASK' and option.endswith('_interval'):
        if not value.isdigit() or int(value) < constants.MINIMAL_TIMED_TASK_INTERVAL:
            return False, f'Invalid task interval for {option.replace("_interval", "")}: "{value}"' \
                          f'(positive integer not less than {constants.MINIMAL_TIMED_TASK_INTERVAL} expected)'

    # normal inspection process:
    if option == 'port':
        if section == 'METADATABASE':
            for item in value.split(','):
                if not check_port_valid(item.strip()):
                    return False, 'Invalid port for %s: %s(1024-65535)' % (config_item, value)
        elif section == 'TSDB':
            if not ignore_tsdb and not check_port_valid(value):
                return False, 'Invalid port for %s: %s(1024-65535)' % (config_item, value)
        elif not check_port_valid(value):
            return False, 'Invalid port for %s: %s(1024-65535)' % (config_item, value)
    if option == 'host':
        if section == 'METADATABASE':
            for item in value.split(','):
                if not check_ip_valid(item.strip()):
                    return False, 'Invalid IP Address for %s: %s' % (config_item, value)
        elif section == 'TSDB':
            if not ignore_tsdb and not check_ip_valid(value):
                return False, 'Invalid IP Address for %s: %s' % (config_item, value)
        elif not check_ip_valid(value):
            return False, 'Invalid IP Address for %s: %s' % (config_item, value)
    if option == 'database':
        if value == NULL_TYPE or value.strip() == '':
            return False, 'Unspecified database name %s' % value
    if config_item in POSITIVE_INTEGER_CONFIG:
        if not str.isdigit(value) or int(value) <= 0:
            return False, 'Invalid value for %s: %s' % (config_item, value)
    if config_item in BOOLEAN_CONFIG:
        if value.lower() not in ConfigParser.BOOLEAN_STATES:
            return False, 'Invalid boolean value for %s.' % config_item
    options = CONFIG_OPTIONS.get(config_item)
    if options and value not in options:
        return False, 'Invalid choice for %s: %s' % (config_item, value)

    if 'dbtype' == option and value in ('opengauss', 'opengauss') and not silent:
        write_to_terminal(
            'WARN: default PostgreSQL connector (psycopg2-binary) does not support GaussDB.\n'
            'It would help if you compiled psycopg2 with GaussDB manually or '
            'created a connection user after setting the GUC password_encryption_type to 1.',
            color='yellow'
        )
    if 'dbtype' == option and value == 'sqlite' and not silent:
        write_to_terminal(
            'NOTE: SQLite currently only supports local deployment, so you only need to provide '
            'METADATABASE-database information. If you provide other information, DBMind will '
            'ignore them.',
            color='yellow'
        )
    if value != NULL_TYPE and 'ssl_certfile' in option:
        dbmind.common.utils.checking.warn_ssl_certificate(value, None, None)
    if value != NULL_TYPE and 'ssl_keyfile' == option:
        dbmind.common.utils.checking.warn_ssl_certificate(None, value, None)
    if value != NULL_TYPE and 'ssl_ca_file' == option:
        dbmind.common.utils.checking.warn_ssl_certificate(None, None, value)
    if section == 'IP_MAP' and option == 'ip_map' and value != NULL_TYPE and value:
        ip_groups = extract_ip_groups(value)
        if not ip_groups:
            return False, f'Please check the format of ip_map'
        for ip_group in ip_groups:
            for ip_pair in ip_group:
                if not len(ip_pair) == 2:
                    return False, f'Invalid format for {config_item}: {value}'
                for ip in ip_pair:
                    if not check_ip_valid(ip):
                        return False, f'Invalid IP in the {config_item}: {ip}'

    # Add more checks here.
    return True, None


# Ignore the following sections while config iterates
SKIP_LIST = ('COMMENT', 'LOG', 'TIMED_TASK', 'TIMED_TASK_LIST')
