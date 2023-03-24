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
import os
from configparser import ConfigParser
from configparser import NoSectionError, NoOptionError

from dbmind import constants
from dbmind.cmd.configs.config_constants import (
    ENCRYPTED_SIGNAL, check_config_validity, NULL_TYPE
)
from dbmind.cmd.configs.configurators import ReadonlyConfig, UpdateConfig
from dbmind.common import security, utils
from dbmind.common.exceptions import ConfigSettingError
from dbmind.common.utils.cli import write_to_terminal
from dbmind.metadatabase import create_dynamic_config_schema
from dbmind.metadatabase.dao.dynamic_config import dynamic_config_get, dynamic_config_set


def load_sys_configs(confile):
    return ReadonlyConfig(confile)


def set_config_parameter(confpath, section: str, option: str, value: str):
    if not os.path.exists(confpath):
        raise ConfigSettingError("Invalid directory '%s', please set up first." % confpath)

    # Section is case sensitive.
    if section.isupper():
        with UpdateConfig(os.path.join(confpath, constants.CONFILE_NAME)) as config:
            # If not found, raise NoSectionError or NoOptionError.
            try:
                old_value, comment = config.get(section, option)
            except (NoSectionError, NoOptionError):
                raise ConfigSettingError('Not found the parameter %s-%s.' % (section, option))
            valid, reason = check_config_validity(section, option, value)
            if not valid:
                raise ConfigSettingError('Incorrect value due to %s.' % reason)
            # If user wants to change password, we should encrypt the plain-text password first.
            if 'password' in option:
                # dynamic_config_xxx searches file from current working directory.
                os.chdir(confpath)
                s1 = dynamic_config_get('dbmind_config', 'cipher_s1')
                s2 = dynamic_config_get('dbmind_config', 'cipher_s2')
                # Every time a new password is generated, update the IV.
                iv = security.generate_an_iv()
                dynamic_config_set('iv_table', '%s-%s' % (section, option), iv)
                cipher = security.encrypt(s1, s2, iv, value)
                value = ENCRYPTED_SIGNAL + cipher
            config.set(section, option, value, comment)
    elif section.islower():
        # dynamic_config_xxx searches file from current working directory.
        os.chdir(confpath)
        try:
            old_value = dynamic_config_get(section, option)
        except ValueError:
            raise ConfigSettingError('Not found the parameter %s-%s.' % (section, option))
        if not old_value:
            raise ConfigSettingError('Not found the parameter %s-%s.' % (section, option))
        dynamic_config_set(section, option, value)
    else:
        # If run here, it seems that the format of section string is not correct.
        raise ConfigSettingError('%s is an incorrect section. '
                                 'Please take note that section string is case sensitive.' % section)

    write_to_terminal('Success to modify parameter %s-%s.' % (section, option), color='green')


def create_dynamic_configs():
    """Create dynamic configuration schema and
    generate security keys."""
    utils.cli.write_to_terminal(
        'Starting to generate a dynamic config file...',
        color='green')
    create_dynamic_config_schema()
    s1_ = security.safe_random_string(16)
    s2_ = security.safe_random_string(16)
    dynamic_config_set('dbmind_config', 'cipher_s1', s1_)
    dynamic_config_set('dbmind_config', 'cipher_s2', s2_)
    return s1_, s2_


def get_config_security_keys():
    s1 = dynamic_config_get('dbmind_config', 'cipher_s1')
    s2 = dynamic_config_get('dbmind_config', 'cipher_s2')
    return s1, s2


def config_standardize_null_value(value):
    # If not set default value,
    # the default value is null.
    v = value.strip()
    if v == '':
        return NULL_TYPE
    return v


def config_is_null_value(value):
    return value == NULL_TYPE


def config_is_encrypted_value(value):
    return value.startswith(ENCRYPTED_SIGNAL)


def config_set_value_encrypted_flag(cipher_text):
    # Use a signal ENCRYPTED_SIGNAL to mark the
    # password that has been encrypted.
    return ENCRYPTED_SIGNAL + cipher_text


def set_config_encryption_iv(iv, section, option):
    """Record IV for each config option to prevent
    rainbow attack from hackers.
    """
    dynamic_config_set('iv_table', '%s-%s' % (section, option), iv)
