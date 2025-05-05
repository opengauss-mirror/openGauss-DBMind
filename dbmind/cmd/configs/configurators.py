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

import configparser
import logging
import os
from configparser import ConfigParser

from dbmind.cmd.configs.config_constants import (
    SKIP_LIST,
    NULL_TYPE,
    ENCRYPTED_SIGNAL,
    DBMIND_CONF_HEADER,
    check_config_validity
)
from dbmind import constants
from dbmind.common import security, utils
from dbmind.common.exceptions import (
    InvalidCredentialException,
    ConfigSettingError,
    DontIgnoreThisError
)
from dbmind.common.utils import cast_to_int_or_float
from dbmind.common.utils.checking import uniform_ip, uniform_instance
from dbmind.metadatabase.dao.dynamic_config import (
    dynamic_config_get,
    dynamic_config_set,
    dynamic_configs_list
)
from dbmind.metadatabase.schema.config_dynamic_params import IV_TABLE

from .base_configurator import BaseConfig
from ...metadatabase.ddl import create_dynamic_config_schema


class ReadonlyConfig(BaseConfig):
    def set(self, section, option, value, *args, **kwargs):
        raise AssertionError('Should not call this method!')

    def __init__(self, filepath):
        """This is a readonly config and bing
        used in the running mode.
        Other apps will get config value by using this class.

        :param filepath: config filepath.
        """
        # Note: To facilitate the user to modify
        # the configuration items through the
        # configuration file easily, we add
        # inline comments to the file, but we need
        # to remove the inline comments while parsing.
        # Otherwise, it will cause the read configuration
        # items to be wrong.
        self._configs = ConfigParser(inline_comment_prefixes='#')
        self.conf_dir = os.path.dirname(filepath)
        with open(file=filepath, mode='r') as fp:
            self._configs.read_file(fp)

    def check_config_validity(self):
        microservice, ignore_tsdb = False, False
        for section in self._configs.sections():
            for option in self._configs.options(section):
                if section == 'TSDB' and option == 'name' and self._configs.get(section, option) == 'ignore':
                    ignore_tsdb = True
                if section == 'AGENT' and option == 'distribute_mode' and self._configs.get(section, option) == 'true':
                    microservice = True
                valid, reason = check_config_validity(section, option, self._configs.get(section, option),
                                                      silent=True, microservice=microservice, ignore_tsdb=ignore_tsdb)
                if not valid:
                    raise ConfigSettingError('%s.' % reason)

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except (AttributeError, KeyError):
            return self._configs.__getattribute__(name)

    # Self-defined converters:
    def get(self, section, option, *args, **kwargs):
        """Faked get() for ConfigParser class."""
        kwargs.setdefault('fallback', None)
        try:
            value = self._configs.get(section, option, *args, **kwargs)
        except configparser.InterpolationSyntaxError as e:
            raise configparser.InterpolationSyntaxError(
                e.section, e.option, 'Found bad configuration: %s-%s.' % (e.section, e.option)
            ) from None

        if value is None:
            logging.warning('Not set %s-%s.', section, option)
            return value

        if value == NULL_TYPE:
            value = ''

        if option == "host":
            if "," in value:
                value = ",".join([uniform_ip(ip) for ip in value.split(",")])
            else:
                value = uniform_ip(value)

        if option == "master_url":
            res = list()
            urls = value.split(",")
            for url in urls:
                if "//" not in url:
                    res.append(url)
                    continue

                url_list = url.strip().split("//", 1)
                url_list[1] = uniform_instance(url_list[1])
                res.append("//".join(url_list))

            value = ",".join(res)

        if 'password' in option and value != '':
            s1, s2 = get_config_security_keys(os.path.join(self.conf_dir, constants.CIPHER_S1))
            iv = dynamic_config_get(IV_TABLE, '%s-%s' % (section, option))
            if value.startswith(ENCRYPTED_SIGNAL) and iv:
                real_value = value[len(ENCRYPTED_SIGNAL):]
            else:
                raise DontIgnoreThisError(
                    configparser.InterpolationSyntaxError(
                        section,
                        option,
                        'DBMind only supports encrypted password. Please try to set %s-%s '
                        'and initialize the configuration file.' % (section, option),
                    )
                )

            try:
                value = security.decrypt(s1, s2, iv, real_value)
            except Exception as e:
                raise InvalidCredentialException(
                    'An exception %s raised while decrypting.' % type(e)
                ) from None

        return value

    def getint(self, section, option, *args, **kwargs):
        """Faked getint() for ConfigParser class."""
        value = self._configs.get(section, option, *args, **kwargs)

        return int(value)

    def getfloat(self, section, option, *args, **kwargs):
        """Faked getfloat() for ConfigParser class."""
        value = self._configs.get(section, option, *args, **kwargs)

        return float(value)


class UpdateConfig(BaseConfig):
    def __init__(self, filepath):
        self.config = ConfigParser(inline_comment_prefixes=None)
        self.filepath = os.path.realpath(filepath)
        self.fp = None
        self.readonly = True

    def get(self, section, option):
        value = self.config.get(section, option)
        try:
            default_value, inline_comment = map(str.strip, value.rsplit('#', 1))
        except ValueError:
            default_value, inline_comment = value.strip(), ''
        if default_value == '':
            default_value = NULL_TYPE
        return default_value, inline_comment

    def set(self, section, option, value, inline_comment=''):
        self.readonly = False
        self.config.set(section, option, '%s  # %s' % (value, inline_comment))

    def sections(self):
        for section in self.config.sections():
            if section not in SKIP_LIST:
                comment = self.config.get('COMMENT', section, fallback='')
                yield section, comment

    def items(self, section):
        for option in self.config.options(section):
            default_value, inline_comment = self.get(section, option)
            yield option, default_value, inline_comment

    def __enter__(self):
        self.fp = open(file=self.filepath, mode='r+', errors='ignore')
        self.config.read_file(self.fp)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.readonly:
            # output configurations
            self.fp.truncate(0)
            self.fp.seek(0)
            self.fp.write(DBMIND_CONF_HEADER)
            self.config.write(self.fp)
            self.fp.flush()
        self.fp.close()


class GenerationConfig(ConfigParser):
    def __init__(self, filepath_src, filepath_dst):
        """This class copies a config file from source path
        and modifies it. Finally, it outputs the modified content
         to a destination path. """
        super().__init__(inline_comment_prefixes=None)
        self.filepath_src = filepath_src
        self.filepath_dst = filepath_dst

    def sections(self):
        """Hide sections existing in SKIP_LIST."""
        return filter(
            lambda s: s not in SKIP_LIST, super().sections()
        )

    def __enter__(self):
        with open(
                file=self.filepath_src,
                mode='r', errors='ignore') as fp:
            self.read_file(fp)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Add header comments (including license and notice).
        with open(file=self.filepath_dst, mode='w+') as fp:
            fp.write(DBMIND_CONF_HEADER)
            # write down user's configurations
            self.write(fp)


class DynamicConfig:
    """Forwarding to dynamic schema functions."""

    @staticmethod
    def get(*args, **kwargs):
        return dynamic_config_get(*args, **kwargs)

    @staticmethod
    def get_int_or_float(*args, **kwargs):
        value = dynamic_config_get(*args, **kwargs)
        return cast_to_int_or_float(value)

    @staticmethod
    def set(*args, **kwargs):
        return dynamic_config_set(*args, **kwargs)

    @staticmethod
    def list():
        return dynamic_configs_list()


def create_dynamic_configs(s1_file):
    """Create dynamic configuration schema and
    generate security keys."""
    utils.cli.write_to_terminal(
        'Starting to generate a dynamic config file...',
        color='green')
    create_dynamic_config_schema()
    s1_ = security.safe_random_string(16)
    s2_ = security.safe_random_string(16)
    with open(s1_file, 'w') as file_h:
        file_h.write(s1_)
    dynamic_config_set(IV_TABLE, 'cipher_s1', s1_)
    dynamic_config_set(IV_TABLE, 'cipher_s2', s2_)
    return s1_, s2_


def get_config_security_keys(s1_file):
    if os.path.exists(s1_file):
        s1 = open(s1_file).read()
    else:
        s1 = dynamic_config_get(IV_TABLE, 'cipher_s1')
    s2 = dynamic_config_get(IV_TABLE, 'cipher_s2')
    return s1, s2
