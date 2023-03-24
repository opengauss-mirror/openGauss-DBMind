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
    SKIP_LIST, NULL_TYPE, ENCRYPTED_SIGNAL,
    DBMIND_CONF_HEADER,
    check_config_validity)
from dbmind.common import security
from dbmind.common.exceptions import (
    InvalidCredentialException, ConfigSettingError)
from dbmind.common.utils import (ExceptionCatcher,
                                 cast_to_int_or_float)
from dbmind.metadatabase.dao.dynamic_config import (
    dynamic_config_get, dynamic_config_set,
    dynamic_configs_list)

from .base_configurator import BaseConfig


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
        with open(file=filepath, mode='r') as fp:
            self._configs.read_file(fp)

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
        if 'password' in option and value != '':
            s1 = dynamic_config_get('dbmind_config', 'cipher_s1')
            s2 = dynamic_config_get('dbmind_config', 'cipher_s2')
            iv = dynamic_config_get('iv_table', '%s-%s' % (section, option))
            if value.startswith(ENCRYPTED_SIGNAL):
                real_value = value[len(ENCRYPTED_SIGNAL):]
            else:
                raise ExceptionCatcher.DontIgnoreThisError(configparser.InterpolationSyntaxError(
                    section, option, 'DBMind only supports encrypted password. '
                                     'Please set %s-%s and initialize the configuration file.' % (section, option),
                ))

            try:
                value = security.decrypt(s1, s2, iv, real_value)
            except Exception as e:
                raise InvalidCredentialException(
                    'An exception %s raised while decrypting.' % type(e)
                ) from None

        else:
            valid, reason = check_config_validity(section, option, value, silent=True)
            if not valid:
                raise ConfigSettingError('DBMind failed to start due to %s.' % reason)

        return value

    def getint(self, section, option, *args, **kwargs):
        """Faked getint() for ConfigParser class."""
        value = self._configs.get(section, option, *args, **kwargs)
        valid, reason = check_config_validity(section, option, value, silent=True)
        if not valid:
            raise ConfigSettingError('DBMind failed to start due to %s.' % reason)

        return int(value)

    def getfloat(self, section, option, *args, **kwargs):
        """Faked getfloat() for ConfigParser class."""
        value = self._configs.get(section, option, *args, **kwargs)
        valid, reason = check_config_validity(section, option, value, silent=True)
        if not valid:
            raise ConfigSettingError('DBMind failed to start due to %s.' % reason)

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
