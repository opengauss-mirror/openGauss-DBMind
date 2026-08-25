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


class ApiClientException(Exception):
    """API client exception, raises when response status code != 200."""
    pass


class SetupError(Exception):
    def __init__(self, msg, *args, **kwargs):
        self.msg = msg


class InvalidCredentialException(Exception):
    pass


class SQLExecutionError(Exception):
    pass


class ConfigSettingError(Exception):
    pass


class DuplicateTableError(Exception):
    pass


class InvalidSequenceException(ValueError):
    pass


class DontIgnoreThisError(Exception):
    """DBMind has exception filtering. Use this exception type to avoid filtering."""
    pass


class InitializationError(Exception):
    """DBMind service setup initialization error."""
    pass


class ModeError(Exception):
    """DBMind service mode error, raised when:
    1. No token authorised V1 API called in distribute mode; 2. V2 API called in single mode.
    """
    pass


class CertCheckException(Exception):
    """Raised when SSL cert file is invalid."""
    pass


class WeakPasswordException(Exception):
    """Raised when using weak password."""
    pass


class WeakPrivateKeyException(Exception):
    """Raised when using unencrypted SSL private key."""
    pass
