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
import json as json_utils
import re
import threading
from urllib.parse import urlparse

import requests

from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.types.ssl import SSLContext
from dbmind.common.utils.exporter import is_exporter_alive
from .base import RPCRequest, RPCResponse
from .errors import RPCExecutionError, RPCConnectionError
from .server import DEFAULT_URI, HEARTBEAT_FLAG, AUTH_FLAG

standard_rpc_url_pattern = re.compile('(https?)://[-A-Za-z0-9+&@#%?=~_|!:,.;\[\]]+/[-A-Za-z0-9]+')
rpc_endpoint_pattern = re.compile('(https?)://[-A-Za-z0-9+&@#%?=~_|!:,.;\[\]]+/?$')


class RPCClient:
    def __init__(self, url, username=None, pwd=None,
                 ssl_cert=None, ssl_key=None, ssl_key_password=None, ca_file=None):
        if not (url.startswith('https://') or url.startswith('http://')):
            raise ValueError(url)

        if re.match(standard_rpc_url_pattern, url):
            self.url = url
        elif re.match(rpc_endpoint_pattern, url):
            # The reason why we strip slash is to avoid wrong URL.
            self.url = url.rstrip('/') + DEFAULT_URI
        else:
            raise ValueError('Invalid url format: %s.' % url)

        self.username = username
        self.pwd = pwd
        self.timeout = None

        self._ssl_context = SSLContext(ssl_cert, ssl_key, ssl_key_password, ca_file)

        self._lock = threading.Lock()

    def set_timeout(self, seconds):
        self.timeout = seconds

    def _post(self, url, data=None, json=None, **kwargs):
        with create_requests_session(
                ssl_context=self._ssl_context) as session:
            return session.post(url, data, json, **kwargs)

    def _call_without_lock(self, funcname, *args, **kwargs):
        """Internal private implementation."""
        req = RPCRequest(self.username, self.pwd, funcname, args, kwargs)
        try:
            recv = self._post(self.url, json=req.json(), timeout=self.timeout)
        except requests.exceptions.ConnectionError as e:
            raise RPCConnectionError(e.strerror or 'Cannot access to %s.' % self.url)

        if not recv.ok:
            raise RPCExecutionError(recv.reason)

        try:
            res = RPCResponse.from_json(recv.json())
        except json_utils.decoder.JSONDecodeError as e:
            raise RPCExecutionError('RPC Client received invalid content: %s, which cannot '
                                    'decode to JSON because %s.' %
                                    (recv.text, e))
        if not res.success:
            raise RPCExecutionError(res.exception)
        return res.result

    def call(self, funcname, *args, **kwargs):
        """Send request to remote server and fetch response from it.

        :exception RPCExecutionError: raise this exception while remote server occurred error. This exception will give
        the details.
        :param funcname: the name of function that registered to RPCFunctionRegister at RPC server.
        :param args: the list of parameters.
        :param kwargs: the dict of parameters.
        :return: the execution result, not including any wrappers.
        """
        with self._lock:
            return self._call_without_lock(funcname, *args, **kwargs)

    def call_with_another_credential(
            self, username, password,
            funcname, *args, **kwargs
    ):
        with self._lock:
            old_username = self.username
            old_pwd = self.pwd

            self.username = username
            self.pwd = password

            try:
                return self._call_without_lock(funcname, *args, **kwargs)
            finally:
                self.username = old_username
                self.pwd = old_pwd

    def heartbeat(self):
        try:
            return self.call(HEARTBEAT_FLAG) == 'ok'
        except (RPCConnectionError, RPCExecutionError):
            return False

    def handshake(self, username=None, password=None, receive_exception=False):
        with self._lock:
            if None in (username, password):
                username = self.username
                password = self.pwd

            try:
                old_username = self.username
                old_pwd = self.pwd

                self.username = username
                self.pwd = password

                try:
                    return (self._call_without_lock(AUTH_FLAG) == 'ok'), None
                finally:
                    self.username = old_username
                    self.pwd = old_pwd

            except RPCConnectionError as e:
                if receive_exception:
                    raise e
                return False, 'Can not connect to the RPC server, please check your opengauss_exporter.'
            except RPCExecutionError as e:
                return False, str(e)


def ping_rpc_url(url):
    p = urlparse(url)
    if not p.hostname:
        return False

    if p.port is None:
        if p.scheme == 'http':
            p.port = 80
        else:
            p.port = 443
    return is_exporter_alive(p.hostname, p.port)
