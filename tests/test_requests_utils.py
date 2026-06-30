# Copyright (c) 2026 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
# See the Mulan PSL v2 for more details.

import functools
import importlib.util
from pathlib import Path
import types


def _create_requests_session():
    module_path = Path(__file__).resolve().parents[1] / 'dbmind' / 'common' / 'http' / 'requests_utils.py'
    if not module_path.exists():
        module_path = Path.cwd() / 'dbmind' / 'common' / 'http' / 'requests_utils.py'
    spec = importlib.util.spec_from_file_location('requests_utils', str(module_path))
    requests_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(requests_utils)
    return requests_utils.create_requests_session


def _ssl_context(ca_file):
    return types.SimpleNamespace(
        ssl_certfile='/path/to/client.crt',
        ssl_keyfile='/path/to/client.key',
        ssl_keyfile_password=None,
        ssl_ca_file=ca_file,
    )


def test_create_requests_session_uses_default_tls_verification_for_empty_ca():
    create_requests_session = _create_requests_session()
    session = create_requests_session(ssl_context=_ssl_context(''))

    assert isinstance(session.request, functools.partial)
    assert session.request.keywords['verify'] is True


def test_create_requests_session_preserves_configured_ca_bundle():
    create_requests_session = _create_requests_session()
    session = create_requests_session(ssl_context=_ssl_context('/path/to/ca.pem'))

    assert isinstance(session.request, functools.partial)
    assert session.request.keywords['verify'] == '/path/to/ca.pem'
