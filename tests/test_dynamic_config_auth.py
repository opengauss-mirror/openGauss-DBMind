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

import types

import pytest
from fastapi import HTTPException

from dbmind.controllers import dbmind_core
from dbmind.service.web import data_transformer


class DummyDynamicConfigs:
    def __init__(self):
        self.calls = []

    def set(self, *args):
        self.calls.append(args)


class DummyRPC:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def call_with_another_credential(self, username, password, funcname, stmt):
        self.calls.append((username, password, funcname, stmt))
        return self.rows


class DummyAgentProxy:
    def __init__(self, rpc):
        self.rpc = rpc

    def current_rpc(self):
        return self.rpc


def _raw_controller(func):
    return func.__wrapped__.__wrapped__


def test_dynamic_config_privilege_accepts_monitoradmin(monkeypatch):
    rpc = DummyRPC([{'rolmonitoradmin': 't', 'rolsystemadmin': 'f'}])
    monkeypatch.setattr(data_transformer.global_vars, 'agent_proxy', DummyAgentProxy(rpc))

    assert data_transformer.has_dynamic_config_write_privilege('alice', 'secret')
    assert rpc.calls[0][:3] == ('alice', 'secret', 'query_in_postgres')


def test_dynamic_config_privilege_rejects_plain_user(monkeypatch):
    rpc = DummyRPC([{'rolmonitoradmin': False, 'rolsystemadmin': False}])
    monkeypatch.setattr(data_transformer.global_vars, 'agent_proxy', DummyAgentProxy(rpc))

    assert not data_transformer.has_dynamic_config_write_privilege('bob', 'secret')


def test_set_setting_rejects_non_admin_user(monkeypatch):
    configs = DummyDynamicConfigs()
    monkeypatch.setattr(dbmind_core, 'oauth2', types.SimpleNamespace(credential=('bob', 'secret')))
    monkeypatch.setattr(dbmind_core.global_vars, 'dynamic_configs', configs)
    monkeypatch.setattr(
        dbmind_core.data_transformer,
        'has_dynamic_config_write_privilege',
        lambda username, password: False
    )

    with pytest.raises(HTTPException) as exc_info:
        _raw_controller(dbmind_core.set_setting)('self_optimization', 'max_elapsed_time', '0')

    assert exc_info.value.status_code == 403
    assert configs.calls == []


def test_update_dynamic_config_allows_admin_user(monkeypatch):
    configs = DummyDynamicConfigs()
    monkeypatch.setattr(dbmind_core, 'oauth2', types.SimpleNamespace(credential=('admin', 'secret')))
    monkeypatch.setattr(dbmind_core.global_vars, 'dynamic_configs', configs)
    monkeypatch.setattr(
        dbmind_core.data_transformer,
        'has_dynamic_config_write_privilege',
        lambda username, password: True
    )
    item = dbmind_core.UpdateDynamicConfig(
        configname='self_optimization',
        config_dict={'max_elapsed_time': '60'}
    )

    result = _raw_controller(dbmind_core.update_dynamic_config)(item)

    assert result == 'success'
    assert configs.calls == [('self_optimization', 'max_elapsed_time', '60')]
