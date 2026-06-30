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

from dbmind.service.multicluster import AgentProxy
from dbmind.service.web import data_transformer


class DummyRPC:
    def __init__(self, username, pwd):
        self.username = username
        self.pwd = pwd
        self.calls = []

    def call(self, funcname, *args, **kwargs):
        self.calls.append(('default', self.username, self.pwd, funcname, args, kwargs))
        return 'default-result'

    def call_with_another_credential(self, username, pwd, funcname, *args, **kwargs):
        self.calls.append(('override', username, pwd, funcname, args, kwargs))
        return 'override-result'


def test_agent_context_uses_thread_credentials_and_restores_default_identity():
    proxy = AgentProxy()
    rpc_a = DummyRPC('agent-a', 'agent-a-pwd')
    rpc_b = DummyRPC('agent-b', 'agent-b-pwd')
    proxy._agents = {'127.0.0.1:8000': rpc_a, '127.0.0.2:8000': rpc_b}
    proxy._cluster.record_one(['127.0.0.1:8000', '127.0.0.2:8000'])
    proxy._finalized = True

    assert proxy.switch_context('127.0.0.1:8000')

    with proxy.context('127.0.0.2:8000', 'caller', 'caller-pwd'):
        assert proxy.call('query_in_database', 'select 1') == 'override-result'
        assert rpc_b.calls[-1][:4] == ('override', 'caller', 'caller-pwd', 'query_in_database')

        assert proxy.switch_context('127.0.0.1:8000')
        assert proxy.call('query_in_database', 'select 2') == 'override-result'
        assert rpc_a.calls[-1][:4] == ('override', 'caller', 'caller-pwd', 'query_in_database')

    assert proxy.current_agent_addr() == '127.0.0.1:8000'
    assert proxy.call('query_in_database', 'select 3') == 'default-result'
    assert rpc_a.calls[-1][:4] == ('default', 'agent-a', 'agent-a-pwd', 'query_in_database')
    assert (rpc_a.username, rpc_a.pwd) == ('agent-a', 'agent-a-pwd')
    assert (rpc_b.username, rpc_b.pwd) == ('agent-b', 'agent-b-pwd')


def test_toolkit_get_query_plan_binds_authenticated_credentials(monkeypatch):
    class DummyContext:
        def __enter__(self):
            calls.append(('enter',))

        def __exit__(self, exc_type, exc_val, exc_tb):
            calls.append(('exit',))

    class DummyAgentProxy:
        def current_agent_addr(self):
            return '127.0.0.1:8000'

        def context(self, instance, username, password):
            calls.append(('context', instance, username, password))
            return DummyContext()

    calls = []
    monkeypatch.setattr(data_transformer.global_vars, 'agent_proxy', DummyAgentProxy())
    monkeypatch.setattr(
        data_transformer,
        'get_query_plan',
        lambda query, db_name, schema_name: calls.append(
            ('plan', query, db_name, schema_name)
        ) or 'plan-result'
    )

    result = data_transformer.toolkit_get_query_plan(
        'caller', 'caller-pwd',
        query='select * from t',
        db_name='postgres',
        schema_name='public'
    )

    assert result == 'plan-result'
    assert calls == [
        ('context', '127.0.0.1:8000', 'caller', 'caller-pwd'),
        ('enter',),
        ('plan', 'select * from t', 'postgres', 'public'),
        ('exit',)
    ]
