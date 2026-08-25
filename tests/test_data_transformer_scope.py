import pytest
from fastapi import HTTPException

from dbmind.service.web import data_transformer
from dbmind.service.web.context_manager import ACCESS_CONTEXT_NAME, set_access_context


def test_correlation_result_rejects_instance_outside_current_scope(monkeypatch):
    set_access_context(
        **{
            ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: ['10.0.0.1:5432'],
            ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ['10.0.0.1'],
        }
    )

    def fail_if_called():
        raise AssertionError('TSDB client should not be used for an out-of-scope instance.')

    monkeypatch.setattr(data_transformer.TsdbClientFactory, 'get_tsdb_client', fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        data_transformer.get_correlation_result(
            'os_cpu_usage', '10.0.0.2:5432', 1000, 2000
        )

    assert exc_info.value.status_code == 403


def test_correlation_result_allows_instance_inside_current_scope(monkeypatch):
    set_access_context(
        **{
            ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: ['10.0.0.1:5432'],
            ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ['10.0.0.1'],
        }
    )
    used_instances = []

    class MockClient:
        all_metrics = []
        scrape_interval = 15

    class MockSequence:
        def __len__(self):
            return 1

    class MockFetcher:
        def filter(self, **kwargs):
            return self

        def from_server(self, instance):
            used_instances.append(instance)
            return self

        def fetchone(self):
            return MockSequence()

    class MockWorker:
        calls = 0

        @classmethod
        def parallel_execute(cls, function, args):
            cls.calls += 1
            if cls.calls == 1:
                return [[]]
            return []

        @staticmethod
        def terminate(*args, **kwargs):
            pass

    monkeypatch.setattr(data_transformer.TsdbClientFactory, 'get_tsdb_client', lambda: MockClient())
    monkeypatch.setattr(data_transformer.dai, 'get_metric_sequence', lambda *args, **kwargs: MockFetcher())
    monkeypatch.setattr(data_transformer.global_vars, 'worker', MockWorker())

    assert data_transformer.get_correlation_result(
        'os_cpu_usage', '10.0.0.1:5432', 1000, 2000
    ) == {'os_cpu_usage from 10.0.0.1:5432': []}
    assert used_instances == ['10.0.0.1:5432']


def test_security_alarms_reject_instance_outside_current_scope(monkeypatch):
    set_access_context(
        **{
            ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: ['10.0.0.1:5432'],
            ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ['10.0.0.1'],
        }
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError('Alarm query should not be used for an out-of-scope instance.')

    monkeypatch.setattr(data_transformer, 'sqlalchemy_query_union_records_logic', fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        data_transformer.get_security_alarms(20, 1, '10.0.0.2:5432')

    assert exc_info.value.status_code == 403


def test_security_alarms_allows_instance_inside_current_scope(monkeypatch):
    set_access_context(
        **{
            ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: ['10.0.0.1:5432'],
            ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ['10.0.0.1'],
        }
    )
    captured = {}

    def mock_query_union_records_logic(**kwargs):
        captured.update(kwargs)
        return ['alarm']

    monkeypatch.setattr(
        data_transformer,
        'sqlalchemy_query_union_records_logic',
        mock_query_union_records_logic
    )

    assert data_transformer.get_security_alarms(20, 1, '10.0.0.1:5432') == ['alarm']
    assert captured['instances'] == ['10.0.0.1:5432']
    assert captured['alarm_type'] == data_transformer.ALARM_TYPES.SECURITY


def test_risk_analysis_rejects_instance_outside_current_scope(monkeypatch):
    set_access_context(
        **{
            ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: ['10.0.0.1:5432'],
            ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ['10.0.0.1'],
        }
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError('Risk analysis should not query an out-of-scope instance.')

    monkeypatch.setattr(data_transformer, 'early_warning', fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        data_transformer.risk_analysis(
            'os_cpu_usage', '10.0.0.2:5432', 1, 90, 10, 'role=primary'
        )

    assert exc_info.value.status_code == 403


def test_risk_analysis_allows_instance_inside_current_scope(monkeypatch):
    set_access_context(
        **{
            ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST: ['10.0.0.1:5432'],
            ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST: ['10.0.0.1'],
        }
    )
    captured = {}

    def mock_early_warning(metric, instance, _unused, warning_hours, upper, lower, labels):
        captured.update({
            'metric': metric,
            'instance': instance,
            'warning_hours': warning_hours,
            'upper': upper,
            'lower': lower,
            'labels': labels,
        })
        return ['warning']

    monkeypatch.setattr(data_transformer, 'early_warning', mock_early_warning)

    assert data_transformer.risk_analysis(
        'os_cpu_usage', '10.0.0.1:5432', 1, '90', '10', 'role=primary'
    ) == ['warning']
    assert captured == {
        'metric': 'os_cpu_usage',
        'instance': '10.0.0.1:5432',
        'warning_hours': 1,
        'upper': 90,
        'lower': 10,
        'labels': {'role': 'primary'},
    }
