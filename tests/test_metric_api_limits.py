import pytest
from fastapi import HTTPException

from dbmind.controllers import dbmind_core


def test_legacy_metric_sequence_rejects_too_large_time_range(monkeypatch):
    def fail_if_called(*args, **kwargs):
        raise AssertionError('Metric query should not be executed after budget rejection.')

    monkeypatch.setattr(dbmind_core.data_transformer, 'get_metric_sequence', fail_if_called)

    with pytest.raises(HTTPException) as exc_info:
        dbmind_core.get_metric_sequence(
            'os_cpu_usage',
            from_timestamp=0,
            to_timestamp=dbmind_core.MAX_METRIC_QUERY_RANGE_MS + 1
        )

    assert exc_info.value.status_code == 400


def test_legacy_metric_sequence_forwards_regex_and_limit(monkeypatch):
    captured = {}

    def mock_get_metric_sequence(*args, **kwargs):
        captured['args'] = args
        captured['kwargs'] = kwargs
        return ['sequence']

    monkeypatch.setattr(dbmind_core.data_transformer, 'get_metric_sequence', mock_get_metric_sequence)

    dbmind_core.get_metric_sequence(
        'os_cpu_usage',
        instance='10.0.0.1',
        from_timestamp=0,
        to_timestamp=1000,
        regrex=True,
        regrex_labels='role=primary',
        limit=9999,
    )

    assert captured['args'][:4] == ('os_cpu_usage', '10.0.0.1', 0, 1000)
    assert captured['kwargs']['regex'] is True
    assert captured['kwargs']['regex_labels'] == 'role=primary'
    assert captured['kwargs']['result_limit'] == dbmind_core.MAX_METRIC_QUERY_SERIES
