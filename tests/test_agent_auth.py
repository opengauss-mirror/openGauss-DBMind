import inspect

from dbmind.controllers import dbmind_core


def test_agent_topology_endpoints_require_authentication():
    for endpoint in (
        dbmind_core.get_all_agents,
        dbmind_core.update_agents,
        dbmind_core.update_agents_force,
    ):
        assert 'token' in inspect.signature(endpoint).parameters
