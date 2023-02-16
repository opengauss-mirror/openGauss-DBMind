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
import logging
import threading

from dbmind import global_vars
from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.rpc.client import RPCClient, DEFAULT_URI
from dbmind.common.types import Sequence
from dbmind.constants import DISTINGUISHING_INSTANCE_LABEL, EXPORTER_INSTANCE_LABEL


class SequenceUtils:
    @staticmethod
    def from_server(s: Sequence):
        distinguishing = s.labels.get(DISTINGUISHING_INSTANCE_LABEL)
        if distinguishing:
            return distinguishing
        # If the metric does not come from reprocessing-exporter,
        # then return the exporter IP directly.
        return SequenceUtils.exporter_ip(s)

    @staticmethod
    def exporter_address(s: Sequence):
        return s.labels.get(EXPORTER_INSTANCE_LABEL)

    @staticmethod
    def exporter_ip(s: Sequence):
        address = SequenceUtils.exporter_address(s)
        if address:
            return address.split(':')[0]


class AgentProxy:

    class RPCAddressError(ValueError):
        pass

    def __init__(self):
        self._agents = {}
        self._cluster_instances = {}

        self._unchecked_agents = []  # candidates
        self._thread_context = threading.local()
        self._finalized = False
        self._lock = threading.Lock()

    def finalize_agents(self):
        """Check whether each agent is validated."""
        # just for re-finalizing
        self._agents.clear()
        self._cluster_instances.clear()

        i = 0
        while i < len(self._unchecked_agents):
            agent = self._unchecked_agents[i]
            if agent.heartbeat():
                host, port = _get_agent_instance_address(agent)
                if not host:
                    continue
                addr = '%s:%s' % (host, port)
                self._agents[addr] = agent

                self._cluster_instances[addr] = _get_remote_instance_addresses(agent)
                self._cluster_instances[addr].append(addr)

                self._unchecked_agents.pop(i)
                i -= 1
            else:
                logging.warning(
                    'Cannot ping the agent %s.', agent.url
                )
            i += 1
        logging.info('Valid monitoring instances and exporter addresses are: %s.',
                     list(map(lambda e: (e[0], e[1].url), self._agents.items())))
        self._finalized = True

    def _try_to_finalize(self):
        """Lazy finalization."""
        with self._lock:
            if not self._finalized:
                self.finalize_agents()

    def add_agent(
            self, url, username, password,
            ssl_certfile=None, ssl_keyfile=None,
            ssl_key_password=None, ca_file=None
    ):
        agent = RPCClient(
            url,
            username=username,
            pwd=password,
            ssl_cert=ssl_certfile,
            ssl_key=ssl_keyfile,
            ssl_key_password=ssl_key_password,
            ca_file=ca_file
        )
        self._unchecked_agents.append(agent)

    def get_all_agents(self):
        self._try_to_finalize()

        rv = {}
        for agent_addr in self._agents:
            rv[agent_addr] = self._cluster_instances[agent_addr]
        return rv

    def switch_context(self, agent_addr):
        """Attach the later
        remote calls to a specific agent.
        :param agent_addr: openGauss database instance address, e.g., 127.0.0.1:6789
        :return return True for success, False meaning failure.
        """
        if not agent_addr:
            self._thread_context.rpc = None
            self._thread_context.agent_addr = None
            self._thread_context.cluster = None
            return True

        self._try_to_finalize()
        if agent_addr not in self._agents:
            return False
        rpc = self._agents[agent_addr]
        self._thread_context.rpc = rpc
        self._thread_context.agent_addr = agent_addr
        self._thread_context.cluster = self._cluster_instances[agent_addr]
        return True

    def _current_rpc(self):
        self._try_to_finalize()

        if (hasattr(self._thread_context, 'rpc')
                and self._thread_context.rpc is not None):
            return self._thread_context.rpc
        if len(self._agents) == 0:
            return None
        elif len(self._agents) == 1:
            # there is only one element, set as default and return it directly.
            agent_addr = next(iter(self._agents.keys()))
            rpc = self._agents[agent_addr]
            self._thread_context.rpc = rpc
            self._thread_context.agent_addr = agent_addr
            self._thread_context.cluster = self._cluster_instances[agent_addr]
            return self._thread_context.rpc
        return None

    def current_rpc(self):
        return self._current_rpc()

    def current_agent_addr(self):
        if self._current_rpc():
            return self._thread_context.agent_addr

    def current_cluster_instances(self):
        if self._current_rpc():
            return self._thread_context.cluster

    def call(self, funcname, *args, **kwargs):
        """If a caller directly use this
        method, we delegate a cached RPCClient by
        thread local value."""
        rpc = self._current_rpc()
        if not rpc:
            raise ValueError('Not switched to a valid RPC, cannot perform remote call.')
        return rpc.call(funcname, *args, **kwargs)

    def __iter__(self):
        self._try_to_finalize()

        return iter(self._agents.items())

    def context(self, instance_address):
        outer = self
        old = outer.current_rpc

        class Inner:
            def __init__(self, addr):
                self.addr = addr

            def __enter__(self):
                if not outer.switch_context(self.addr):
                    raise AgentProxy.RPCAddressError(
                        'Cannot switch to this RPC address %s' % instance_address
                    )

            def __exit__(self, exc_type, exc_val, exc_tb):
                outer.switch_context(old)

        return Inner(instance_address)

    def available_agents(self):
        return len(self._agents)

    def has(self, instance_address):
        return instance_address in self._agents

    def get(self, instance_address):
        return self._agents.get(instance_address, None)


def _get_agent_instance_address(rpc: RPCClient):
    try:
        # Firstly, try to use the less overhead method.
        # The following method depends on the openGauss exporter's logic, not
        # the RPCClient logic.
        # Try to access /info URI below.
        # This URI /info is only used in the new version.
        get_info_url = rpc.url[:-len(DEFAULT_URI)] + '/info'  # e.g., http://foo/info
        with create_requests_session() as session:
            response = session.get(get_info_url)
        if response.ok and response.json().get('monitoring'):
            split_r = response.json().get('monitoring').split(':')
            if len(split_r) == 2:
                return split_r

        # Above method failed, try to the below method.
        rows = rpc.call('query_in_postgres',
                        'SELECT inet_server_addr() as host, inet_server_port() '
                        'as port;')
        instance_host, instance_port = rows[0]['host'], rows[0]['port']
    except Exception as e:
        logging.warning('Failed to connect the RPC server due to %s %s.',
                        type(e), e, exc_info=True)
        instance_host, instance_port = None, None

    return instance_host, instance_port


def get_agent_instance_address():
    if (not global_vars.agent_proxy or
            not global_vars.agent_proxy.current_rpc()):
        return None, None
    return _get_agent_instance_address(global_vars.agent_proxy.current_rpc())


def _get_remote_instance_addresses(rpc: RPCClient):
    stmt = """
    SELECT name,
           items[1] AS KEY,
           items[2] AS value
    FROM
      (SELECT name,
              string_to_array(part, '=') AS items
       FROM
         (SELECT name,
                 unnest(string_to_array(setting, ' ')) AS part
          FROM pg_settings
          WHERE name like 'replconninfo%'
            AND length(setting) > 0 )) WHERE KEY in ('remotehost', 'remoteport');
    """
    r = rpc.call('query_in_postgres',
                 stmt)
    if not r:
        return []
    # extract other standby instance addresses
    tmp = {}
    for row in r:
        name, key, value = row['name'], row['key'], row['value']
        if key == 'remotehost':
            tmp[name] = value + tmp.get(name, '')
        elif key == 'remoteport':
            # database server port is this HA port minus 1
            tmp[name] = tmp.get(name, '') + f':{int(value) - 1}'
    return list(tmp.values())


def get_remote_instance_addresses():
    if (not global_vars.agent_proxy or
            not global_vars.agent_proxy.current_rpc()):
        return None, None
    return _get_remote_instance_addresses(global_vars.agent_proxy.current_rpc())
