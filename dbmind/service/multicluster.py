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

"""We introduce this file to indicate
which RPC agent we are connecting to.
And we can utilize `AgentProxy` to control multi-cluster.
"""

import json
import logging
import threading

from dbmind import global_vars
from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.rpc import RPCClient
from dbmind.common.rpc.server import DEFAULT_URI
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.utils.base import FixedDict
from dbmind.common.utils.checking import prepare_ip, split_ip_port
from dbmind.service.agent_adapter import AgentAdapter

REMOTE_INSTANCE_ADDRESSES_CENTRALIZED_STMT = """
SELECT 
    name,
    items[1] AS key,
    items[2] AS value
FROM (
    SELECT 
        name,
        pg_catalog.string_to_array(part, '=') AS items
    FROM (
        SELECT 
            name,
            pg_catalog.unnest(pg_catalog.string_to_array(setting, ' ')) AS part
        FROM 
            pg_catalog.pg_settings
        WHERE 
            name like 'replconninfo%'
            AND 
            pg_catalog.length(setting) > 0 )
    ) 
WHERE 
    key in ('remotehost', 'remoteport');
"""

REMOTE_INSTANCE_ADDRESSES_DISTRIBUTED_STMT = """
SELECT 
    node_type, 
    node_host, 
    node_port 
FROM 
    pg_catalog.pgxc_node;
"""

INET_SERVER_ADDR_STMT = """
SELECT 
    pg_catalog.inet_server_addr() AS host, 
    pg_catalog.inet_server_port() AS port;
"""

IS_PRIMARY_STMT = """
SELECT 
    NOT pg_catalog.pg_is_in_recovery() AS r;
"""


class RPCAddressError(ValueError):
    pass


def replace_sequence_ip(sequence):
    def replace_instances(ins_list, ins_map):
        for i, instance in enumerate(ins_list):
            res = split_ip_port(instance)
            if len(res) != 2:
                continue

            ip, port = res
            target_ip = ins_map.get(ip, ip)
            ins_list[i] = f"{prepare_ip(target_ip)}:{port}"

    ip_map = global_vars.ip_map
    if 'instance' not in sequence.labels:
        return

    from_instance_ip, from_instance_port = split_ip_port(sequence.labels['instance'])
    from_instance_ip_map = ip_map.get(from_instance_ip, {})
    instance_map = {db_ip: ft_ip for db_ip, ft_ip in from_instance_ip_map.items()}

    instances = sequence.labels.get("primary", "").split(",")
    replace_instances(instances, instance_map)
    sequence.labels["primary"] = ",".join(instances)

    instances = sequence.labels.get("standby", "").split(",")
    replace_instances(instances, instance_map)
    sequence.labels["standby"] = ",".join(instances)

    instances = sequence.labels.get("normal", "").split(",")
    replace_instances(instances, instance_map)
    sequence.labels["normal"] = ",".join(instances)

    instances = sequence.labels.get("abnormal", "").split(",")
    replace_instances(instances, instance_map)
    sequence.labels["abnormal"] = ",".join(instances)


class _ClusterDetails:
    def __init__(self):
        """This class is a cache utility
        for fast lookups."""
        self._clusters = list()
        # This data structure can evict its own element
        # according to max length.
        self._location_map = FixedDict(max_len=16)

    def record_one(self, cluster):
        self._clusters.append(cluster)

    def search_one(self, address):
        if address in self._location_map:
            return self._location_map[address]

        # Try our best to cache due O(N^2) time complexity.
        for l in self._clusters:
            for i in l:
                if address == i:
                    self._location_map[address] = l
                    return l

        # not found
        return []

    def obtain_all(self):
        """Allow to modify."""
        return self._clusters

    def clear(self):
        self._clusters.clear()
        self._location_map.clear()


def _get_remote_instance_addresses_from_tsdb(instance):
    tsdb = TsdbClientFactory.get_tsdb_client()
    cluster_sequence = tsdb.get_current_metric_value('opengauss_cluster_state')

    for _sequence in cluster_sequence:
        replace_sequence_ip(_sequence)
        if 'cn_state' not in _sequence.labels.keys() and _sequence.labels.get('primary', None) == instance:
            standby = _sequence.labels.get('standby', [])
            instances = standby.strip(',').split(',') if standby else []
            instances.append(instance)
            return True, instances
        if 'cn_state' in _sequence.labels.keys():
            cn_state = json.loads(_sequence.labels['cn_state'])
            central_nodes = [f"{prepare_ip(cn.get('ip'))}:{cn.get('port')}" for cn in cn_state]
            if instance in central_nodes:
                cluster_instances = central_nodes
                primary = _sequence.labels.get('primary', [])
                standby = _sequence.labels.get('standby', [])
                dn_primary = primary.strip(',').split(',') if primary else []
                dn_standby = standby.strip(',').split(',') if standby else []
                cluster_instances.extend(dn_primary)
                cluster_instances.extend(dn_standby)
                return True, cluster_instances
    return False, None


class AgentProxy(AgentAdapter):
    def __init__(self):
        """Control available agents
        and their contexts."""
        self._agents = {}
        self._cluster = _ClusterDetails()

        self._unchecked_agents = []  # candidates
        self._thread_context = threading.local()
        self._finalized = False
        # WARN: The following locks should be used in scenarios
        # that may involve **concurrent** interactions.
        # For scenarios where logically there is no concurrent
        # operation, this lock does not need to be used.
        # Scenarios that may involve concurrent operations,
        # (such as interaction with the client,
        # interaction with different background tasks, etc.);
        # scenarios that do not have concurrent operations,
        # such as the initialization phase (such as `init_rpc_with_config()`).
        self._lock = threading.Lock()
        self._autodiscover_args = {}

    @property
    def agents(self):
        return self._agents.copy()

    def set_autodiscover_connection_info(self,
                                         username, password,
                                         ssl_certfile=None,
                                         ssl_keyfile=None,
                                         ssl_key_password=None,
                                         ca_file=None):
        self._autodiscover_args = dict(
            username=username, password=password,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_key_password=ssl_key_password,
            ca_file=ca_file
        )

    def autodiscover(self, tsdb=None):
        if not self._autodiscover_args:
            logging.warning("[AgentProxy] AgentProxy performed autodiscover function "
                            "but hadn't set connection information.")
            return

        if not tsdb:
            tsdb = TsdbClientFactory.get_tsdb_client()

        urls = autodiscover_rely_on_tsdb(tsdb)

        # Get rid of Https URLs if SSL doesn't set.
        # only support one SSL under autodiscover mode
        enable_ssl = len(self._autodiscover_args.get('ssl_certfile') or ()) > 1

        for url in urls:
            if url.startswith('https') and not enable_ssl:
                logging.warning("[AgentProxy] Remove %s from agent URL list since SSL "
                                "information didn't set or set incorrectly.", url)
                continue

            self.agent_add(
                url=url,
                **self._autodiscover_args
            )

    def agent_finalize(self):
        """Check whether each agent is validated."""
        # just for re-finalizing
        self._agents.clear()
        self._cluster.clear()

        # The following is a check process.
        # We have put unchecked agent URLs into the
        # list `_unchecked_agents`. Then, we should
        # pop validated agents from `_unchecked_agents`.
        # And put them into another list `_agents` that contains
        # valid agents.
        i = 0
        while i < len(self._unchecked_agents):
            agent = self._unchecked_agents[i]
            if agent.heartbeat():
                # double check for primary instance
                if not _is_primary(agent):
                    i += 1
                    continue

                host, port = _get_agent_instance_details(agent)
                if not host:
                    i += 1
                    continue
                addr = f"{prepare_ip(host)}:{port}"
                self._agents[addr] = agent

                # Record cluster each node addresses.
                success, cluster_instances = _get_remote_instance_addresses_from_tsdb(addr)
                if not success:
                    cluster_instances = get_remote_instance_addresses(agent)
                # put primary instance first
                self._cluster.record_one(cluster_instances)

                self._unchecked_agents.pop(i)
                i -= 1
            else:
                logging.warning('Cannot ping the agent %s.', agent.url)
            i += 1
        logging.info('[AgentProxy] Valid monitoring instances '
                     'and exporter addresses are: %s.',
                     list(map(lambda e: (e[0], e[1].url), self._agents.items())))
        self._finalized = True

    def _try_to_finalize(self):
        """Lazy finalization."""
        # double check trick
        if not self._finalized:
            with self._lock:
                if self._finalized:
                    return
                self.agent_finalize()

    def agent_add(
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
        agent.set_timeout(seconds=10)
        self._unchecked_agents.append(agent)

    def agent_lightweight_update(self):
        """Move agents from current `self._agents` to
        `self._unchecked_agents`. Then, perform `self.agent_finalize()`
        to check.
        """
        # We want to block `self._try_to_finalize()`
        with self._lock:
            logging.info('[AgentProxy] Starting to update agents '
                         'in the mode of lightweight.')
            self._finalized = False
            self._unchecked_agents.extend(self._agents.values())
            self.agent_finalize()

    def agent_can_heavyweight_update(self):
        return bool(self._autodiscover_args)

    def agent_heavyweight_update(self):
        """Refresh active primary agents using autodiscover.
        This is heavyweight and relies on autodiscover enabled.
        """
        if not self.agent_can_heavyweight_update():
            return

        # We want to block `self._try_to_finalize()`
        with self._lock:
            logging.info('[AgentProxy] Starting to update agents '
                         'in the mode of heavyweight.')
            self._finalized = False
            self._unchecked_agents.clear()
            self.autodiscover()
            self.agent_finalize()

    def agent_get_all(self):
        """:return dict object. key indicates primary address \
        and value indicates other addresses in the cluster.
        """
        self._try_to_finalize()

        rv = {}
        for agent_addr in self._agents:
            rv[agent_addr] = self._cluster.search_one(agent_addr)
        return rv

    def switch_context(self, agent_addr, username=None, pwd=None):
        """Attach the later
        remote calls to a specific agent.
        :param agent_addr: openGauss database instance address, e.g., 127.0.0.1:6789
        :param username: openGauss database instance username;
        :param pwd: openGauss database instance password;
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
        if username and pwd:
            self._thread_context.rpc.pwd = pwd
            self._thread_context.rpc.username = username
        self._thread_context.agent_addr = agent_addr
        self._thread_context.cluster = self._cluster.search_one(agent_addr)
        return True

    def current_rpc(self):
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
            self._thread_context.cluster = self._cluster.search_one(agent_addr)
            return self._thread_context.rpc
        return None

    def current_agent_addr(self):
        if self.current_rpc():
            return self._thread_context.agent_addr
        # Caller should handle this return value.
        return None

    def current_cluster_instances(self):
        if self.current_rpc():
            return self._thread_context.cluster
        return []

    def call(self, funcname, *args, **kwargs):
        """If a caller directly use this
        method, we delegate a cached RPCClient by
        thread local value."""
        rpc = self.current_rpc()
        if not rpc:
            raise ValueError(
                'Not switched to a valid RPC, '
                'cannot perform remote call.'
            )
        return rpc.call(funcname, *args, **kwargs)

    def __iter__(self):
        self._try_to_finalize()

        return iter(self.agents.items())

    def context(self, instance_address, username=None, pwd=None):
        outer = self
        old = outer.current_rpc()

        class Inner:
            def __init__(self, addr):
                self.addr = addr

            def __enter__(self):
                if not outer.switch_context(self.addr, username, pwd):
                    raise RPCAddressError('Cannot switch to this RPC address %s' % instance_address)

            def __exit__(self, exc_type, exc_val, exc_tb):
                outer.switch_context(old)

        return Inner(instance_address)

    def has(self, instance_address):
        return instance_address in self._agents

    def get(self, instance_address):
        return self._agents.get(instance_address, None)


def autodiscover_rely_on_tsdb(tsdb):
    """Autodiscover agents using TSDB. This relies on
    openGauss-exporter to enable RPC agent and support
    the metric `opengauss_exporter_fixed_info`.

    :return dict object. Its key indicates agent address
    and value indicates if this exporter monitors a primary instance:
    True means primary, False means standby, None means unknown.
    """

    sequences = tsdb.get_current_metric_value(
        'opengauss_exporter_fixed_info',
        label_config={'rpc': 'True', 'primary': 'True'},  # Currently, we only use an RPC enabled exporter.
    )
    rv = set()
    for s in sequences:
        # fixed key name, refer to openGauss exporter component. And override the following scenario
        host = prepare_ip(split_ip_port(s.labels['instance'])[0])
        if '/0.0.0.0' in s.labels['url']:
            rv.add(s.labels['url'].replace('/0.0.0.0', f"/{host}"))
        elif '/[::]' in s.labels['url']:
            rv.add(s.labels['url'].replace('/[::]', f"/{host}"))
        else:
            rv.add(s.labels['url'])

    return rv


def _is_primary(rpc: RPCClient):
    """Return if the instance is primary.
    If this function cannot connect to the RPC,
    regarding as False.
    """
    try:
        rows = rpc.call('query_in_postgres', IS_PRIMARY_STMT)
        return rows[0]['r']
    except Exception as e:
        logging.exception(e)
        return False


def _get_agent_instance_details(rpc: RPCClient):
    try:
        # Firstly, try to use the less overhead method.
        # The following method depends on the openGauss exporter's logic, not
        # the RPCClient logic.
        # Try to access /info URI below.
        # This URI /info is only used in the new version.
        get_info_url = rpc.url[:-len(DEFAULT_URI)] + '/info'
        with create_requests_session(
            username=rpc.username,
            password=rpc.pwd,
            ssl_context=rpc._ssl_context
        ) as session:
            response = session.get(get_info_url)

        if response.ok and response.json().get('monitoring'):
            split_r = split_ip_port(response.json().get('monitoring'))
            if len(split_r) == 2:
                return split_r

        # Above method failed, try to the below method.
        rows = rpc.call('query_in_postgres', INET_SERVER_ADDR_STMT)
        instance_host, instance_port = rows[0]['host'], rows[0]['port']
    except Exception as e:
        logging.warning('Failed to connect the RPC server due to %s %s.',
                        type(e), e, exc_info=True)
        instance_host, instance_port = None, None

    return instance_host, instance_port


def _get_inet_server_addr(rpc: RPCClient):
    rows = rpc.call('query_in_postgres', INET_SERVER_ADDR_STMT)
    instance_host, instance_port = rows[0]['host'], rows[0]['port']
    return [f'{prepare_ip(instance_host)}:{instance_port}']


def _get_remote_instance_addresses_centralized(rpc: RPCClient):
    instances = _get_inet_server_addr(rpc)
    rows = rpc.call('query_in_postgres', REMOTE_INSTANCE_ADDRESSES_CENTRALIZED_STMT)
    if not rows:
        return instances
    # extract other standby instance addresses
    tmp = {}
    for row in rows:
        name, key, value = row['name'], row['key'], row['value']
        if key == 'remotehost':
            tmp[name] = value + tmp.get(name, '')
        elif key == 'remoteport':
            # database server port is this HA port minus 1
            tmp[name] = f"{prepare_ip(tmp.get(name, ''))}:{int(value) - 1}"
    instances.extend(list(tmp.values()))
    return instances


def _get_remote_instance_addresses_distributed(rpc: RPCClient):
    rows = rpc.call('query_in_postgres', REMOTE_INSTANCE_ADDRESSES_DISTRIBUTED_STMT)
    if not rows:
        return _get_inet_server_addr(rpc)
    instances = [f'{prepare_ip(row.get("node_host"))}:{row.get("node_port")}' for row in rows]
    return instances


def get_remote_instance_addresses(rpc: RPCClient):
    instances = _get_remote_instance_addresses_distributed(rpc)
    if len(instances) <= 1:
        return _get_remote_instance_addresses_centralized(rpc)
    return instances
