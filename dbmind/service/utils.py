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

from dbmind import global_vars
from dbmind.common.types import Sequence

# Notice: 'DISTINGUISHING_INSTANCE_LABEL' is a magic string, i.e., our own name.
# Thus, not all collection agents (such as Prometheus's openGauss-exporter)
# distinguish different instance addresses through this one.
# Actually, this is a risky action for us, currently.
DISTINGUISHING_INSTANCE_LABEL = 'from_instance'


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
        return s.labels.get('instance')

    @staticmethod
    def exporter_ip(s: Sequence):
        address = SequenceUtils.exporter_address(s)
        if address:
            return address.split(':')[0]


def is_rpc_valid():
    # Determine whether RPC service is valid in the current environment.
    try:
        result = global_vars.agent_rpc_client.call('query_in_database',
                                                   'select 1',
                                                   'postgres',
                                                   return_tuples=True)
        return result[0][0] == 1
    except Exception:
        global_vars.agent_rpc_client = None
        logging.warning("Maybe the RPC service isn't started.", exc_info=True)
        return False


def is_tsdb_valid():
    # Determine whether TSDB is valid in the current environment.
    from dbmind.common.tsdb import TsdbClientFactory
    try:
        client = TsdbClientFactory.get_tsdb_client()
        return client.check_connection()
    except Exception:
        logging.warning("Maybe the TSDB service isn't started.", exc_info=True)
        return False


def get_master_instance_address():
    try:
        rows = global_vars.agent_rpc_client.call('query_in_database',
                                                 'SELECT inet_server_addr(), inet_server_port();',
                                                 'postgres',
                                                 return_tuples=True)
        instance_host, instance_port = rows[0][0], rows[0][1]
    except Exception as e:
        logging.warning("Maybe the RPC service isn't started.", exc_info=True)
        instance_host, instance_port = None, None

    return instance_host, instance_port
