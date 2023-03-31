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
import logging

from dbmind import global_vars
from dbmind.service.multicluster import RPCAddressError
from dbmind.components.xtuner.tuner.character import AbstractMetric
from dbmind.components.xtuner.tuner.recommend import recommend_knobs as rk
from dbmind.components.xtuner.tuner.utils import cached_property
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils


def _fetch_value_by_rpc(sql, database='postgres', default_val=0):
    # Ensure that RPC collects correct data
    try:
        result = global_vars.agent_proxy.call('query_in_database',
                                              sql,
                                              database,
                                              return_tuples=True)
        return result[0][0]
    except Exception as e:
        logging.warning(
            'Failed to use RPC in the KnobRecommendation.', exc_info=e
        )
        return default_val


def _fetch_all_value(metric_name, from_server=None, **condition):
    if from_server is not None:
        seqs = dai.get_latest_metric_value(metric_name=metric_name).from_server(
            f"{from_server}").filter(**condition).fetchall()
    else:
        seqs = dai.get_latest_metric_value(metric_name=metric_name).filter(**condition).fetchall()
    return seqs


def _fetch_one_value(metric_name, from_server=None, default_val=-1, **condition):
    if from_server is None:
        seq = dai.get_latest_metric_value(metric_name=metric_name).from_server(
            f"{from_server}").filter(**condition).fetchone()
    else:
        seq = dai.get_latest_metric_value(metric_name=metric_name).filter(**condition).fetchone()
    if seq.values:
        return seq.values[0]
    return default_val


def _fetch_one_value_by_host(metric_name, host, default_val=0, **condition):
    seqs = dai.get_latest_metric_value(metric_name=metric_name).filter(**condition).fetchall()
    for seq in seqs:
        if host in SequenceUtils.from_server(seq):
            return seq.values[0]
    return default_val


def get_database_addresses():
    seqs = _fetch_all_value("pg_node_info_uptime")
    database_addresses = set()
    for seq in seqs:
        database_addresses.add(SequenceUtils.from_server(seq))
    return database_addresses


def recommend_knobs():
    addresses = get_database_addresses()
    result = dict()
    metric = RPCAndTSDBMetric()
    for address in addresses:
        try:
            with global_vars.agent_proxy.context(address):
                host, port = address.split(':')
                metric.set_host(host)
                metric.set_port(port)
                metric.set_address()
                knobs = rk("recommend", metric)
                result[address] = [knobs, metric.to_dict()]
        except RPCAddressError as e:
            logging.warning(
                'Cannot recommend knobs for the address %s because %s.',
                address, str(e)
            )
        except Exception as e:
            logging.warning(
                'Cannot recommend knobs for the '
                'address %s maybe because of information lack.', address,
                exc_info=e
            )
    return result


class RPCAndTSDBMetric(AbstractMetric):

    def __init__(self):
        self.is_rpc_valid = False
        self.database_host = None
        self.database_port = None
        self.database_address = None
        AbstractMetric.__init__(self)

    def __getitem__(self, item):
        """Get GUC from database instance."""
        value = self.fetch_current_guc_value(item)
        try:
            return float(value)
        except ValueError:
            return value

    @cached_property
    def most_xact_db(self):
        seqs = _fetch_all_value("pg_db_xact_commit", from_server=self.database_address)
        database = 'postgres'
        max_xact_commit = -float("inf")
        for seq in seqs:
            if seq.values:
                xact_commit = seq.values[0]
                if xact_commit > max_xact_commit:
                    database = seq.labels.get("datname")
                    max_xact_commit = xact_commit
        return database

    def set_host(self, database_host):
        self.database_host = database_host

    def set_port(self, database_port):
        self.database_port = database_port

    def set_address(self):
        self.database_address = "%s:%s" % (self.database_host, self.database_port)

    def get_one_value_from_seqs_according_to_database_host(self, seqs, default_val=None):
        val = default_val
        for seq in seqs:
            host = SequenceUtils.from_server(seq).split(":")[0]
            val = seq.values[0]
            if self.database_host == host:
                return val
        return val

    def fetch_current_guc_value(self, guc_name):
        result = _fetch_one_value("pg_settings_setting", from_server=self.database_address, name=guc_name)
        return result

    @property
    def cache_hit_rate(self):
        # You could define used internal state here.
        # this is a demo, cache_hit_rate, we will use it while tuning shared_buffer.
        pg_db_blks_hit = _fetch_one_value("pg_db_blks_hit", from_server=self.database_address)
        pg_db_blks_read = _fetch_one_value("pg_db_blks_read", from_server=self.database_address)
        cache_hit_rate = pg_db_blks_hit / (pg_db_blks_hit + pg_db_blks_read + 0.001)
        return cache_hit_rate

    @cached_property
    def is_64bit(self):
        seq = _fetch_one_value_by_host("node_uname_info", host=self.database_host, machine="x86_64")
        return seq != 0

    @property
    def uptime(self):
        return _fetch_one_value("pg_node_info_uptime", from_server=self.database_address)

    @property
    def current_connections(self):
        result = 0
        seqs = _fetch_all_value("pg_stat_activity_count", from_server=self.database_address)
        for seq in seqs:
            value = seq.values[0] if seq.values else 0
            result += value
        return result

    @property
    def average_connection_age(self):
        return _fetch_one_value("pg_stat_activity_p95_state_duration", from_server=self.database_address)

    @property
    def all_database_size(self):
        return _fetch_one_value("pg_database_all_size", from_server=self.database_address) / 1024  # unit: kB

    @property
    def current_prepared_xacts_count(self):
        return _fetch_one_value("pg_prepared_xacts_count", from_server=self.database_address)

    @property
    def current_locks_count(self):
        return _fetch_one_value("pg_lock_count", from_server=self.database_address)

    @property
    def checkpoint_proactive_triggering_ratio(self):
        return _fetch_one_value("pg_stat_bgwriter_checkpoint_proactive_triggering_ratio",
                                from_server=self.database_address, default_val=0)

    @property
    def checkpoint_avg_sync_time(self):
        return _fetch_one_value("pg_stat_bgwriter_checkpoint_avg_sync_time", from_server=self.database_address)

    @property
    def shared_buffer_heap_hit_rate(self):
        if self.is_rpc_valid:
            stmt = "select pg_catalog.sum(heap_blks_hit)*100 / (pg_catalog.sum(heap_blks_read) + " \
                   "pg_catalog.sum(heap_blks_hit)+1) from pg_statio_user_tables;"
            return float(_fetch_value_by_rpc(stmt, database=self.most_xact_db, default_val=100))
        return 100.0

    @property
    def shared_buffer_toast_hit_rate(self):
        if self.is_rpc_valid:
            stmt = "select pg_catalog.sum(toast_blks_hit)*100 / (pg_catalog.sum(toast_blks_read) + " \
                   "pg_catalog.sum(toast_blks_hit)+1) from pg_statio_user_tables;"
            return float(_fetch_value_by_rpc(stmt, database=self.most_xact_db, default_val=100))
        return 100.0

    @property
    def shared_buffer_tidx_hit_rate(self):
        if self.is_rpc_valid:
            stmt = "select pg_catalog.sum(tidx_blks_hit)*100 / (pg_catalog.sum(tidx_blks_read) + " \
                   "pg_catalog.sum(tidx_blks_hit)+1) from pg_statio_user_tables;"
            return float(_fetch_value_by_rpc(stmt, database=self.most_xact_db, default_val=100))
        return 100.0

    @property
    def shared_buffer_idx_hit_rate(self):
        if self.is_rpc_valid:
            stmt = "select pg_catalog.sum(idx_blks_hit)*100/(pg_catalog.sum(idx_blks_read) + " \
                   "pg_catalog.sum(idx_blks_hit)+1) from pg_statio_user_tables ;"
            return float(_fetch_value_by_rpc(stmt, database=self.most_xact_db, default_val=100))
        return 100.0

    @property
    def temp_file_size(self):
        pg_db_temp_bytes = _fetch_one_value("pg_db_temp_bytes", from_server=self.database_address)
        pg_db_temp_files = _fetch_one_value("pg_db_temp_files", from_server=self.database_address)
        if pg_db_temp_files == 0:
            return 0.0
        return (pg_db_temp_bytes / pg_db_temp_files) / 1024  # unit is kB

    @property
    def read_write_ratio(self):
        tup_returned = _fetch_one_value("pg_db_tup_returned", from_server=self.database_address,
                                        datname=self.most_xact_db)
        tup_inserted = _fetch_one_value("pg_db_tup_inserted", from_server=self.database_address,
                                        datname=self.most_xact_db)
        tup_updated = _fetch_one_value("pg_db_tup_updated", from_server=self.database_address,
                                       datname=self.most_xact_db)
        tup_deleted = _fetch_one_value("pg_db_tup_deleted", from_server=self.database_address,
                                       datname=self.most_xact_db)
        res = tup_returned / (tup_inserted + tup_updated + tup_deleted + 0.001)

        return res

    @property
    def search_modify_ratio(self):
        tup_returned = _fetch_one_value("pg_db_tup_returned", from_server=self.database_address,
                                        datname=self.most_xact_db)
        tup_inserted = _fetch_one_value("pg_db_tup_inserted", from_server=self.database_address,
                                        datname=self.most_xact_db)
        tup_updated = _fetch_one_value("pg_db_tup_updated", from_server=self.database_address,
                                       datname=self.most_xact_db)
        tup_deleted = _fetch_one_value("pg_db_tup_deleted", from_server=self.database_address,
                                       datname=self.most_xact_db)
        res = (tup_returned + tup_inserted) / (tup_updated + tup_deleted + 0.01)

        return res

    @property
    def fetched_returned_ratio(self):
        tup_returned = _fetch_one_value("pg_db_tup_returned", from_server=self.database_address,
                                        datname=self.most_xact_db)
        tup_fetched = _fetch_one_value("pg_db_tup_fetched", from_server=self.database_address,
                                       datname=self.most_xact_db)
        res = tup_fetched / (tup_returned + 0.01)

        return res

    @property
    def rollback_commit_ratio(self):
        xact_commit = _fetch_one_value("pg_db_xact_commit", from_server=self.database_address,
                                       datname=self.most_xact_db)
        xact_rollback = _fetch_one_value("pg_db_xact_rollback", from_server=self.database_address,
                                         datname=self.most_xact_db)
        res = xact_rollback / (xact_commit + 0.01)

        return res

    @cached_property
    def os_cpu_count(self):
        cores = _fetch_one_value("os_cpu_processor_number", from_server=self.database_host, default_val=1)
        return int(cores)

    @property
    def current_free_mem(self):
        return _fetch_one_value_by_host("node_memory_MemFree_bytes", host=self.database_host)  # unit is Kb

    @cached_property
    def os_mem_total(self):
        return _fetch_one_value_by_host("node_memory_MemTotal_bytes", host=self.database_host)  # unit is Kb

    @cached_property
    def dirty_background_bytes(self):
        return _fetch_one_value_by_host("node_memory_Dirty_bytes", host=self.database_host)  # unit is Kb

    @cached_property
    def block_size(self):
        return self["block_size"]

    @property
    def load_average(self):
        load1 = _fetch_one_value_by_host("node_load1", host=self.database_host)
        load5 = _fetch_one_value_by_host("node_load5", host=self.database_host)
        load15 = _fetch_one_value_by_host("node_load15", host=self.database_host)
        if load1:
            load1 = load1 / self.os_cpu_count
        if load5:
            load5 = load5 / self.os_cpu_count
        if load15:
            load15 = load15 / self.os_cpu_count

        return load1, load5, load15

    @cached_property
    def nb_gaussdb(self):
        number = 0
        seqs = _fetch_all_value("gaussdb_qps_by_instance")
        for seq in seqs:
            if seq.labels and self.database_host in SequenceUtils.from_server(seq):
                number += 1
        return number

    @cached_property
    def is_hdd(self):
        return False
