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
from dbmind.common.parser import sql_parsing
import logging

from dbmind.app import monitoring
from dbmind.metadatabase.dao import statistical_metric
from ..slow_sql.query_info_source import QueryContext


def _search_table_structures(table_structures, table_name):
    for table_structure in table_structures:
        if table_structure.table_name == table_name:
            return table_structure


def _search_in_existing_indexes(index_info, seqscan_info):
    result = []
    for index_name, index_columns in index_info.items():
        if set(index_columns) & set(seqscan_info.columns):
            result.append({'index_name': index_name, 'index_column': index_columns})
    return result


def _get_historical_statistics(metric_name, host):
    result = statistical_metric.select_metric_statistic_avg_records(host=host, metric_name=metric_name, only_avg=True)
    for item in result:
        avg = list(item)[0] if list(item) else 0
        return avg


def _get_operator_cost(node):
    max_child_total_cost = max([item.total_cost for item in node.children])
    max_child_start_cost = max([item.start_cost for item in node.children])
    if node.start_cost >= max_child_total_cost:
        return node.total_cost - max_child_total_cost
    return node.total_cost - max_child_start_cost


def _hashjoin_adaptor(node):
    # HASHJOIN is only suitable for attr=xxx
    child1, child2 = node.children
    for key, value in child1.properties.items():
        if sql_parsing._regular_match(value, r'[\w.]+ = [\w.]+'):
            return True
    for key, value in child2.properties.items():
        if sql_parsing._regular_match(value, r'[\w.]+ = [\w.]+'):
            return True
    return False


def recommend_max_process_memory(total_mem, nb_gaussdb):
    omega = 0.8
    omega_min = 0.65
    suitable_mem = round(total_mem * omega / nb_gaussdb)
    min_mem = round(total_mem * omega_min / nb_gaussdb)
    return suitable_mem, min_mem


class QueryFeature:
    """
    Feature processing factory
    """

    def __init__(self, query_context: QueryContext = None):
        """
        :param query_context context including the necessary information of metrics when SQL occurs

        self.table_structure: data structure to save table structure
        self.lock_info: data structure to save lock information of slow query
        self.database_info: data structure to save lock information of database info such as QPS, CONNECTION, etc
        self.system_info: data structure to save system information
        self.detail: data structure to save diagnosis information
        """
        self.slow_sql_instance = query_context.slow_sql_instance
        self.query_context = query_context
        self.table_structure = None
        self.lock_info = None
        self.database_info = None
        self.system_info = None
        self.network_info = None
        self.pg_setting_info = None
        self.plan_parse_info = None
        self.recommend_index_info = None
        self.rewritten_sql_info = None
        self.timed_task_info = None
        self.wait_events = None
        self.sort_condition_info = None
        self.detail = {}
        self.suggestion = {}

    def initialize_metrics(self):
        """Initialize the data structure such as database_info, table_structure, lock_info, etc"""
        self.database_info = self.query_context.acquire_database_info()
        self.table_structure = self.query_context.acquire_tables_structure_info()
        self.lock_info = self.query_context.acquire_lock_info()
        self.system_info = self.query_context.acquire_system_info()
        self.network_info = self.query_context.acquire_network_info()
        self.rewritten_sql_info = self.query_context.acquire_rewritten_sql()
        self.recommend_index_info = self.query_context.acquire_recommend_index()
        self.plan_parse_info = self.query_context.acquire_plan_parse()
        self.timed_task_info = self.query_context.acquire_timed_task()
        self.wait_events = self.query_context.acquire_wait_event()
        self.pg_setting_info = self.query_context.acquire_pg_settings()
        self.sort_condition_info = self.query_context.acquire_sort_condition()

    @property
    def select_type(self) -> bool:
        """Determine whether it is a select statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('select'):
            return True
        return False

    @property
    def update_type(self) -> bool:
        """Determine whether it is a update statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('update'):
            return True
        return False

    @property
    def delete_type(self) -> bool:
        """Determine whether it is a delete statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('delete'):
            return True
        return False

    @property
    def insert_type(self) -> bool:
        """Determine whether it is a insert statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('insert'):
            return True
        return False

    @property
    def lock_contention(self) -> bool:
        """Determine whether the query is blocked during execution"""
        if not any((self.slow_sql_instance.lock_wait_count, self.slow_sql_instance.lwlock_wait_count)):
            return False
        if self.lock_info.locker_query:
            self.detail['lock_contention'] = "SQL was blocked by: '%s'." % self.lock_info.locker_query
            return True
        return False

    @property
    def large_table(self) -> bool:
        """Determine whether the query related table is large"""
        if not self.table_structure:
            return False
        tuples_info = {f"{item.schema_name}:{item.table_name}": {'live_tuples': item.live_tuples,
                                                                 'dead_tuples': item.dead_tuples,
                                                                 'table_size': item.table_size}
                       for item in self.table_structure}
        self.detail['large_table'] = {}
        for table_name, table_info in tuples_info.items():
            if table_info['live_tuples'] + table_info['dead_tuples'] > monitoring.get_threshold('tuple_number_threshold') or \
                    table_info['table_size'] > monitoring.get_threshold('table_total_size_threshold'):
                table_info['table_size'] = "%sMB" % table_info['table_size']
                self.detail['large_table'][table_name] = table_info
        if self.detail.get('large_table'):
            return True
        return False

    @property
    def many_dead_tuples(self) -> bool:
        """Determine whether the query related table has too many dead tuples"""
        if not self.table_structure or not self.large_table or self.insert_type:
            return False
        dead_rate_info = {f"{item.schema_name}:{item.table_name}": item.dead_rate for item in
                          self.table_structure}
        self.detail['dead_rate'] = {}
        if self.plan_parse_info is None:
            for table_name, dead_rate in dead_rate_info.items():
                if dead_rate > monitoring.get_threshold('dead_rate_threshold'):
                    self.detail['dead_rate'][table_name] = dead_rate
        else:
            # matching: 'Seq Scan', 'Index Scan', 'Index Only Scan'
            scan_operators = self.plan_parse_info.find_operators('Scan', accurate=False)
            scan_tables = []
            for operator in scan_operators:
                if operator.table:
                    scan_tables = operator.table
            for table in scan_tables:
                for table_info in self.table_structure:
                    if table_info.table_name == table and table_info.dead_rate > monitoring.get_threshold(
                            'dead_rate_threshold'):
                        self.detail['dead_rate'][
                            f"{table_info.schema_name}:{table_info.table_name}"] = table_info.dead_rate
        if self.detail.get('dead_rate'):
            return True
        return False

    @property
    def fetch_large_data(self) -> bool:
        """Determine whether the query related table has too many fetch tuples"""
        fetched_tuples = self.slow_sql_instance.n_tuples_fetched
        returned_tuples = self.slow_sql_instance.n_tuples_returned
        returned_rows = self.slow_sql_instance.n_returned_rows
        if fetched_tuples + returned_tuples > monitoring.get_threshold('fetch_tuples_threshold') or \
                returned_rows > monitoring.get_threshold('returned_rows_threshold'):
            self.detail['returned_rows'] = returned_rows
            self.detail['fetched_tuples'] = fetched_tuples + returned_tuples
            self.detail['fetched_tuples_rate'] = 'UNKNOWN'
            return True
        return False

    @property
    def unreasonable_database_knob(self):
        """support shared_buffers, work_mem"""
        total_memory = self.system_info.total_memory
        shared_buffers = self.pg_setting_info['shared_buffers'].setting
        work_mem = self.pg_setting_info['work_mem'].setting
        self.detail['unreasonable_database_knob'], self.suggestion['unreasonable_database_knob'] = '', ''
        return False

    @property
    def redundant_index(self) -> bool:
        """Determine whether the query related table has too redundant index"""
        if not self.table_structure or not self.large_table or self.select_type:
            return False
        redundant_index_info = {f"{item.schema_name}:{item.table_name}": item.redundant_index for item in
                                self.table_structure}
        self.detail['redundant_index'] = {}
        if self.plan_parse_info is not None:
            indexscan_operators = self.plan_parse_info.find_operators('Index Scan')
            indexonlyscan_operators = self.plan_parse_info.find_operators('Index Only Scan')
            indexscan_indexlist = [item.index for item in indexscan_operators + indexonlyscan_operators]
        else:
            indexscan_indexlist = []
        for table_name, redundant_index_list in redundant_index_info.items():
            not_use_redundant_index_list = []
            for redundant_index in redundant_index_list:
                if redundant_index not in indexscan_indexlist:
                    not_use_redundant_index_list.append(redundant_index)
            if not_use_redundant_index_list:
                self.detail['redundant_index'][table_name] = not_use_redundant_index_list
        if self.detail.get('redundant_index'):
            self.suggestion['redundant_index'] = "Deleting redundant indexes can improve " \
                                                 "the efficiency of update statements"
            return True
        return False

    @property
    def update_large_data(self) -> bool:
        """Determine whether the query related table has large update tuples"""
        updated_tuples = self.slow_sql_instance.n_tuples_updated
        if updated_tuples > monitoring.get_threshold('updated_tuples_threshold'):
            self.detail['updated_tuples'] = updated_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['updated_tuples_rate'] = round(updated_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['updated_tuples_rate'] = 'UNKNOWN'
            return True
        return False

    @property
    def insert_large_data(self) -> bool:
        """Determine whether the query related table has large insert tuples"""
        inserted_tuples = self.slow_sql_instance.n_tuples_inserted
        if inserted_tuples > monitoring.get_threshold('inserted_tuples_threshold'):
            self.detail['inserted_tuples'] = inserted_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['inserted_tuples_rate'] = round(inserted_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['inserted_tuples_rate'] = 'UNKNOWN'
            return True
        return False

    @property
    def delete_large_data(self) -> bool:
        """Determine whether the query related table has too many delete tuples"""
        deleted_tuples = self.slow_sql_instance.n_tuples_deleted
        if deleted_tuples > monitoring.get_threshold('deleted_tuples_threshold'):
            self.detail['deleted_tuples'] = deleted_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['deleted_tuples_rate'] = round(deleted_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['deleted_tuples_rate'] = 'UNKNOWN'
            return True
        return False

    @property
    def too_many_index(self) -> bool:
        """Determine whether the query related table has too many indexes, general experience: five indexes is enough
        """
        if not self.table_structure or self.select_type:
            return False
        self.detail['index'] = {}
        for table in self.table_structure:
            if len(table.index) > monitoring.get_threshold('index_number_threshold'):
                self.detail['index'][table.table_name] = f"{table.table_name} has {len(table.index)} index.\n"
        if self.detail.get('index'):
            return True
        return False

    @property
    def external_sort(self) -> bool:
        """Determine whether the query related table has external sort"""
        if self.sort_condition_info.get('sort_spill'):
            self.detail['external_sort'] = "Disk-Spill may occur during SQL sorting"
        if self.sort_condition_info.get('hash_spill'):
            self.detail['external_sort'] = "Disk-Spill may occur during HASH"
        if self.detail.get('external_sort'):
            self.suggestion['external_sort'] = "Adjust the size of WORK_MEM according to the business"
            return True
        return False

    @property
    def vacuum_event(self) -> bool:
        """Determine whether the query related table has vacuum operation
        todo: determine whether the current database is performing an autovacuum operation"""
        if not self.table_structure or not self.large_table:
            return False
        probable_time_interval = monitoring.get_threshold('analyze_operation_probable_time_interval')
        auto_vacuum_info = {f"{item.schema_name}:{item.table_name}": item.last_autovacuum for item in
                            self.table_structure}
        user_vacuum_info = {f"{item.schema_name}:{item.table_name}": item.vacuum for item in
                            self.table_structure}
        self.detail['autovacuum'] = {}
        self.detail['vacuum'] = {}
        for table_name, autovacuum_time in auto_vacuum_info.items():
            if self.slow_sql_instance.start_at <= autovacuum_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    autovacuum_time < self.slow_sql_instance.start_at < autovacuum_time + probable_time_interval:
                self.detail['autovacuum'][table_name] = autovacuum_time

        for table_name, vacuum_time in user_vacuum_info.items():
            if self.slow_sql_instance.start_at <= vacuum_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    vacuum_time < self.slow_sql_instance.start_at < vacuum_time + probable_time_interval:
                self.detail['vacuum'][table_name] = vacuum_time
        if self.detail.get('autovacuum') or self.detail.get('vacuum'):
            return True
        return False

    @property
    def analyze_event(self) -> bool:
        """Determine whether the query related table has analyze operation"""
        if not self.table_structure or not self.large_table:
            return False
        probable_time_interval = monitoring.get_threshold('analyze_operation_probable_time_interval')
        auto_analyze_info = {f"{item.schema_name}:{item.table_name}": item.last_autoanalyze for item in
                             self.table_structure}
        user_analyze_info = {f"{item.schema_name}:{item.table_name}": item.analyze for item in
                             self.table_structure}
        self.detail['autoanalyze'] = {}
        self.detail['analyze'] = {}
        for table_name, autoanalyze_time in auto_analyze_info.items():
            if self.slow_sql_instance.start_at <= autoanalyze_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    autoanalyze_time < self.slow_sql_instance.start_at < autoanalyze_time + probable_time_interval:
                self.detail['autoanalyze'][table_name] = autoanalyze_time
        for table_name, analyze_time in user_analyze_info.items():
            if self.slow_sql_instance.start_at <= analyze_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    analyze_time < self.slow_sql_instance.start_at < analyze_time + probable_time_interval:
                self.detail['analyze'][table_name] = analyze_time
        if self.detail.get('autoanalyze') or self.detail.get('analyze'):
            return True
        return False

    @property
    def workload_contention(self) -> bool:
        """Determine whether it is caused by the load of the database itself
        todo: need add metric in cmd_exporter"""
        cur_database_tps = self.database_info.current_tps
        if not cur_database_tps:
            return False
        indexes = ['a', 'b', 'c', 'd', 'e']
        tps_threshold = max(monitoring.get_param('tps_threshold'),
                            self.pg_setting_info['max_connections'].setting * 10)
        self.detail['workload_contention'], self.suggestion['workload_contention'] = '', ''
        if cur_database_tps > tps_threshold:
            index = indexes.pop(0)
            historical_statistics = _get_historical_statistics('tps', self.slow_sql_instance.db_host)
            if historical_statistics >= tps_threshold:
                self.detail['workload_contention'] += '%s. The database TPmc has continued to be significant ' \
                                                      'in the recent period, current TPmc: %s, historical TPmc: %s\n' \
                                                     % (index, cur_database_tps, historical_statistics)
                self.suggestion['workload_contention'] += '%s. Consider whether the business ' \
                                                          'is growing too fast\n' % index
            else:
                self.detail['workload_contention'] += '%s. The current TPS  of the database is large: %s\n' \
                                                     % (index, cur_database_tps)
                self.suggestion['workload_contention'] += '%s. It may be caused by instantaneous service jitter\n'\
                                                          % index
        if self.system_info.db_cpu_usage and \
                max(self.system_info.db_cpu_usage) > monitoring.get_param('cpu_usage_threshold'):
            index = indexes.pop(0)
            historical_statistics = _get_historical_statistics('db_cpu_usage', self.slow_sql_instance.db_host)
            if historical_statistics and historical_statistics >= monitoring.get_param('cpu_usage_threshold'):
                self.detail['workload_contention'] += "%s. The database CPU load has continued to be " \
                                                      "significant in the recent period, current cpu load: %s, " \
                                                      "historical cpu load: %s;" \
                                                      % (index, self.system_info.db_cpu_usage, historical_statistics)
                self.suggestion['workload_contention'] = '%s. If the CPU load is significant for a long time, ' \
                                                         'consider expanding the capacity;' % index
            else:
                self.detail['workload_contention'] += "%s. The current database CPU load is significant: %s" \
                                                      % (index, self.system_info.db_cpu_usage)
                self.suggestion['workload_contention'] += '%s. We need to locate the further cause of high CPU load;' \
                                                          % index
        if self.system_info.db_mem_usage and \
                max(self.system_info.db_mem_usage) > monitoring.get_param('mem_usage_threshold'):
            index = indexes.pop(0)
            historical_statistics = _get_historical_statistics('db_mem_usage', self.slow_sql_instance.db_host)
            if historical_statistics and historical_statistics >= monitoring.get_param('memory_usage_threshold'):
                self.detail['workload_contention'] += "%s. The database memory load has continued to be " \
                                                      "significant in the recent period, current cpu load: %s, " \
                                                      "historical cpu load: %s;" \
                                                      % (index, self.system_info.db_mem_usage, historical_statistics)
                self.suggestion['workload_contention'] += '%s. If the memory load is significant for a long time, ' \
                                                          'consider expanding the capacity;' % index
            else:
                self.detail['workload_contention'] += "%s. The current database memory load is significant: %s;" \
                                                      % (index, self.system_info.db_mem_usage)
                self.suggestion['workload_contention'] += '%s. We need to locate the further cause of high memory' \
                                                          ' load;' % index
        if self.system_info.db_data_occupy_rate > monitoring.get_param('disk_usage_threshold'):
            index = indexes.pop(0)
            self.detail['workload_contention'] += "%s. Insufficient free space in the database directory\n" % index
            self.suggestion['workload_contention'] += "%s. Please adjust or expand disk capacity in time\n" % index
        if self.database_info.max_conn and \
                self.database_info.used_conn / self.database_info.max_conn > \
                monitoring.get_param('connection_usage_threshold'):
            index = indexes.pop()
            self.detail['workload_contention'] += "%s. The connections to the database is large: " \
                                                  "current connections: %s, max connections is: %s\n" % \
                                                  (index,
                                                   self.database_info.used_conn,
                                                   self.pg_setting_info['max_connections'].setting)
            self.suggestion['workload_contention'] += "%s. No Suggestion" % index
        if self.detail['workload_contention']:
            return True
        return False

    @property
    def cpu_resource_contention(self) -> bool:
        if self.plan_parse_info is None:
            return False
        """Determine whether other processes outside the database occupy too many CPU resources"""
        if self.system_info.cpu_usage and max(self.system_info.cpu_usage) > monitoring.get_param('cpu_usage_threshold'):
            historical_statistics = _get_historical_statistics('os_cpu_usage', self.slow_sql_instance.db_host)
            if historical_statistics > monitoring.get_param('cpu_usage_threshold'):
                self.detail['system_cpu_contention'] = "The system cpu usage(exclude database process) " \
                                                       "has continued to be significant in the recent " \
                                                       "period, current value: %s, historical value: %s" \
                                                       % (self.system_info.cpu_usage, historical_statistics)
            else:
                self.detail['system_cpu_contention'] = "The current system cpu usage((exclude database process))" \
                                                       " is significant: %s." \
                                                       % self.system_info.cpu_usage
            self.suggestion['system_cpu_contention'] = "No Suggestion" 
            return True
        return False

    @property
    def io_resource_contention(self) -> bool:
        """Determine whether other processes outside the database occupy too many IO resources"""
        io_utils_dict = {}
        indexes = ['a', 'b', 'c']
        self.detail['system_io_contention'], self.suggestion['system_io_contention'] = '', ''
        if self.system_info.iops and max(self.system_info.iops) > monitoring.get_param('iops_threshold'):
            iops_historical_statistics = _get_historical_statistics('db_iops', self.slow_sql_instance.db_host)
            index = indexes.pop(0)
            if iops_historical_statistics and iops_historical_statistics > monitoring.get_param('iops_threshold'):
                self.detail['system_io_contention'] += "%s. The system IOPS has continued to be " \
                                                        "significant in the recent period, current " \
                                                        "current IOPS: %s, historical IOPS: %s;" \
                                                        % (index,
                                                           self.system_info.iops,
                                                           iops_historical_statistics)
            else:
                self.detail['system_io_contention'] += "%s. The current system IOPS is significant: %s;" \
                                                        % (index, self.system_info.iops)
            self.suggestion['system_io_contention'] += "a. Detect whether there are processes outside the " \
                                                       "database occupying IO resources for a long time;"
        for device, io_utils in self.system_info.ioutils.items():
            if io_utils > monitoring.get_param('disk_ioutils_threshold'):
                io_utils_dict[device] = io_utils
        if io_utils_dict:
            index = indexes.pop(0)
            self.detail['system_io_contention'] += '%s. The IO-Utils exceeds the threshold %s;' \
                                                    % (index, monitoring.get_param('disk_ioutils_threshold'))

            self.suggestion['system_io_contention'] += "%s. No Suggestion" % index
        if self.detail['system_io_contention']:
            return True
        return False

    @property
    def memory_resource_contention(self) -> bool:
        """Determine whether other processes outside the database occupy too much memory resources"""
        if self.system_info.mem_usage and max(self.system_info.mem_usage) > monitoring.get_param('mem_usage_threshold'):
            historical_statistics = _get_historical_statistics('os_mem_usage', self.slow_sql_instance.db_host)
            if historical_statistics > monitoring.get_param('mem_usage_threshold'):
                self.detail['system_mem_contention'] = "The system mem usage(exclude database process) " \
                                                       "has continued to be significant in the recent " \
                                                       "period, current value: %s, historical value: %s;" \
                                                       % (self.system_info.mem_usage, historical_statistics)
            else:
                self.detail['system_mem_contention'] = "The current system mem usage(exclude database process)" \
                                                       " is significant: %s;" \
                                                       % self.system_info.mem_usage
            self.suggestion['memory_resource_contention'] += "No Suggestion" 
            return True
        return False

    @property
    def large_network_drop_rate(self):
        # Detect network packet loss rate
        node_network_transmit_drop = self.network_info.transmit_drop
        node_network_receive_drop = self.network_info.receive_drop
        node_network_transmit_packets = self.network_info.transmit_packets
        node_network_receive_packets = self.network_info.receive_packets
        if node_network_receive_drop / node_network_receive_packets > \
                monitoring.get_param('package_drop_rate_threshold') or \
                node_network_transmit_drop / node_network_transmit_packets > \
                monitoring.get_param('package_drop_rate_threshold'):
            self.detail['network_drop'] = "The current server network is abnormal: ."
            self.suggestion['network_drop'] = "Diagnose the current network situation in time."
            return True
        return False

    @property
    def os_resource_contention(self) -> bool:
        """Determine whether other processes outside the database occupy too many handle resources
        todo: judge the limit of fds"""
        if self.system_info.process_fds_rate and max(self.system_info.process_fds_rate) > \
                monitoring.get_param('handler_occupation_threshold'):
            historical_statistics = _get_historical_statistics('os_fds_rate', self.slow_sql_instance.db_host)
            if historical_statistics > monitoring.get_param('handler_occupation_threshold'):
                self.detail['os_resource_contention'] = "The system fds occupation rate has continued to be " \
                                                       "significant in the recent period, current " \
                                                       "current value: %s, historical value: %s;" \
                                                       % (self.system_info.process_fds_rate, historical_statistics)
            else:
                self.detail['os_resource_contention'] = "The system fds occupation rate is significant: %s;" \
                                                       % self.system_info.cpu_usage
            self.suggestion['os_resource_contention'] += "No Suggestion"
            return True
        return False

    @property
    def database_wait_event(self) -> bool:
        """todo: blocked by wait event"""
        wait_events_list = []
        self.detail['wait_event'] = []
        for wait_event in self.wait_events:
            type = wait_event.type
            event = wait_event.event
            wait_events_list.append("%s: %s" % (type, event))
        if wait_events_list:
            self.detail['wait_event'] = ', '.join(wait_events_list)
            self.suggestion['wait_event'] = "No Suggestion"
            return True
        return False

    @property
    def lack_of_statistics(self) -> bool:
        if not self.table_structure:
            return False
        auto_analyze_info = {f"{item.schema_name}:{item.table_name}": item.last_autoanalyze for item in
                             self.table_structure}
        manual_analyze_info = {f"{item.schema_name}:{item.table_name}": item.analyze for item in
                               self.table_structure}
        tables = []
        for table_name, auto_analyze_time in auto_analyze_info.items():
            if auto_analyze_time == 0 and manual_analyze_info.get(table_name, 0) == 0:
                tables.append("%s(%s)" % (table_name, 'never'))
            else:
                not_auto_analyze_time = (self.slow_sql_instance.start_at - auto_analyze_time)
                not_manual_analyze_time = (self.slow_sql_instance.start_at - manual_analyze_info.get(table_name, 0))
                if min(not_auto_analyze_time, not_manual_analyze_time) > monitoring.get_threshold(
                        'update_statistics_threshold') * 1000:
                    not_update_statistic_time = min(not_manual_analyze_time, not_auto_analyze_time)
                    tables.append("%s(%ss)" % (table_name, not_update_statistic_time / 1000))
        if tables:
            self.detail['lack_of_statistics'] = "Table statistics are not updated in time: %s" % (','.join(tables))
            self.suggestion['lack_of_statistics'] = "Perform the analyze operation on the table in time"
            return True
        return False

    @property
    def missing_index(self):
        if self.recommend_index_info:
            self.detail['missing_index'] = 'Missing required index'
            self.suggestion['missing_index'] = 'Recommended index: %s' % str(self.recommend_index_info)
            return True
        return False

    @property
    def poor_join_performance(self):
        """
        Scenarios suitable for nestloop: 1) inner table has suitable index;
                                         2) the tuple of outer table is small(<10000).
        Scenarios suitable for hashjoin: Suitable for tables with large amounts of data(>10000).
        The poor_join_performance include the following three situations:
        1. Inappropriate join operator: enable_hashjoin=off, lead to use nestloop but not hashjoin.
        2. large joins: The amount of join data is very large(>1000000).
        3.
        """
        if self.plan_parse_info is None:
            return False
        indexes = ['a', 'b', 'c']
        self.detail['poor_join_performance'], self.suggestion['poor_join_performance'] = '', ''
        nestloop_info = self.plan_parse_info.find_operators('Nested Loop', accurate=False)
        hash_inner_join_info = self.plan_parse_info.find_operators('Hash Join', accurate=False)
        hash_left_join_info = self.plan_parse_info.find_operators('Hash Left Join', accurate=False)
        hash_right_join_info = self.plan_parse_info.find_operators('Hash Right Join', accurate=False)
        hash_full_join_info = self.plan_parse_info.find_operators('Hash Full Join', accurate=False)
        hash_anti_join_info = self.plan_parse_info.find_operators('Hash Anti Join', accurate=False)
        merge_join_info = self.plan_parse_info.find_operators('Merge Join', accurate=False)
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        hashjoin_info = hash_inner_join_info + hash_left_join_info + hash_right_join_info + \
                        hash_full_join_info + hash_anti_join_info
        abnormal_join, large_join, inappropriate_operator = False, False, False
        if plan_total_cost <= 0:
            return False
        abnormal_nestloop_info = [item for item in nestloop_info if _get_operator_cost(item) / plan_total_cost >
                                  monitoring.get_threshold('cost_rate_threshold')]
        abnormal_hashjoin_info = [item for item in hashjoin_info if _get_operator_cost(item) / plan_total_cost >
                                  monitoring.get_threshold('cost_rate_threshold')]
        abnormal_mergejoin_info = [item for item in merge_join_info if _get_operator_cost(item) / plan_total_cost >
                                   monitoring.get_threshold('cost_rate_threshold')]
        enable_hashjoin = self.pg_setting_info['enable_hashjoin'].setting
        for node in abnormal_nestloop_info + abnormal_hashjoin_info + abnormal_mergejoin_info:
            abnormal_join = True
            child1, child2 = node.children
            if 'Nested Loop' in node.name:
                # If the number of outer-table rows of the nest-loop is large,
                # the join node is considered inappropriate, in addition,
                # the inner table needs to establish an efficient data access method.
                if _hashjoin_adaptor(node) and \
                        min(child1.rows, child2.rows) > monitoring.get_threshold('nestloop_rows_threshold'):
                    inappropriate_operator = True
            if max(child1.rows, child2.rows) > monitoring.get_threshold('large_join_threshold'):
                # The amount of data in the join child node is too large
                large_join = True
        if large_join:
            index = indexes.pop(0)
            self.detail['poor_join_performance'] += '%s. Large Joins ' % index
            self.suggestion['poor_join_performance'] += 'a. Temporary tables can filter data, ' \
                                                        'reducing data orders of magnitude '
        if inappropriate_operator:
            index = indexes.pop(0)
            if enable_hashjoin == 0:
                self.detail['poor_join_performance'] += '%s. Inappropriate join operator ' % index
                self.suggestion['poor_join_performance'] += "%s. Detect 'enable_hashjoin=off', you can set the " \
                                                            "enable_hashjoin=on and let the optimizer " \
                                                            "choose by itself "
            else:
                self.detail['poor_join_performance'] += "%s. The NSETLOOP operator is not good " % index
                self.suggestion['poor_join_performance'] += "%s. No Suggestion " % index
        if not large_join and not inappropriate_operator and abnormal_join:
            index = indexes.pop(0)
            self.detail['poor_join_performance'] += '%s. The Join operator is expensive ' % index
            self.suggestion['poor_join_performance'] += "%s. No Suggestion " % index
        if self.detail['poor_join_performance']:
            return True
        return False

    @property
    def complex_boolean_expression(self):
        """
        1. select * from table where column in ('x', 'x', 'x', ..., 'x');
        To be added....
        """
        boolean_expressions = sql_parsing.exists_bool_clause(self.slow_sql_instance.query)
        for expression in boolean_expressions:
            expression_number = len(expression) if isinstance(expression, list) else 0
            if expression_number > monitoring.get_threshold('large_in_list_threshold'):
                self.detail['complex_boolean_expression'] = "%s. Large IN-Clause"
                self.suggestion['complex_boolean_expression'] = "%s. Rewrite large in-clause as a constant " \
                                                                "subquery or temporary table."
                return True
        return False

    @property
    def string_matching(self):
        """
        1. select id from table where func(id) > 5;
        2. select id from table where info like '%x';
        3. select id from table where info like '%x%';
        4. select id from table where info like 'x%';
        """
        indexes = ['a', 'b']
        self.detail['string_matching'], self.suggestion['string_matching'] = '', ''
        if sql_parsing.exists_regular_match(self.slow_sql_instance.query):
            index = indexes.pop(0)
            self.detail['string_matching'] = "%s. Existing grammatical structure: like '%%xxx';" % index
            self.suggestion['string_matching'] = "%s. Rewrite LIKE %%X into a range query;" % index
        if sql_parsing.exists_function(self.slow_sql_instance.query):
            index = indexes.pop(0)
            self.detail['string_matching'] += "%s. Suspected to use a function on an index;" % index
            self.suggestion['string_matching'] += "%s. Avoid using functions or expression " \
                                                  "operations on indexed columns;" % index
        if self.detail['string_matching']:
            return True
        return False

    @property
    def complex_execution_plan(self):
        """
        The SQL is complex：
          case1: Existing a large number of join operations.
          case2: Existing a large number of group operations.
        """
        if self.complex_boolean_expression:
            return False
        if self.plan_parse_info is None:
            return False
        indexes = ['a', 'b']
        self.detail['complex_execution_plan'], self.suggestion['complex_execution_plan'] = '', ''
        join_operator = self.plan_parse_info.find_operators('Join', accurate=False)
        agg_operator = self.plan_parse_info.find_operators('Aggregate', accurate=False)
        nestloop_operator = self.plan_parse_info.find_operators('Nested Loop', accurate=False)
        existing_subquery = sql_parsing.exists_subquery(self.slow_sql_instance.query)
        if 'SubPlan' in self.slow_sql_instance.query_plan and existing_subquery:
            index = indexes.pop(0)
            self.detail['complex_execution_plan'] += "%s. There may be sub-links that cannot be promoted." % index
            self.suggestion['complex_execution_plan'] += "%s. Try to rewrite the statement " \
                                                         "to support sublink-release; " % index
        if len(agg_operator) + len(join_operator) + len(nestloop_operator) > \
                monitoring.get_threshold('complex_operator_threshold') or self.plan_parse_info.height >= \
                monitoring.get_threshold('plan_height_threshold'):
            index = indexes.pop(0)
            self.detail['complex_execution_plan'] += "%s. The SQL statement is complex, and there are a " \
                                                     "large number of heavy operators" % index
        if self.plan_parse_info.height >= monitoring.get_threshold('plan_height_threshold'):
            index = indexes.pop(0)
            self.detail['complex_execution_plan'] += "%s. Execution plan is too complex" % index
        if self.detail.get('complex_execution_plan'):
            return True
        return False

    @property
    def correlated_subquery(self):
        """
        case1: select * from test where test.info1 in (select test2.info1 from test2 where test.info1=test2.info2);
            -> select * from test where exists
        case2: select * from t1 where c1 >(select t2.c1 from t2 where t2.c1=t1.c1);
            -> select * from t1 where c1 >(select max(t2.c1) from t2 where t2.c1=t1.c1);
        ...
        If the SQL not support Sublink-Release, the user needs to rewrite the SQL.
        """
        if self.plan_parse_info is None:
            return False
        indexes = ['a', 'b']
        self.detail['correlated_subquery'], self.suggestion['correlated_subquery'] = '', ''
        boolean_expression = sql_parsing.exists_bool_clause(self.slow_sql_instance.query)
        for expression in boolean_expression:
            if isinstance(expression, str) and \
                    expression.startswith('SELECT') and \
                    sql_parsing.exists_related_select(expression):
                index = indexes.pop(0)
                self.detail['correlated_subquery'] += "%s. Suspected correlated subquery in IN-Clause" % index
                self.suggestion['correlated_subquery'] += "%s. Rewrite subquery for IN-Clause, " \
                                                          "such as using 'exist'" % index
                break
        if ('SubPlan' in self.slow_sql_instance.query_plan and
            sql_parsing.exists_related_select(self.slow_sql_instance.query)) or \
                len(self.plan_parse_info.find_operators('Subquery Scan', accurate=True)) >= \
                monitoring.get_threshold('subquery_threshold'):
            index = indexes.pop(0)
            self.detail['correlated_subquery'] += "%s. There may be subquery that " \
                                                  "do not support sublink-release" % index
            self.suggestion['correlated_subquery'] += "%s. Rewrite SQL to support sublink-release" % index
        if self.detail.get('correlated_subquery'):
            return True
        return False

    @property
    def poor_aggregation_performance(self):
        if self.plan_parse_info is None:
            return False
        groupagg_info = self.plan_parse_info.find_operators('GroupAggregate')
        hashagg_info = self.plan_parse_info.find_operators('HashAggregate')
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        if plan_total_cost <= 0:
            return False
        abnormal_groupagg_info = [item for item in groupagg_info if _get_operator_cost(item) / plan_total_cost >
                                  monitoring.get_threshold('cost_rate_threshold')]
        abnormal_hashagg_info = [item for item in hashagg_info if _get_operator_cost(item) / plan_total_cost >
                                 monitoring.get_threshold('cost_rate_threshold')]
        enable_hashagg = self.pg_setting_info['enable_hashagg'].setting
        special_scene = False
        typical_scene = False
        abnormal_hashagg_scene = False
        abnormal_groupagg_scene = False
        indexes = ['a', 'b', 'c', 'd']
        self.detail['poor_aggregation_performance'], self.suggestion['poor_aggregation_performance'] = '', ''
        for node in abnormal_groupagg_info + abnormal_hashagg_info:
            if 'GroupAggregate' in node.name and enable_hashagg == 0:
                typical_scene = True
            elif 'GroupAggregate' in node.name and sql_parsing._regular_match(
                    self.slow_sql_instance.query, r"(?!)count\s*(\s*distinct \w+)?"):
                special_scene = True
            elif 'GroupAggregate' in node.name:
                abnormal_groupagg_scene = True
            elif 'HashAggregate' in node.name:
                abnormal_hashagg_scene = True
        if typical_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. Find that enable_hashagg=off and current " \
                                                           "uses the GroupAggregate operator" % index
            self.suggestion['poor_aggregation_performance'] += "%s. In general, HASHAGG performs " \
                                                               "better than GROUPAGG, but sometimes groupagg " \
                                                               "has better performance, you can set the " \
                                                               "enable_hashjoin=on and let the optimizer " \
                                                               "choose by itself" % index
        if special_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. HASHAGG does not support: 'count(distinct xx)'" \
                                                           % index
            self.suggestion['poor_aggregation_performance'] += "%s. Rewrite SQL to support HASHAGG" % index
        if abnormal_groupagg_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. The GROUPAGG operator cost is too high" % index
            self.suggestion['poor_aggregation_performance'] += "%s. Determine whether the statement " \
                                                               "can be optimized" % index
        if abnormal_hashagg_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. The HASHAGG operator cost is too high" % index
            self.suggestion['poor_aggregation_performance'] += "%s. If the number of group keys or NDV is large, " \
                                                               "the hash table may be larger and lead to spill " \
                                                               "to disk" % index
        if self.detail.get('poor_aggregation_performance'):
            return True
        return False

    @property
    def abnormal_sql_structure(self):
        if self.rewritten_sql_info:
            self.suggestion['rewritten_sql'] = self.rewritten_sql_info
            return True
        return False

    @property
    def timed_task_conflict(self):
        if not self.pg_setting_info:
            return False
        if self.pg_setting_info['job_queue_processes'] == 0:
            return False
        abnormal_timed_task = []
        for timed_task in self.timed_task_info:
            if max(timed_task.last_start_date, self.slow_sql_instance.start_at) < \
                    min(timed_task.last_end_date,
                        self.slow_sql_instance.start_at + self.slow_sql_instance.duration_time):
                abnormal_timed_task.append("job_id(%s), priv_user(%s), job_status(%s)" % (timed_task.job_id,
                                                                                          timed_task.priv_user,
                                                                                          timed_task.job_status))
        if abnormal_timed_task:
            self.detail['timed_task_conflict'] = ';'.join(abnormal_timed_task)
            return True
        return False

    @property
    def abnormal_process_occupation(self):
        # Implementation of follow-up supplementary functions
        return False

    def __call__(self):
        self.detail['system_cause'] = {}
        self.detail['plan'] = {}
        try:
            features = [self.lock_contention,
                        self.many_dead_tuples,
                        self.fetch_large_data,
                        self.unreasonable_database_knob,
                        self.redundant_index,
                        self.update_large_data,
                        self.insert_large_data,
                        self.delete_large_data,
                        self.too_many_index,
                        self.external_sort,
                        self.vacuum_event,
                        self.analyze_event,
                        self.workload_contention,
                        self.cpu_resource_contention,
                        self.io_resource_contention,
                        self.memory_resource_contention,
                        self.large_network_drop_rate,
                        self.os_resource_contention,
                        self.database_wait_event,
                        self.lack_of_statistics,
                        self.missing_index,
                        self.poor_join_performance,
                        self.complex_boolean_expression,
                        self.string_matching,
                        self.complex_execution_plan,
                        self.correlated_subquery,
                        self.poor_aggregation_performance,
                        self.abnormal_sql_structure,
                        self.timed_task_conflict,
                        self.abnormal_process_occupation]
            features = [int(item) for item in features]
        except Exception as e:
            logging.error(str(e), exc_info=True)
            features = [0] * 30
        return features, self.detail, self.suggestion
