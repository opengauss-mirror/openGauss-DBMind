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
import time

from dbmind.app import monitoring
from dbmind.common.parser import sql_parsing
from ..slow_sql.query_info_source import QueryContext

PROPERTY_LENGTH = 40
MINIMAL_HIT_RATE = 0.98


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


def _get_operator_cost(node):
    """Get the cost of a single operator which do not include the cost of children."""
    max_child_total_cost, max_child_start_cost = 0.0, 0.0
    if node.children:
        max_child_total_cost = max([item.total_cost for item in node.children])
        max_child_start_cost = max([item.start_cost for item in node.children])
    if node.start_cost >= max_child_total_cost:
        return node.total_cost - max_child_total_cost
    return node.total_cost - max_child_start_cost


def _get_operator_total_cost(node):
    """Get the cost of operator which include cost of children."""
    total_cost = _get_operator_cost(node)
    for child in node.children:
        total_cost += _get_operator_cost(child)
    return total_cost


def _hashjoin_adaptor(node):
    # HASHJOIN is only suitable for attr=xxx
    child1, child2 = node.children
    for key, value in child1.properties.items():
        if sql_parsing.regular_match(r'[\w.]+ = [\w.]+', value):
            return True
    for key, value in child2.properties.items():
        if sql_parsing.regular_match(r'[\w.]+ = [\w.]+', value):
            return True
    return False


def _get_node_rows(table_structures, node):
    if 'Seq Scan' in node.name:
        if node.properties.get('Filter'):
            for table_info in table_structures:
                if table_info.table_name == node.table:
                    return table_info.live_tuples + table_info.dead_tuples
    return node.rows


class QueryFeature:
    """
    Feature processing factory
    """

    def __init__(self, query_context: QueryContext):
        """
        :param query_context context including the necessary information of metrics when SQL occurs

        self.table_structure: data structure to save table structure
        self.database_info: data structure to save lock information of database info such as QPS, CONNECTION, etc
        self.system_info: data structure to save system information
        self.detail: data structure to save diagnosis information
        """
        self.slow_sql_instance = query_context.slow_sql_instance
        self.query_context = query_context
        self.table_structure = None
        self.database_info = None
        self.system_info = None
        self.network_info = None
        self.pg_setting_info = None
        self.plan_parse_info = None
        self.recommend_index_info = None
        self.rewritten_sql_info = None
        self.timed_task_info = None
        self.wait_event_info = None
        self.unused_index_info = None
        self.redundant_index_info = None
        self.total_memory_detail = None
        self.detail = {}
        self.suggestion = {}

    def initialize_metrics(self):
        """Initialize the data structure such as database_info, table_structure, etc"""
        self.database_info = self.query_context.acquire_database_info()
        self.table_structure = self.query_context.acquire_tables_structure_info()
        self.system_info = self.query_context.acquire_system_info()
        self.network_info = self.query_context.acquire_network_info()
        self.rewritten_sql_info = self.query_context.acquire_rewritten_sql()
        index_analysis_info = self.query_context.acquire_index_analysis_info()
        if index_analysis_info:
            self.recommend_index_info, self.redundant_index_info = index_analysis_info
        self.plan_parse_info = self.query_context.acquire_plan_parse()
        self.wait_event_info = self.query_context.acquire_wait_event_info()
        self.pg_setting_info = self.query_context.acquire_pg_settings()
        self.unused_index_info = self.query_context.acquire_unused_index()
        self.total_memory_detail = self.query_context.acquire_total_memory_detail()

    @property
    def select_type(self) -> bool:
        """Determine whether it is a select statement."""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('select'):
            return True
        return False

    @property
    def update_type(self) -> bool:
        """Determine whether it is a update statement."""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('update'):
            return True
        return False

    @property
    def delete_type(self) -> bool:
        """Determine whether it is a delete statement."""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('delete'):
            return True
        return False

    @property
    def insert_type(self) -> bool:
        """Determine whether it is a insert statement."""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('insert'):
            return True
        return False

    @property
    def lock_contention(self) -> bool:
        """Determine whether the query is blocked during execution."""
        if self.wait_event_info.block_sessionid is not None:
            self.detail['lock_contention'] = "SQL was blocked by session: %s, lockmode: %s, locktag: %s" % \
                                             (self.wait_event_info.block_sessionid,
                                              self.wait_event_info.lockmode,
                                              self.wait_event_info.locktag)
            return True
        return False

    @property
    def large_table(self) -> bool:
        """
        Determine whether the query related table is large, which includes the following two aspects:
          case1: Table size exceeds a certain threshold.
          case2: The number of table tuples exceeds a certain threshold.
        """
        if not self.table_structure:
            return False
        tuples_info = {f"{item.schema_name}:{item.table_name}": {'live_tuples': item.live_tuples,
                                                                 'dead_tuples': item.dead_tuples,
                                                                 'table_size': item.table_size}
                       for item in self.table_structure}
        self.detail['large_table'] = {}
        for table_name, table_info in tuples_info.items():
            if table_info['live_tuples'] + table_info['dead_tuples'] >= monitoring.get_slow_sql_param(
                    'tuple_number_threshold') or \
                    table_info['table_size'] >= monitoring.get_slow_sql_param('table_total_size_threshold'):
                table_info['table_size'] = "%sMB" % table_info['table_size']
                self.detail['large_table'][table_name] = table_info
        if self.detail.get('large_table'):
            return True
        return False

    @property
    def many_dead_tuples(self) -> bool:
        """
        Determine whether the query related table has too many dead tuples,
        bloat-table will affects query performance.
        """
        if not self.table_structure or self.insert_type:
            return False
        table_info = {f"{item.schema_name}:{item.table_name}": {'dead_rate': item.dead_rate,
                                                                'live_tuples': item.live_tuples,
                                                                'dead_tuples': item.dead_tuples,
                                                                'table_size': item.table_size}
                      for item in self.table_structure}
        self.detail['dead_rate'] = {}
        abnormal_dead_rate = ''
        if self.plan_parse_info is None:
            for table_name, detail in table_info.items():
                if detail['live_tuples'] + detail['dead_tuples'] <= monitoring.get_slow_sql_param(
                        'tuple_number_threshold') and \
                        detail['table_size'] <= monitoring.get_slow_sql_param('table_total_size_threshold'):
                    continue
                if detail['dead_rate'] >= monitoring.get_slow_sql_param('dead_rate_threshold'):
                    abnormal_dead_rate += '%s: live_tup(%s) dead_tup(%s) dead_rate(%s)  ' % (table_name,
                                                                                             detail['live_tuples'],
                                                                                             detail['dead_tuples'],
                                                                                             detail['dead_rate'])
        else:
            # Matching 'Seq Scan', 'Index Scan', 'Index Only Scan', etc
            scan_operators = self.plan_parse_info.find_operators('Scan', accurate=False)
            scan_tables = set()
            for operator in scan_operators:
                if operator.table:
                    scan_tables.add(operator.table)
            for table in scan_tables:
                for table_info in self.table_structure:
                    # The plan can not reflect schema information, so table may be under other schema.
                    if table_info.table_name == table and \
                            table_info.dead_rate >= monitoring.get_slow_sql_param('dead_rate_threshold') and \
                            self.large_table:
                        abnormal_dead_rate += '%s: live_tup(%s) dead_tup(%s) dead_rate(%s)  ' % (table_info.table_name,
                                                                                                 table_info.live_tuples,
                                                                                                 table_info.dead_tuples,
                                                                                                 table_info.dead_rate)
        if abnormal_dead_rate:
            self.detail['many_dead_tuples'] = "Dead tuples affect SQL query performance. Detail: %s" % \
                                              abnormal_dead_rate
            self.suggestion['many_dead_tuples'] = "Clean up dead tuples in time to avoid affecting query performance."
            return True
        return False

    @property
    def heavy_scan_operator(self) -> bool:
        """Determine whether the query related table has too many fetched tuples and the hit rate is normal."""
        if self.update_large_data or self.delete_large_data:
            return False
        hit_rate = self.slow_sql_instance.hit_rate
        fetched_tuples = self.slow_sql_instance.n_tuples_fetched
        returned_tuples = self.slow_sql_instance.n_tuples_returned
        returned_rows = self.slow_sql_instance.n_returned_rows
        if self.plan_parse_info is None:
            if fetched_tuples + returned_tuples >= monitoring.get_slow_sql_param('fetch_tuples_threshold') or \
                    returned_rows >= monitoring.get_slow_sql_param('returned_rows_threshold'):
                if hit_rate <= MINIMAL_HIT_RATE:
                    self.detail['heavy_scan_operator'] = "Existing large scan situation and hit rate is low. " \
                                                      "Detail: fetch_tuples(%s), returned_rows(%s), hit_rate: %s" % \
                                                      (fetched_tuples + returned_tuples, returned_rows, hit_rate)
                else:
                    self.detail['heavy_scan_operator'] = "Existing large scan situation. " \
                                                      "Detail: fetch_tuples(%s), returned_rows(%s), hit_rate: %s" % \
                                                      (fetched_tuples + returned_tuples, returned_rows, hit_rate)
                self.suggestion['heavy_scan_operator'] = "According to business adjustments, try to avoid large scans"
                return True
            return False
        suggestions = []
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        seq_scan_operators = [operator for operator in
                              self.plan_parse_info.find_operators('Seq Scan', accurate=False)
                              if _get_operator_cost(operator) / plan_total_cost >=
                              monitoring.get_slow_sql_param('cost_rate_threshold')]
        index_scan_operators = [operator for operator in
                                self.plan_parse_info.find_operators('Index Scan', accurate=False)
                                if _get_operator_cost(operator) / plan_total_cost >=
                                monitoring.get_slow_sql_param('cost_rate_threshold')]
        heap_scan_operators = [operator for operator in
                               self.plan_parse_info.find_operators('Heap Scan', accurate=False)
                               if _get_operator_cost(operator) / plan_total_cost >=
                               monitoring.get_slow_sql_param('cost_rate_threshold')]
        if seq_scan_operators and not (index_scan_operators or heap_scan_operators):
            self.detail['heavy_scan_operator'] = "Existing expensive seq scans"
        elif not seq_scan_operators and (index_scan_operators or heap_scan_operators):
            self.detail['heavy_scan_operator'] = "Existing expensive index scans"
        elif seq_scan_operators and (index_scan_operators or heap_scan_operators):
            self.detail['heavy_scan_operator'] = "Existing expensive index and seq scans"
        detail = ','.join("(name: %s, parent: %s, rows:%s(%s), cost rate: %s%%)" %
                          (operator.name,
                           operator.parent.name if operator.parent is not None else "None",
                           operator.table, _get_node_rows(self.table_structure, operator),
                           round(_get_operator_cost(operator) * 100 / plan_total_cost, 2)) for operator in
                          seq_scan_operators + index_scan_operators + heap_scan_operators)
        if all(item in detail for item in ('Hash', 'Join')):
            suggestions.append("HashJoin and SeqScan related, normally it is acceptable")
        if 'Nested Loop' in detail:
            suggestions.append("Confirm that the inner table has index")
        if seq_scan_operators and sql_parsing.exists_count_operation(self.slow_sql_instance.query):
            suggestions.append("Find 'count' operation, try to avoid this behavior")
        # if there are many random scan tuples, iit ndicates a problem with index filter ability
        if index_scan_operators and \
                self.slow_sql_instance.n_tuples_fetched > self.slow_sql_instance.n_tuples_returned:
            suggestions.append("consider that index filter ability is not strong")
        if self.detail.get('heavy_scan_operator'):
            self.detail['heavy_scan_operator'] += ". Detail: %s" % detail
            if not suggestions:
                self.suggestion['heavy_scan_operator'] = "According to business adjustments, try to avoid it"
            else:
                self.suggestion['heavy_scan_operator'] = '; '.join(suggestions)
            return True
        return False

    @property
    def abnormal_plan_time(self):
        """Determinate whether the parse status is normal."""
        n_soft_parse = self.slow_sql_instance.n_soft_parse
        n_hard_parse = self.slow_sql_instance.n_hard_parse
        plan_time = self.slow_sql_instance.plan_time
        exc_time = self.slow_sql_instance.duration_time
        if plan_time / exc_time >= monitoring.get_slow_sql_param('plan_time_rate_threshold') \
                and n_hard_parse > n_soft_parse:
            self.detail['abnormal_plan_time'] = "There exists some hard parses in the execution plan generation process"
            self.suggestion['abnormal_plan_time'] = "Modify business to support PBE"
            return True
        return False

    @property
    def unused_and_redundant_index(self) -> bool:
        """
        Determine whether the query related table has too redundant index,
        we consider indexes that have not been used for a long time as redundant indexes.
        """
        if not self.large_table:
            return False
        if not self.unused_index_info and not self.redundant_index_info:
            return False
        if self.select_type:
            return False
        self.detail['unused_and_redundant_index'] = "Found unused or redundant indexes. " \
                                                    "Unused indexes: %s, redundant indexes: %s" % \
                                                    (self.unused_index_info, self.redundant_index_info)
        self.suggestion['unused_and_redundant_index'] = "Clean up redundant and unused indexes"
        return True

    @property
    def update_large_data(self) -> bool:
        """
        Determine whether the query related table has large update tuples.
        Note: it will be deleted in the future
        """
        updated_tuples = self.slow_sql_instance.n_tuples_updated
        if updated_tuples >= monitoring.get_slow_sql_param('updated_tuples_threshold'):
            self.detail['updated_tuples'] = updated_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['update_large_data'] = "Update a large number of tuples(%s rows)" % \
                                                   round(updated_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['update_large_data'] = 'UNKNOWN'
            return True
        if self.plan_parse_info is None:
            return False
        update_operator = self.plan_parse_info.find_operators("Update on", accurate=False)
        for operator in update_operator:
            table = operator.table
            rows = operator.rows
            if rows > monitoring.get_slow_sql_param('updated_tuples_threshold'):
                self.detail["update_large_data"] = "Update a large number of tuples: %s(%s rows)" % (table, rows)
                self.suggestion["update_large_data"] = "Make adjustments to the business"
        if self.detail.get("update_large_data"):
            return True
        return False

    @property
    def insert_large_data(self) -> bool:
        """Determine whether the query related table has large insert tuples."""
        inserted_tuples = self.slow_sql_instance.n_tuples_inserted
        if inserted_tuples >= monitoring.get_slow_sql_param('inserted_tuples_threshold'):
            self.detail['inserted_tuples'] = inserted_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail["insert_large_data"] = "Insert a large number of tuples(%s rows)" % \
                                                   round(inserted_tuples / self.table_structure[0].live_tuples, 4)
                self.suggestion["insert_large_data"] = "Make adjustments to the business"
            self.detail['update_large_data'] = 'UNKNOWN'
            return True
        else:
            if self.plan_parse_info is None:
                return False
            insert_operator = self.plan_parse_info.find_operators("Insert on", accurate=False)
            for operator in insert_operator:
                table = operator.table
                rows = operator.rows
                if rows > monitoring.get_slow_sql_param('inserted_tuples_threshold'):
                    self.detail["insert_large_data"] = "Insert a large number of tuples: %s(%s rows)" % (table, rows)
                    self.suggestion["insert_large_data"] = "Make adjustments to the business"
            if self.detail.get("insert_large_data"):
                return True
        return False

    @property
    def delete_large_data(self) -> bool:
        """
        Determine whether the query related table has too many delete tuples.
        Note: it will be deleted in the future
        """
        deleted_tuples = self.slow_sql_instance.n_tuples_deleted
        if deleted_tuples >= monitoring.get_slow_sql_param('deleted_tuples_threshold'):
            self.detail['deleted_tuples'] = deleted_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['deleted_tuples_rate'] = "Delete a large number of tuples(%s rows)" % \
                                                     round(deleted_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['deleted_tuples_rate'] = 'UNKNOWN'
            return True
        else:
            if self.plan_parse_info is None:
                return False
            delete_operator = self.plan_parse_info.find_operators("Delete on", accurate=False)
            for operator in delete_operator:
                table = operator.table
                rows = operator.rows
                if rows > monitoring.get_slow_sql_param('deleted_tuples_threshold'):
                    self.detail["delete_large_data"] = "Delete a large number of tuples: %s(%s rows)" % (table, rows)
                    self.suggestion["delete_large_data"] = "Make adjustments to the business"
            if self.detail.get("delete_large_data"):
                return True
        return False

    @property
    def too_many_index(self) -> bool:
        """Too many indexes affect the performance of insert and update to a certain extent."""
        if not self.table_structure or self.select_type:
            return False
        self.detail['too_many_index'] = {}
        related_tables = ''
        for table in self.table_structure:
            if len(table.index) >= monitoring.get_slow_sql_param('index_number_threshold'):
                related_tables += "%s(%s) " % (table.table_name, len(table.index))
        if related_tables:
            self.detail['too_many_index'] = "Found a large number of indexes in the table. Detail: %s" % related_tables
            self.suggestion["too_many_index"] = "Too many index will affect the speed of " \
                                                "insert, delete, and update statement"
            return True
        return False

    @property
    def disk_spill(self) -> bool:
        """Determine whether existing disk spill during the execution of SQL."""
        if self.plan_parse_info is None:
            if self.slow_sql_instance.sort_spill_count >= monitoring.get_slow_sql_param('sort_rate_threshold') or \
                    self.slow_sql_instance.hash_spill_count >= monitoring.get_slow_sql_param('sort_rate_threshold'):
                self.detail['disk_spill'] = "Disk-Spill may occur during SORT or Hash operation"
        else:
            plan_total_cost = self.plan_parse_info.root_node.total_cost
            if plan_total_cost <= 0:
                return False
            sort_operators = self.plan_parse_info.find_operators('Sort', accurate=True)
            hash_operators = [item for item in self.plan_parse_info.find_operators('Hash', accurate=True)]
            abnormal_operator_detail = ','.join("(parent: %s, rows: %s, cost rate: %s%%)" %
                                                (operator.parent.name if operator.parent is not None else "None",
                                                 operator.rows,
                                                 round(_get_operator_cost(operator) * 100 / plan_total_cost, 2))
                                                for operator in sort_operators + hash_operators
                                                if round(_get_operator_cost(operator) / plan_total_cost, 2) >=
                                                monitoring.get_slow_sql_param('cost_rate_threshold'))
            if abnormal_operator_detail and \
                    (self.slow_sql_instance.sort_spill_count > 0 or self.slow_sql_instance.hash_spill_count > 0):
                self.detail['disk_spill'] = "The SORT/HASH operation may spill to disk. Detail: %s" % \
                                            abnormal_operator_detail
        if self.detail.get('disk_spill'):
            self.suggestion['disk_spill'] = "Analyze whether the business needs to " \
                                            "adjust the size of the work_mem parameter"
            return True
        return False

    @property
    def vacuum_event(self) -> bool:
        """Determine whether the query related table has vacuum operation."""
        if not self.table_structure:
            return False
        probable_time_interval = monitoring.get_slow_sql_param('analyze_operation_probable_time_interval')
        vacuum_info = {f"{item.schema_name}:{item.table_name}": int(time.time() * 1000) - item.vacuum_delay * 1000
                       for item in self.table_structure}
        self.detail['vacuum'] = {}
        for table_name, vacuum_time in vacuum_info.items():
            if self.slow_sql_instance.start_at <= vacuum_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    vacuum_time <= self.slow_sql_instance.start_at <= vacuum_time + probable_time_interval * 1000:
                self.detail['vacuum'][table_name] = vacuum_time
        if self.detail.get('vacuum'):
            return True
        return False

    @property
    def analyze_event(self) -> bool:
        """Determine whether the query related table has an analyzing operation."""
        if not self.table_structure:
            return False
        probable_time_interval = monitoring.get_slow_sql_param('analyze_operation_probable_time_interval')
        analyze_info = {f"{item.schema_name}:{item.table_name}": int(time.time() * 1000) - item.analyze_delay * 1000
                        for item in self.table_structure}
        self.detail['analyze'] = {}
        for table_name, analyze_time in analyze_info.items():
            if self.slow_sql_instance.start_at <= analyze_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    analyze_time <= self.slow_sql_instance.start_at <= analyze_time + probable_time_interval * 1000:
                self.detail['analyze'][table_name] = analyze_time
        if self.detail.get('analyze'):
            return True
        return False

    @property
    def workload_contention(self) -> bool:
        """
        Determine whether it is caused by the load of the database itself, which includes:
          case1: The CPU resource occupied by the database is abnormal.
          case2: The MEMORY resource occupied by the database is abnormal.
          case3: Insufficient space in database data directory.
          case4: The connections of database accounts for too much of the total connections.
        """
        indexes = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']
        wk_expansion_factor = 10
        self.detail['workload_contention'], self.suggestion['workload_contention'] = '', ''

        # determine whether the process_used_memory occupancy rate is too high
        if self.total_memory_detail.process_used_memory / self.total_memory_detail.max_process_memory >= \
                monitoring.get_detection_threshold('db_memory_rate_threshold'):
            self.detail['workload_contention'] += "%s. The rate of max_process_memory is too high: %s" % \
                                                  (indexes.pop(0),
                                                   round(self.total_memory_detail.process_used_memory /
                                                         self.total_memory_detail.max_process_memory, 2))

        # determine whether the dynamic_used_memory occupancy rate is too high
        if self.total_memory_detail.dynamic_used_memory / self.total_memory_detail.max_dynamic_memory >= \
                monitoring.get_detection_threshold('db_memory_rate_threshold'):
            self.detail['workload_contention'] += "%s. The rate of max_dynamic_memory is too high: %s" % \
                                                  (indexes.pop(0),
                                                   round(self.total_memory_detail.dynamic_used_memory /
                                                         self.total_memory_detail.max_dynamic_memory, 2))

        # determine whether the other_used_memory is too large
        if self.total_memory_detail.other_used_memory >= \
                monitoring.get_detection_threshold('other_used_memory_threshold'):
            self.detail['workload_contention'] += "%s. The other_used_memory is high: %s" % \
                                                  (indexes.pop(0),
                                                   self.total_memory_detail.other_used_memory)

        # determine whether the TPS is too large
        if self.database_info.tps and 'max_connections' in self.pg_setting_info and \
                self.database_info.tps >= self.pg_setting_info['max_connections'].setting * wk_expansion_factor:
            self.detail['workload_contention'] += "%s. Current business is heavy \n" % indexes.pop(0)

        # determine whether the db_cpu_usage is too high
        if self.system_info.db_cpu_usage and \
                max(self.system_info.db_cpu_usage) >= monitoring.get_detection_threshold('cpu_usage_threshold'):
            self.detail['workload_contention'] += "%s. The current database CPU usage is significant: %s\n" \
                                                  % (indexes.pop(0), max(self.system_info.db_cpu_usage))

        # determine whether the db_mem_usage is too high
        if self.system_info.db_mem_usage and \
                max(self.system_info.db_mem_usage) >= monitoring.get_detection_threshold('mem_usage_threshold'):
            self.detail['workload_contention'] += "%s. The current database memory usage is significant: %s\n" \
                                                  % (indexes.pop(0), max(self.system_info.db_mem_usage))

        # determine whether the disk usage which data directory located is too high
        if self.system_info.disk_usage and \
                max(self.system_info.disk_usage) >= monitoring.get_detection_threshold('disk_usage_threshold'):
            self.detail['workload_contention'] += "%s. Insufficient free space in the database directory\n" % \
                                                  indexes.pop(0)

        # determine whether the connection occupancy rate is too high
        if 'max_connections' in self.pg_setting_info and \
                self.database_info.connection / self.pg_setting_info['max_connections'].setting >= \
                monitoring.get_detection_threshold('connection_usage_threshold'):
            self.detail['workload_contention'] += "%s. The rate of connection usage is high: %s\n" % \
                                                  (indexes.pop(0),
                                                   self.database_info.connection /
                                                   self.pg_setting_info['max_connections'].setting)

        # determine whether the thread pool occupancy rate is too high
        if (
            'enable_thread_pool' in self.pg_setting_info and
            self.pg_setting_info['enable_thread_pool'].setting and
            self.database_info.thread_pool_rate >= monitoring.get_detection_threshold('thread_pool_usage_threshold')
        ):
            self.detail['workload_contention'] += "%s. The rate of thread pool usage is high: %s\n" % \
                                                  (indexes.pop(0), self.database_info.thread_pool_rate)
        indexes = ['a', 'b', 'c']
        if self.detail.get('workload_contention'):
            if 'other_used_memory' in self.detail['workload_contention']:
                self.suggestion['workload_contention'] += \
                    "%s. Detecting whether exists third-party memory usage heaps" % indexes.pop(0)
            if 'max_dynamic_memory' in self.detail.get('workload_contention') or \
                    'max_process_memory' in self.detail.get('workload_contention'):
                self.suggestion['workload_contention'] += "%s. Detecting whether exists memory context " \
                                                          "consuming too much dynamic memory" % indexes.pop(0)
            self.suggestion['workload_contention'] += "%s. Current workload is heavy. Determine whether it is caused " \
                                                      "by abnormal transaction or just busy business" % indexes.pop(0)
            return True
        return False

    @property
    def cpu_resource_contention(self) -> bool:
        """Determine whether other processes outside the database occupy too many CPU resources."""
        if self.system_info.user_cpu_usage and \
                max(self.system_info.user_cpu_usage) >= monitoring.get_detection_threshold('cpu_usage_threshold'):
            self.detail['system_cpu_contention'] = "The current user cpu usage is significant: %s." \
                                                   % max(self.system_info.user_cpu_usage)
        if self.detail.get('system_cpu_contention'):
            self.suggestion['system_cpu_contention'] = "Handle exception processes in system"
            return True
        return False

    @property
    def io_resource_contention(self) -> bool:
        """
        Determine whether the current IO resources are tight,
        currently we can not distinguish whether it is occupied by database or other processes.
        """
        io_utils_dict = {}
        self.detail['system_io_contention'], self.suggestion['system_io_contention'] = '', ''
        for device, io_utils in self.system_info.ioutils.items():
            if max(io_utils) >= monitoring.get_detection_threshold('disk_ioutils_threshold'):
                io_utils_dict[device] = max(io_utils)
        if io_utils_dict:
            self.detail['system_io_contention'] = 'The IO-Utils exceeds the threshold %s during execution. Detail: %s' \
                                                  % (monitoring.get_detection_threshold('disk_ioutils_threshold'),
                                                     io_utils_dict)
            self.suggestion['system_io_contention'] = "a. Detect whether processes outside the database " \
                                                      "compete for resources " \
                                                      "b. Detect current long transaction in database"
        if self.detail['system_io_contention']:
            return True
        return False

    @property
    def memory_resource_contention(self) -> bool:
        """Determine whether other processes outside the database occupy too much memory resources."""
        if self.system_info.system_mem_usage and \
                max(self.system_info.system_mem_usage) >= monitoring.get_detection_threshold('mem_usage_threshold'):
            self.detail['system_mem_contention'] = "The current system mem usage" \
                                                   " is significant: %s;" \
                                                   % max(self.system_info.system_mem_usage)
        if self.detail.get('system_mem_contention'):
            self.suggestion['system_mem_contention'] = "Check whether there exists external " \
                                                       "process which snatch resources"
            return True
        return False

    @property
    def abnormal_network_status(self):
        details = []
        # determine whether the current network packet loss rate is abnormal
        # format is: [{'device': 'ens2f0', 'drop': 0.02}, {...}, ...]
        for item in self.network_info.receive_drop:
            if item['drop'] >= monitoring.get_detection_threshold('package_drop_rate_threshold'):
                details.append('The receive drop rate is abnormal on device: %s, rate: %s' %
                               (item['device'], item['drop']))
        for item in self.network_info.transmit_drop:
            if item['drop'] >= monitoring.get_detection_threshold('package_drop_rate_threshold'):
                details.append('The transmit drop rate is abnormal on device: %s, rate: %s' %
                               (item['device'], item['drop']))
        # determine whether the current network bandwidth usage is abnormal
        for device, bandwidth_info in self.network_info.bandwidth_usage.items():
            if isinstance(bandwidth_info, dict):
                if max(bandwidth_info['transmit']) >= \
                        monitoring.get_detection_threshold('network_bandwidth_usage_threshold'):
                    details.append('The transmit bandwidth rate is abnormal on device: %s, rate: %s' %
                                   (device, max(bandwidth_info['transmit'])))
                if max(bandwidth_info['receive']) >= \
                        monitoring.get_detection_threshold('network_bandwidth_usage_threshold'):
                    details.append('The receive bandwidth rate is abnormal on device: %s, rate: %s' %
                                   (device, max(bandwidth_info['receive'])))
            elif isinstance(bandwidth_info, list):
                if max(bandwidth_info) >= monitoring.get_detection_threshold('network_bandwidth_usage_threshold'):
                    details.append('The bandwidth rate is abnormal on device: %s, rate: %s' %
                                   (device, max(bandwidth_info)))
        if details:
            self.detail['network_status'] = ';'.join(details)
            self.suggestion['network_status'] = "Diagnose the current network situation in time."
            return True
        return False

    @property
    def os_resource_contention(self) -> bool:
        """Determine whether other processes outside the database occupy too many handle resources."""
        if self.system_info.process_fds_rate and \
                max(self.system_info.process_fds_rate) >= \
                monitoring.get_detection_threshold('handler_occupation_threshold'):
            self.detail['os_resource_contention'] = "The system fds occupation rate is significant: %s;" \
                                                    % max(self.system_info.process_fds_rate)
            self.suggestion['os_resource_contention'] = "Determine the handle resource is occupied " \
                                                        "by database or other processes"
            return True
        return False

    @property
    def database_wait_event(self) -> bool:
        if self.wait_event_info and self.wait_event_info.wait_event:
            self.detail['wait_event'] = "wait_status: %s, wait_event: %s" % \
                                        (self.wait_event_info.wait_status, self.wait_event_info.wait_event)
        if self.detail.get('wait_event'):
            self.suggestion['wait_event'] = "No Suggestion"
            return True
        return False

    @property
    def lack_of_statistics(self) -> bool:
        """
        The business table has not updated statistics for a long time,
        which may result in serious deterioration of the execution plan.
        """
        abnormal_tables = []
        for table_info in self.table_structure:
            if table_info.data_changed_delay == -1:
                continue
            if table_info.tuples_diff > \
                    monitoring.get_slow_sql_param('tuples_diff_threshold'):
                abnormal_tables.append("%s:%s(%s tuples)" % (table_info.schema_name,
                                                             table_info.table_name,
                                                             table_info.tuples_diff))
        if abnormal_tables:
            self.detail['lack_of_statistics'] = "Statistics not updated in time. " \
                                                "Detail: %s" % ','.join(abnormal_tables)
            self.suggestion['lack_of_statistics'] = "Timely update statistics to help the " \
                                                    "planner choose the most suitable plan"
            return True
        return False

    @property
    def missing_index(self):
        """Use the workload-index-recommend interface to get the recommended index."""
        if self.recommend_index_info:
            self.detail['missing_index'] = 'Missing required index'
            self.suggestion['missing_index'] = 'Recommended index: %s' % str(self.recommend_index_info)
            return True
        return False

    @property
    def poor_join_performance(self):
        """
        The poor_join_performance include the following situations:
          case1: The GUC parameter 'enable_hashjoin=off', result in more tendency to NestLoop or other join operator,
                 although the scenario is suitable for HashJoin.
          case2: The optimizer incorrectly chooses the NestLoop operator(>10000), although the 'set_hashjoin=on'.
          case3: The join operation involves a large amount of data(>1000000), lead to high execution cost.
          case4: The cost of join operator is expensive.
        In general:
          The scenarios which suitable for NestLoop: 1) inner table has suitable index.
                                                     2) the tuple of outer table is small(<10000).
          The scenarios which suitable for HashJoin: Suitable for tables with large amounts of data(>10000),
                                                     and index will reduce HashJoin performance to a certain extent.
                                                     Note: it need high memory consumption.
        """
        if self.plan_parse_info is None:
            return False
        indexes = ['a', 'b', 'c']
        self.detail['poor_join_performance'], self.suggestion['poor_join_performance'] = '', ''
        nestloop_info = self.plan_parse_info.find_operators('Nested Loop', accurate=False)
        hash_inner_join_info = self.plan_parse_info.find_operators('Hash Join', accurate=False)
        hash_left_join_info = self.plan_parse_info.find_operators('Hash Left Join', accurate=False)
        hash_right_join_info = self.plan_parse_info.find_operators('Hash Right Join', accurate=False)
        hash_right_semi_join_info = self.plan_parse_info.find_operators('Hash Right Semi Join', accurate=False)
        hash_semi_join_info = self.plan_parse_info.find_operators('Hash Semi Join', accurate=False)
        hash_full_join_info = self.plan_parse_info.find_operators('Hash Full Join', accurate=False)
        hash_anti_join_info = self.plan_parse_info.find_operators('Hash Anti Join', accurate=False)
        merge_join_info = self.plan_parse_info.find_operators('Merge Join', accurate=False)
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        hashjoin_info = (
                hash_inner_join_info + hash_left_join_info +
                hash_right_join_info +
                hash_full_join_info + hash_anti_join_info +
                hash_right_semi_join_info + hash_semi_join_info)
        if plan_total_cost <= 0:
            return False
        abnormal_nestloop_info = [item for item in nestloop_info if _get_operator_cost(item) / plan_total_cost >=
                                  monitoring.get_slow_sql_param('cost_rate_threshold')]
        abnormal_hashjoin_info = [item for item in hashjoin_info if _get_operator_cost(item) / plan_total_cost >=
                                  monitoring.get_slow_sql_param('cost_rate_threshold')]
        abnormal_mergejoin_info = [item for item in merge_join_info if _get_operator_cost(item) /
                                   plan_total_cost >= monitoring.get_slow_sql_param('cost_rate_threshold')]
        enable_hashjoin = self.pg_setting_info['enable_hashjoin'].setting
        large_join_node_cond, inappropriate_join_node_cond, expensive_join_cond = [], [], []
        for node in abnormal_nestloop_info + abnormal_hashjoin_info + abnormal_mergejoin_info:
            child1, child2 = node.children
            if 'Nested Loop' in node.name:
                join_filter = 'Join Filter: ' + node.properties.get('Join Filter', '')[:PROPERTY_LENGTH] + '...'
                node_cond = "%s{cost rate: %s%%, %s}" % \
                            (node.name,
                             round(_get_operator_cost(node) * 100 / plan_total_cost, 2),
                             join_filter)
            elif all(item in node.name for item in ('Hash', 'Join')):
                join_filter = ''
                if node.properties.get('Join Filter'):
                    join_filter = 'Join Filter: ' + node.properties.get('Join Filter', '')[:PROPERTY_LENGTH] + '...'
                hash_cond = 'Hash Cond: ' + node.properties.get('Hash Cond', '')[:PROPERTY_LENGTH] + '...'
                node_cond = "%s{cost rate: %s%%, %s, %s}" % \
                            (node.name,
                             round(_get_operator_cost(node) * 100 / plan_total_cost, 2),
                             hash_cond,
                             join_filter)
            else:
                merge_cond = 'Merge Cond: ' + node.properties.get('Merge Cond', '')[:PROPERTY_LENGTH] + '...'
                node_cond = "%s{cost rate: %s%%, %s}" % \
                            (node.name,
                             round(_get_operator_cost(node) * 100 / plan_total_cost, 2),
                             merge_cond)
            if 'Nested Loop' in node.name and _hashjoin_adaptor(node) and \
                    min(child1.rows, child2.rows) >= monitoring.get_slow_sql_param('nestloop_rows_threshold'):
                # If the number of outer-table rows of the nest-loop is large,
                # the join node is considered inappropriate, in addition,
                # the inner table needs to establish an efficient data access method.
                inappropriate_join_node_cond.append(node_cond)
            elif 'Merge Join' in node.name:
                inappropriate_join_node_cond.append(node_cond)
            elif max(child1.rows, child2.rows) >= monitoring.get_slow_sql_param('large_join_threshold'):
                # The amount of data in the join child node is too large
                large_join_node_cond.append(node_cond)
            else:
                expensive_join_cond.append(node_cond)
        if large_join_node_cond:
            index = indexes.pop(0)
            self.detail['poor_join_performance'] += '%s. Large Joins operation. Detail: %s' % \
                                                    (index, ','.join(large_join_node_cond))
            self.suggestion['poor_join_performance'] += '%s. Temporary tables can filter data, ' \
                                                        'reducing data orders of magnitude ' % index
        if inappropriate_join_node_cond:
            index = indexes.pop(0)
            if enable_hashjoin == 0:
                self.detail['poor_join_performance'] += '%s. Found join operators which may be not suitable. ' \
                                                        'Detail: %s' % (index, ','.join(inappropriate_join_node_cond))
                self.suggestion['poor_join_performance'] += "%s. Detect 'enable_hashjoin=off', you can set the " \
                                                            "enable_hashjoin=on and let the optimizer " \
                                                            "choose by itself " % index
            else:
                self.detail['poor_join_performance'] += "%s. The current operator may not be good, Detail: %s " % \
                                                        (index, ','.join(inappropriate_join_node_cond))
                self.suggestion['poor_join_performance'] += "%s. Optimize SQL structure to reduce JOIN cost" % index
        if not large_join_node_cond and not inappropriate_join_node_cond and expensive_join_cond:
            index = indexes.pop(0)
            self.detail['poor_join_performance'] += '%s. The Join operators is expensive. Detail: %s' % \
                                                    (index, ','.join(expensive_join_cond))
            self.suggestion['poor_join_performance'] += "%s. Optimize SQL structure to reduce JOIN cost" % index
        if self.detail['poor_join_performance']:
            return True
        return False

    @property
    def complex_boolean_expression(self):
        """
        Wrong in-clause structure can lead to wrong execution plan, it includes the following situations:
          case1: For SQL like "select * from table where column in ('x', 'x', 'x', ..., 'x');",
                 the length of in_clause is too long, it will lead to poor SQL execution performance.
        """
        boolean_expressions = sql_parsing.exists_bool_clause(self.slow_sql_instance.query)
        for expression in boolean_expressions:
            expression_number = len(expression) if isinstance(expression, list) else 0
            if expression_number >= monitoring.get_slow_sql_param('large_in_list_threshold'):
                self.detail['complex_boolean_expression'] = "Large IN-Clause, length is %s. Detail: %s" % \
                                                            (len(expression),
                                                             ','.join(expression)[:PROPERTY_LENGTH] + '...')
                self.suggestion['complex_boolean_expression'] = "Rewrite large in-clause as a constant " \
                                                                "subquery or temporary table and " \
                                                                "replace 'not in' with 'not exists'"
                return True
        return False

    @property
    def string_matching(self):
        """
        Some conditions can cause index columns to fail, now it includes the following situations:
          case1: select id from table where func(info) = 'something';
          case2: select id from table where info like '%x';
          case3: select id from table where info like '%x%';
          case4: select id from table where info like 'x%';
          case5: order by random()
        """
        indexes = ['a', 'b', 'c']
        abnormal_functions, abnormal_regulations = [], []
        self.detail['string_matching'], self.suggestion['string_matching'] = '', ''
        matching_results = sql_parsing.exists_regular_match(self.slow_sql_instance.query)
        existing_functions = sql_parsing.exists_function(self.slow_sql_instance.query)
        if self.plan_parse_info is not None:
            matching_results = [item.replace('like', '~~') for item in matching_results]
            existing_functions = [sql_parsing.remove_bracket(item) for item in existing_functions]
            seq_scan_properties = ';'.join([node.properties.get('Filter').replace('\"', '') for
                                            node in self.plan_parse_info.find_operators('Seq Scan', accurate=False)
                                            if node.properties.get('Filter') is not None])
            for function in existing_functions:
                if function in seq_scan_properties:
                    abnormal_functions.append(function)
            for matching_result in matching_results:
                if matching_result in seq_scan_properties:
                    abnormal_regulations.append(matching_result)
        else:
            abnormal_functions = existing_functions
            abnormal_regulations = matching_results
        if abnormal_regulations:
            index = indexes.pop(0)
            self.detail['string_matching'] += "%s. Existing grammatical structure " \
                                              "which may cause SeqScan: %s" % \
                                              (index, ','.join(abnormal_regulations))
            self.suggestion['string_matching'] += "%s. Rewrite LIKE %%X into a range query" % index
        if abnormal_functions:
            index = indexes.pop(0)
            self.detail['string_matching'] += "%s. Suspected to use a function on columns " \
                                              "which may cause SeqScan: %s" % \
                                              (index, ','.join(abnormal_functions))
            self.suggestion['string_matching'] += "%s. Avoid using functions or expression " \
                                                  "operations on indexed columns or " \
                                                  "create expression index for it" % index
        if self.plan_parse_info is not None:
            sort_operators = self.plan_parse_info.find_operators('Sort', accurate=True)
            if sql_parsing.regular_match(r"order\s+by\s+random()", self.slow_sql_instance.query.lower()) and \
                    sort_operators:
                index = indexes.pop(0)
                for sort_operator in sort_operators:
                    if 'random' in sort_operator.properties.get('Sort Key'):
                        self.detail['string_matching'] += "%s. Suspected to use 'order by random()' " \
                                                          "which may cause index failure" % index
                        self.suggestion['string_matching'] += "%s. Confirm whether the scene requires " \
                                                              "this operation" % index
                        break
        if self.detail['string_matching']:
            return True
        return False

    @property
    def complex_execution_plan(self):
        """
        The execution plan is complex, it includes the following situations:
          case1: Existing a large number of join operations or group operations.
          case2: The execution plan is very complex, now we judged by the height of execution plan.
        """
        if self.complex_boolean_expression:
            return False
        if self.plan_parse_info is None:
            return False
        self.detail['complex_execution_plan'], self.suggestion['complex_execution_plan'] = '', ''
        join_operator = (
                self.plan_parse_info.find_operators('Hash Join', accurate=False) +
                self.plan_parse_info.find_operators('Nested Loop', accurate=False) +
                self.plan_parse_info.find_operators('Merge Join', accurate=False)
        )
        if len(join_operator) >= monitoring.get_slow_sql_param('complex_operator_threshold'):
            self.detail['complex_execution_plan'] = "The SQL statements involves " \
                                                    "%s JOIN operators" % \
                                                    len(join_operator)
            self.suggestion['complex_execution_plan'] = "It is not recommended to have too many table join operations"
        elif self.plan_parse_info.height >= monitoring.get_slow_sql_param('plan_height_threshold'):
            self.detail['complex_execution_plan'] = "The execution plan is too complex"
            self.suggestion['complex_execution_plan'] = "No Suggestion"
        if self.detail.get('complex_execution_plan'):
            return True
        return False

    @property
    def correlated_subquery(self):
        """
        SQL execution involves sub-queries that cannot be promoted, which includes:
          case1: The execution plan contains the 'SubPlan' keyword.
        If the SQL structure not support Sublink-Release, the user needs to rewrite the SQL.
        """
        if self.plan_parse_info is None:
            return False
        self.detail['correlated_subquery'], self.suggestion['correlated_subquery'] = '', ''
        existing_subquery = sql_parsing.exists_subquery(self.slow_sql_instance.query)
        if 'SubPlan' in self.slow_sql_instance.query_plan and existing_subquery:
            self.detail['correlated_subquery'] = "There are subqueries that cannot be promoted"
            self.suggestion['correlated_subquery'] = "Try to rewrite the statement " \
                                                     "to support sublink-release"
        if self.detail.get('correlated_subquery'):
            return True
        return False

    @property
    def poor_aggregation_performance(self):
        """
        The poor_aggregation_performance include the following three situations:
          case1: the GUC parameter 'enable_hashagg=off', result in more tendency to the GroupAgg.
          case2: Existing the scenarios like 'count(distinct col)', result in the unavailability of HashAgg.
          case3: The cost of GroupAgg is expensive.
          case4: The cost of HashAgg is expensive.
        """
        if self.plan_parse_info is None:
            return False
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        if plan_total_cost <= 0:
            return False
        typical_agg_cond, special_agg_cond, abnormal_hashagg_cond, abnormal_groupagg_cond = [], [], [], []
        special_scene, typical_scene, abnormal_hashagg_scene, abnormal_groupagg_scene = False, False, False, False
        groupagg_info = self.plan_parse_info.find_operators('GroupAggregate', accurate=False)
        hashagg_info = self.plan_parse_info.find_operators('HashAggregate', accurate=False)
        if groupagg_info and sql_parsing.regular_match(
                r"count\s*\(\s*distinct \w+\)?", self.slow_sql_instance.query.lower()):
            special_scene = True
        abnormal_groupagg_info = [item for item in groupagg_info if item.children[0].name == 'Sort' and
                                  _get_operator_cost(item) / plan_total_cost >=
                                  monitoring.get_slow_sql_param('cost_rate_threshold')]
        abnormal_hashagg_info = [item for item in hashagg_info if _get_operator_cost(item) / plan_total_cost >=
                                 monitoring.get_slow_sql_param('cost_rate_threshold')]
        enable_hashagg = self.pg_setting_info['enable_hashagg'].setting
        indexes = ['a', 'b', 'c', 'd']
        self.detail['poor_aggregation_performance'], self.suggestion['poor_aggregation_performance'] = '', ''
        for node in abnormal_groupagg_info + abnormal_hashagg_info:
            node_cond = "%s{cost rate: %s%%, GroupKey: %s}" % \
                        (node.name,
                         round(100 * _get_operator_cost(node) /
                               plan_total_cost, 2), node.properties.get('Group By Key', '')[:PROPERTY_LENGTH] + '...')
            if 'GroupAggregate' in node.name and enable_hashagg == 0:
                typical_scene = True
                typical_agg_cond.append(node_cond)
            elif 'GroupAggregate' in node.name:
                abnormal_groupagg_scene = True
                abnormal_groupagg_cond.append(node_cond)
            elif 'HashAggregate' in node.name:
                abnormal_hashagg_scene = True
                abnormal_hashagg_cond.append(node_cond)
        if typical_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. Detect 'enable_hashagg=off', and current " \
                                                           "using the GroupAgg operator. Detail: %s" \
                                                           % (index, ','.join(typical_agg_cond))
            self.suggestion['poor_aggregation_performance'] += "%s. In general, HashAgg performs " \
                                                               "better than GroupAgg, but sometimes GroupAgg " \
                                                               "has better performance, you can set the " \
                                                               "enable_hashagg=on and let the optimizer " \
                                                               "choose by itself" % index
        if special_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. HashAgg does not support: 'count(distinct xx)' " % \
                                                           index
            self.suggestion['poor_aggregation_performance'] += "%s. Rewrite SQL to support HashAgg" % index
        if abnormal_groupagg_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. The GroupAgg operator cost is too expensive. " \
                                                           "Detail: %s" % (index, ','.join(abnormal_groupagg_cond))
            self.suggestion['poor_aggregation_performance'] += "%s. Check if SQL can be optimized" % index
        if abnormal_hashagg_scene:
            index = indexes.pop(0)
            self.detail['poor_aggregation_performance'] += "%s. The HashAgg operator cost is too expensive. " \
                                                           "Detail: %s" % (index, ','.join(abnormal_hashagg_cond))
            self.suggestion['poor_aggregation_performance'] += "%s. If the number of group keys or NDV is large, " \
                                                               "the hash table may be larger and lead to spill " \
                                                               "to disk" % index
        if self.detail.get('poor_aggregation_performance'):
            return True
        return False

    @property
    def abnormal_sql_structure(self):
        """Bad SQL structure leads to poor performance of SQL."""
        if self.rewritten_sql_info:
            self.detail['abnormal_sql_structure'] = "Poor SQL structure"
            self.suggestion['abnormal_sql_structure'] = self.rewritten_sql_info
            return True
        return False

    @property
    def timed_task_conflict(self):
        """Conflict with scheduled tasks during SQL execution."""
        # useless
        return False

    def __call__(self):
        self.detail['system_cause'] = {}
        self.detail['plan'] = {}
        feature_names = (
            'lock_contention',
            'many_dead_tuples',
            'heavy_scan_operator',
            'abnormal_plan_time',
            'unused_and_redundant_index',
            'update_large_data',
            'insert_large_data',
            'delete_large_data',
            'too_many_index',
            'disk_spill',
            'vacuum_event',
            'analyze_event',
            'workload_contention',
            'cpu_resource_contention',
            'io_resource_contention',
            'memory_resource_contention',
            'abnormal_network_status',
            'os_resource_contention',
            'database_wait_event',
            'lack_of_statistics',
            'missing_index',
            'poor_join_performance',
            'complex_boolean_expression',
            'string_matching',
            'complex_execution_plan',
            'correlated_subquery',
            'poor_aggregation_performance',
            'abnormal_sql_structure',
            'timed_task_conflict')
        feature_vector = []
        for feature_name in feature_names:
            try:
                feature = int(getattr(self, feature_name))
                feature_vector.append(feature)
            except Exception as e:
                logging.error(
                    'Cannot get the feature %s, for details: %s.', feature_name, e, exc_info=True
                )
                feature_vector.append(0)
        return feature_vector, self.detail, self.suggestion
