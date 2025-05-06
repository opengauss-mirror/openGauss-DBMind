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
from dbmind.common.utils import dbmind_assert
from .enums import ALARM_LEVEL


class _Define:
    def __init__(self, category,
                 detail=None,
                 suggestion='',
                 level=ALARM_LEVEL.WARNING):
        self.category = category
        self.detail = detail
        self.level = level
        self.suggestion = suggestion
        self.title = ''


class RootCause:
    SYSTEM_ERROR = _Define('[SYSTEM]', 'System error')

    # system level
    SYSTEM_ANOMALY = _Define('[SYSTEM][UNKNOWN]', 'Monitoring metric has anomaly.')
    WORKING_CPU_CONTENTION = _Define('[SYSTEM][CPU]', 'Workloads compete to use CPU resources.',
                                     'Reduce CPU intensive concurrent clients.')
    WORKING_IO_CONTENTION = _Define('[SYSTEM][IO]', 'Workloads compete to use IO resources.',
                                    'Reduce IO intensive concurrent clients.')
    LARGE_IO_CAPACITY = _Define('[SYSTEM][IO]', 'Current IO CAPACITY is too large.',
                                'Reduce IO intensives.')
    WORKING_MEM_CONTENTION = _Define('[SYSTEM][MEMORY]', 'Workloads compete to use memory resources.',
                                     'Reduce memory intensive concurrent clients.')
    SMALL_SHARED_BUFFER = _Define('[SYSTEM][BUFFER]', 'shared buffer is small.',
                                  'Tune the shared_buffer parameter larger.')
    TABLE_EXPANSION = _Define('[SYSTEM][EXPANSION]', 'too many dirty tuples exist.',
                              'Vacuum the database.')
    VACUUM = _Define('[SYSTEM][VACUUM]', 'An vacuum operation is being performed during SQL execution.',
                     'Adjust the freqency of vacuum.')
    BGWRITER_CHECKPOINT = _Define('[SYSTEM][CHECKPOINT]', 'background checkpoint.',
                                  'Adjust the frequency of checkpoint.')
    ANALYZE = _Define('[SYSTEM][ANALYZE]', 'An analyze operation is being performed during SQL execution.',
                      'Adjust the frequency of analyze.')
    WALWRITER = _Define('[SYSTEM][WAL]', 'writing WAL.',
                        'Reduce insert/update concurrent clients.')
    FULL_CONNECTIONS = _Define('[SYSTEM][CONNECTIONS]', 'too many connections.',
                               'Reduce number of clients.')
    FAST_CONNECTIONS_INCREASE = _Define('[SYSTEM][CONNECTIONS]', 'connections grows too fast.',
                                        'Mentions the impact of business.')

    COMPLEX_SLOW_QUERY = _Define('[SYSTEM][SLOWQUERY]', 'slow queries exist.',
                                 'Check the slow query diagnosis for more details.')
    REPLICATION_SYNC = _Define('[SYSTEM][REPLICATION]', 'replication delay.',
                               'Repair the states of stand-by servers.')
    LOW_NETWORK_BANDWIDTH = _Define('[SYSTEM][NETWORK]', 'network is busy.')
    LOW_IO_BANDWIDTH = _Define('[SYSTEM][IO]', 'IO is busy.')
    LOW_CPU_IDLE = _Define('[SYSTEM][CPU]', 'CPU is busy.')
    HIGH_CPU_USAGE = _Define('[SYSTEM][CPU]', 'CPU usage is too high.',
                             'Suggestions:\na). Reduce the workloads.\nb). Increase CPU core numbers.')
    DISK_WILL_SPILL = _Define('[SYSTEM][DISK]', 'Disk will spill.', 'Properly expand the disk capacity.')
    DISK_BURST_INCREASE = _Define('[SYSTEM][DISK]', 'Disk usage increase too fast.',
                                  'a) Check for business impact and expand disk capacity.')
    MEMORY_USAGE_BURST_INCREASE = _Define('[SYSTEM][MEMORY]', 'Memory usage grows faster', 'Check for business impact.')
    HIGH_MEMORY_USAGE = _Define('[SYSTEM][MEMORY]', 'Memory usage is too high', 'Check for business impact.')
    QPS_VIOLENT_INCREASE = _Define('[DB][QPS]', 'Database QPS rises sharply.', 'NONE')
    POOR_SQL_PERFORMANCE = _Define('[DB][SQL]', 'Database has poor performance of SQL.', 'NONE')
    # slow query
    REMOTE_QUERY = _Define('[SLOW SQL][OPERATOR]',
                           'Sql can not be pushdown.',
                           'Avoid using functions or expressions which can not be pushdown.')
    LARGE_BROADCAST = _Define('[SLOW SQL][OPERATOR]',
                              'Large table in Broadcast streaming.',
                              'Optimize the sql to avoid large table in broadcast streaming.')
    LOCK_CONTENTION = _Define('[SLOW QUERY][LOCK]',
                              '{lock_contention}.',
                              'Adjust the business reasonably to avoid lock blocking.')
    MANY_DEAD_TUPLES = _Define('[SLOW QUERY][TABLE EXPANSION]',
                               '{many_dead_tuples}.',
                               '{many_dead_tuples}.')
    HEAVY_SCAN_OPERATOR = _Define('[SLOW QUERY][SCAN]',
                                  '{heavy_scan_operator}.',
                                  '{heavy_scan_operator}.')
    ABNORMAL_PLAN_TIME = _Define('[SLOW SQL][PLAN]',
                                 'hard parse cost too much time, '
                                 'detail: {abnormal_plan_time}.',
                                 '{abnormal_plan_time}.')
    UNUSED_AND_REDUNDANT_INDEX = _Define('[SLOW SQL][UNUSED_AND_REDUNDANT_INDEX]',
                                         '{unused_and_redundant_index}.',
                                         '{unused_and_redundant_index}.')
    UPDATE_LARGE_DATA = _Define('[SLOW SQL][UPDATED TUPLES]',
                                '{update_large_data}.',
                                '{update_large_data}.'
                                )
    INSERT_LARGE_DATA = _Define('[SLOW SQL][INSERTED TUPLES]',
                                '{insert_large_data}.',
                                '{insert_large_data}.'
                                )
    DELETE_LARGE_DATA = _Define('[SLOW SQL][DELETED TUPLES]',
                                '{delete_large_data}.',
                                '{delete_large_data}.')
    TOO_MANY_INDEX = _Define('[SLOW SQL][INDEX NUMBER]',
                             '{too_many_index}.',
                             '{too_many_index}.')
    DISK_SPILL = _Define('[SLOW SQL][EXTERNAL SORT]',
                         '{disk_spill}.',
                         '{disk_spill}.')
    VACUUM_EVENT = _Define('[SLOW SQL][VACUUM]',
                           'During SQL execution, related tables are executing VACUUM tasks, '
                           'resulting in slow queries,'
                           'detail: {vacuum}.',
                           )
    ANALYZE_EVENT = _Define('[SLOW SQL][ANALYZE]',
                            'During SQL execution, related tables are executing ANALYZE tasks, '
                            'resulting in slow queries,'
                            'detail: {analyze}.',
                            )
    WORKLOAD_CONTENTION = _Define('[SLOW SQL][WORKLOAD]',
                                  '{workload_contention}.',
                                  '{workload_contention}.'
                                  )

    CPU_RESOURCE_CONTENTION = _Define('[SLOW SQL][SYSTEM CPU]',
                                      '{system_cpu_contention}.',
                                      '{system_cpu_contention}.'
                                      )
    IO_RESOURCE_CONTENTION = _Define('[SLOW SQL][SYSTEM IO]',
                                     '{system_io_contention}.',
                                     '{system_io_contention}.'
                                     )
    MEMORY_RESOURCE_CONTENTION = _Define('[SLOW SQL][SYSTEM IO]',
                                         '{system_mem_contention}.',
                                         '{system_mem_contention}.'
                                         )
    ABNORMAL_NETWORK_STATUS = _Define('[SLOW SQL][NETWORK]',
                                      '{network_status}.',
                                      '{network_status}.'
                                      )
    OS_RESOURCE_CONTENTION = _Define('[SLOW SQL][FILE HANDLER]',
                                     '{os_resource_contention}.',
                                     '{os_resource_contention}.'
                                     )
    WAIT_EVENT = _Define('[SLOW SQL][DATABASE]',
                         '{wait_event}.',
                         )
    LACK_STATISTIC_INFO = _Define('[SLOW SQL][DATABASE]',
                                  '{lack_of_statistics}.',
                                  '{lack_of_statistics}.')
    MISSING_INDEXES = _Define('[SLOW SQL][MISSING INDEX]',
                              '{missing_index}.',
                              '{missing_index}.'
                              )
    POOR_JOIN_PERFORMANCE = _Define('[SLOW SQL][OPERATOR]',
                                    '{poor_join_performance}.',
                                    '{poor_join_performance}.')
    COMPLEX_BOOLEAN_EXPRESSIONS = _Define('[SLOW SQL][OPERATOR]',
                                          '{complex_boolean_expression}.',
                                          '{complex_boolean_expression}.')
    STRING_MATCHING = _Define('[SLOW SQL][OPERATOR]',
                              '{string_matching}.',
                              '{string_matching}.')
    COMPLEX_EXECUTION_PLAN = _Define('[SLOW SQL][OPERATOR]',
                                     '{complex_execution_plan}.',
                                     '{complex_execution_plan}.')
    CORRELATED_SUBQUERY = _Define('[SLOW SQL][TASK]',
                                  '{correlated_subquery}.',
                                  '{correlated_subquery}.')
    POOR_AGGREGATION_PERFORMANCE = _Define('[SLOW SQL][PLAN]',
                                           '{poor_aggregation_performance}.',
                                           '{poor_aggregation_performance}.')
    ABNORMAL_SQL_STRUCTURE = _Define('[SLOW SQL][PLAN]',
                                     '{abnormal_sql_structure}.',
                                     '{abnormal_sql_structure}.')
    RISK_INFORMATION = _Define('[SLOW SQL][WDR]',
                               '{risk_information}')
    DATABASE_VIEW = _Define('[SLOW SQL][VIEW]',
                            'Poor performance of database views',
                            'System table query service, no suggestion.')
    ILLEGAL_SQL = _Define('[SLOW SQL][SQL]',
                          'Only support UPDATE, DELETE, INSERT, SELECT')
    EXISTING_EXCEPTION = _Define('[SLOW SQL][EXCEPTION]',
                                 'Exception occurred during diagnosis.')
    NO_ROOT_CAUSE_FOUND = _Define('[SLOW SQL][UNKNOWN]', 'Not found the possible root cause.')
    INVALID_SQL = _Define('[SLOW SQL][INVALID]',
                          'Not found the possible root cause.',
                          'Confirm whether the SQL is legal. '
                          'Confirm the schema and database is correct')
    UNSUPPORTED_TYPE = _Define('[SLOW SQL][TYPE]',
                               'The SQL type is not supported.')
    LACK_INFORMATION = _Define('[LACK INFORMATION]',
                               'LACK NECESSARY INFORMATION.')
    # security
    TOO_MANY_INVALID_LOGINS = _Define(
        '[SECURITY][RISK]',
        'Too many invalid logins to the database is a short in a short period of time,'
        ' a brute-force attack may have occurred.',
        ' Please check whether the access interface exposed to the user is secure.'
    )
    SCANNING_ATTACK = _Define(
        '[SECURITY][RISK]',
        'The database has produced too many execution errors or too many invalid logins in a short period of time,'
        ' a scanning or penetration attack may have occurred.',
        ' Please check whether the access interface exposed to the user is secure and for no vulnerable application.'
    )
    TOO_MANY_USER_VIOLATION = _Define(
        '[SECURITY][RISK]',
        'Too many user violation in a short period of time,'
        ' Please check whether there is operational issue or maybe someone is exploring the database.'
    )

    # Define more root causes *above*.

    @staticmethod
    def has(title):
        return isinstance(title, str) and hasattr(RootCause, title.upper())

    @staticmethod
    def get(title):
        """Generate dynamic ``RootCause`` object."""
        defined = getattr(RootCause, title.upper())
        dbmind_assert(isinstance(defined, _Define))
        return RootCause(title.upper(), 1., defined)

    def format(self, *args, **kwargs):
        self.detail = self.detail.format(*args, **kwargs)
        return self

    def format_suggestion(self, *args, **kwargs):
        self.suggestion = self.suggestion.format(*args, **kwargs)
        return self

    def __init__(self, title, probability, defined):
        self.title = title
        self.probability = probability
        self.category = defined.category
        self.detail = defined.detail
        self.level = defined.level
        self.suggestion = defined.suggestion

    def set_probability(self, probability):
        self.probability = probability
        return self

    def set_detail(self, detail):
        self.detail = detail
        return self

    def set_level(self, level):
        self.level = level
        return self

    def __repr__(self):
        return 'RootCause{title=%s, category=%s, level=%s, detail=%s, prob=%f}' % (
            self.title, self.category, self.level, self.detail, self.probability
        )


# Set the title for each _Define object.
for attr in dir(RootCause):
    if attr.isupper() and not attr.startswith('_'):
        root_cause = getattr(RootCause, attr)
        setattr(root_cause, 'title', attr)
