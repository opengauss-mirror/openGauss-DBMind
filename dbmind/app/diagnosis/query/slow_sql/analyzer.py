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
from collections import defaultdict
from typing import List, Dict
import time

import numpy as np
from sql_metadata.compat import get_query_tables

from dbmind.common.types.root_cause import RootCause
from dbmind import global_vars

from .featurelib import load_feature_lib, get_feature_mapper
from .query_feature import QueryFeature
from ..slow_sql import query_info_source, SlowQuery

# In general, 'application name' can be used to judge whether it is a DevOps-SQL,
# but it cannot be provided in an interactive situation, so in order to roughly
# judge whether it is DevOps-SQL when the database cannot be connected, we write them locally.
# Due to the complexity of actual business, the results may not be accurate, but its impact is limited.
_system_table_keywords = ('PG_CLASS', 'PG_STAT_DATABASE', 'PG_STAT_REPLICATION', 'STATEMENT',
                          'STATEMENT_HISTORY', 'GLOBAL_WAIT_EVENTS', 'PG_STAT_USER_TABLES',
                          'PG_STATIO_USER_TABLES', 'PG_STAT_USER_INDEXES', 'PG_STATIO_USER_INDEXES',
                          'PG_STAT_DATABASE_CONFLICTS', 'PG_STAT_ACTIVITY', 'PG_LOCKS', 'PG_INDEXES',
                          'GLOBAL_SESSION_STAT_ACTIVITY', 'PG_STAT_REPLICATION', 'PG_REPLICATION_SLOTS',
                          'PG_DATABASE', 'PG_CONTROL_CHECKPOINT', 'PG_STAT_BGWRITER', 'GS_SESSION_MEMORY_DETAIL',
                          'GLOBAL_RECOVERY_STATUS', 'PG_XLOG_LOCATION_DIFF', 'STATEMENT_RESPONSETIME_PERCENTILE',
                          'PG_PREPARED_XACTS', 'GS_SQL_COUNT', 'PG_JOB', 'GS_REDO_STAT', 'LOCL_CKPT_STAT',
                          'LOCAL_DOUBLE_WRITE_STAT', 'GS_TOTAL_MEMORY_DETAIL', 'SUMMARY_FILE_IOSTAT', 'INSTANCE_TIME',
                          'PG_JOB', 'PG_NAMESPACE', 'PG_CONSTRAINT')
_system_query_threshold = 0.6


FEATURE_LIB = load_feature_lib()
FEATURES_CAUSE_MAPPER = get_feature_mapper()
FEATURES, CAUSES, WEIGHT_MATRIX = FEATURE_LIB['features'], FEATURE_LIB['labels'], FEATURE_LIB['weight_matrix']


def _vector_distance(vector1, vector2, label: int, weight_matrix: List[List]) -> float:
    """
    Calculate the distance between vectors based on the improved Hamming algorithm
    :param vector1: input vector
    :param vector2: input vector
    :param label: input label value of vector2
    :param weight_matrix: weight matrix
    :return: distance of two vectors
    """
    if len(vector1) != len(vector2):
        raise ValueError('not equal.')
    distance = 0.0
    for index in range(len(vector1)):
        if vector1[index] == vector2[index] and vector1[index]:
            distance += weight_matrix[label - 1][index] * vector1[index]
    return distance


def _euclid_distance(vector1, vector2) -> float:
    """
    Calculate the distance between vectors based on the euclid algorithm
    :param vector1: input vector
    :param vector2: input vector
    :return: distance of two vectors
    """
    dist = np.sqrt(np.sum(np.square([item1 - item2 for item1, item2 in zip(vector1, vector2)])))
    return round(dist, 4)


def _calculate_nearest_feature(sql_feature) -> List:
    """
    Return the topk feature that is most similar to the input vector
    :param sql_feature: input vector
    :return: The most similar feature and its index information
    """
    indexes = []
    for feature, cause in zip(FEATURES, CAUSES):
        dis = _vector_distance(feature, sql_feature, cause, WEIGHT_MATRIX)
        indexes.append((dis, cause))
    indexes = sorted(indexes, key=lambda x: x[1], reverse=True)
    indexes = list(filter(lambda x: x[0], indexes))
    filter_indexes = []
    _id = 0
    while _id < len(indexes):
        max_dis = 0
        while _id + 1 < len(indexes) and indexes[_id][1] == indexes[_id + 1][1]:
            max_dis = max(indexes[_id][0], indexes[_id + 1][0])
            _id += 1
        filter_indexes.append([max(max_dis, indexes[_id][0]), indexes[_id][1]])
        _id += 1
    probability_sum = sum([item[0] for item in filter_indexes])
    filter_indexes = [[item[0] / probability_sum, item[1]] for item in filter_indexes]
    filter_indexes = sorted(filter_indexes, key=lambda x: x[0], reverse=True)
    return filter_indexes


class SlowSQLAnalyzer:
    """
    Classes for diagnosing slow SQL
    """

    def __init__(self, buffer_capacity: int = 500, buffer=None):
        """
        :param buffer_capacity: The length of slow SQL buffer queue
        :param buffer: A shared list for each worker.
        """
        self.sql_buffers = buffer if buffer is not None else []
        self.buffer_capacity = buffer_capacity

    def run(self, query_context) -> [SlowQuery, None]:
        """
        API for slow SQL diagnostic calls
        :param query_context: The context of a slow query
        :return: The result of slow query, if the slow query has been diagnosed, then return None
        """
        if self._is_diagnosed(query_context.slow_sql_instance, strict=True):
            query_context.slow_sql_instance.mark_replicated()
            return
        self._analyze(query_context)
        return query_context.slow_sql_instance

    def _is_diagnosed(self, slow_sql_instance: SlowQuery, strict=False) -> bool:
        """Determine if the SQL has been diagnosed, return True if it has been diagnosed, else return False"""
        # To reduce duplicated diagnosis results, we use a list to record
        # flags that mark if a slow query has been diagnosed.
        # In fact, we should implement a LRU-based buffer instead of an ordinary list.
        # But for the interaction among multiple processes, an LRU-based buffer is more complicated.
        if strict:
            diagnosed_flag_tuple = (slow_sql_instance.start_at,
                                    slow_sql_instance.duration_time,
                                    slow_sql_instance.template_id)
            if diagnosed_flag_tuple in self.sql_buffers:
                return True
        else:
            # Add start time for expired action.
            sql_hashcode = slow_sql_instance.hash_query(use_root_cause=True)
            diagnosed_flag_tuple = (slow_sql_instance.start_at,) + sql_hashcode

            retention_seconds = global_vars.dynamic_configs.get_int_or_float('self_monitoring',
                                                                             'result_retention_seconds')
            min_retention_time = time.time() - retention_seconds
            # Evict expired records.
            i = 0
            while i < len(self.sql_buffers):
                flag_tuple = self.sql_buffers[i]
                start_time = flag_tuple[0] / 1000  # The first element always represents start time.
                if start_time <= min_retention_time:
                    self.sql_buffers.pop(i)
                    i -= 1
                i += 1
            # Look for the diagnosed flag.
            for flag_tuple in self.sql_buffers:
                cached_hashcode = flag_tuple[1], flag_tuple[2]
                if cached_hashcode == sql_hashcode:
                    return True

        self.sql_buffers.append(diagnosed_flag_tuple)
        if len(self.sql_buffers) >= self.buffer_capacity:
            self.sql_buffers.pop(0)
        return False

    @staticmethod
    def associate_table_with_schema(query: str, schema_name: str, exist_tables: Dict):
        """Find schema and table in query."""
        tables_info = get_query_tables(query)
        for table_info in tables_info:
            if '.' in table_info:
                schema, table = table_info.split('.')
            else:
                schema, table = schema_name, table_info
            exist_tables[schema].append(table)

    def _analyze(self, query_context: query_info_source.QueryContext) -> [SlowQuery, None]:
        """Slow SQL diagnosis main process"""
        exist_tables = defaultdict(list)
        # if the SQL is not valid, we just need to skip it
        if not query_context.is_sql_valid:
            root_cause = RootCause.get(FEATURES_CAUSE_MAPPER.get('C_INVALID_SQL'))
            query_context.slow_sql_instance.add_cause(root_cause)
            return
        query = query_context.slow_sql_instance.query
        self.associate_table_with_schema(query, query_context.slow_sql_instance.schema_name, exist_tables)
        if len(exist_tables) and \
                sum(item in query_context.slow_sql_instance.query.upper() for item in _system_table_keywords) / \
                len(exist_tables) > _system_query_threshold:
            root_cause = RootCause.get(FEATURES_CAUSE_MAPPER.get('C_VIEW'))
            query_context.slow_sql_instance.add_cause(root_cause)
            return
        query_context.slow_sql_instance.tables_name = exist_tables
        feature_generator = QueryFeature(query_context)
        feature_generator.initialize_metrics()
        feature, details, suggestions = feature_generator()
        logging.debug("[SLOW QUERY] Feature vector: %s, detail: %s", feature, details)
        topk_root_cause = _calculate_nearest_feature(feature)
        logging.debug("[SLOW QUERY] Topk root cause: %s", topk_root_cause)
        if topk_root_cause:
            for probability, index in topk_root_cause:
                cause_key = f"C{index}"
                title = FEATURES_CAUSE_MAPPER.get(cause_key, 'C_UNKNOWN')
                root_cause = RootCause.get(title)
                root_cause.set_probability(probability)
                root_cause.format(**details)
                root_cause.format_suggestion(**suggestions)
                query_context.slow_sql_instance.add_cause(root_cause)
        if not query_context.slow_sql_instance.root_causes:
            title = FEATURES_CAUSE_MAPPER.get('C_UNKNOWN')
            root_cause = RootCause.get(title)
            query_context.slow_sql_instance.add_cause(root_cause)
        # Further to avoid repetition and diagnosis for slow sql
        if self._is_diagnosed(query_context.slow_sql_instance, strict=False):
            query_context.slow_sql_instance.mark_replicated()
