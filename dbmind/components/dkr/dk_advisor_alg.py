#!/usr/bin/env python3
# coding=utf-8
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

"""This file is used to implement the core algorithms of distribution key recommendation.
Including the generation and use of join relationship graph,
the recommendation of replication tables,
the recommendation of distribute transaction.
"""
from .utils import *


class EdgeState:
    UNUSED = -1
    TYPE1 = 0
    TYPE2 = 1
    FORBID = 2


class Edge:

    def __init__(self, tbl_a, tbl_b, col_a, col_b, cost):
        self.vertex_from = tbl_a
        self.vertex_to = tbl_b
        self.attr_from = col_a
        self.attr_to = col_b
        self.weight = cost
        self.color = EdgeState.UNUSED

    def __repr__(self):
        return '(%s.%s --- %s ---> %s.%s)' % (self.vertex_from, self.attr_from,
                                              self.weight, self.vertex_to,
                                              self.attr_to)


class BaseInfo:
    def __init__(self, workload_info, dn_num):
        self.workload_info = workload_info
        self.dn_num = dn_num


class GraphAlg(BaseInfo):
    """
    Use the graph to extract the optimal combination of joins,
    through the maximum spanning tree algorithm
    """

    def __init__(self, workload_info, dn_num):
        super(GraphAlg, self).__init__(workload_info, dn_num)
        self._join_graph = {}

    def _build_graph_common(self, table_a, table_b, col_a, col_b, cost):
        if table_a not in self._join_graph.keys():
            self._join_graph[table_a] = []
        if table_b not in self._join_graph.keys():
            self._join_graph[table_b] = []
        self._join_graph[table_a].append(Edge(table_a, table_b, col_a, col_b, cost))
        self._join_graph[table_b].append(Edge(table_b, table_a, col_b, col_a, cost))

    def _calculate_edge_weight(self, table_a, table_b):
        """
        The join relationship adopts the minimum value of the
         cost generated when redistributing or broadcasting.
        """
        size_a = self.workload_info.wl_tables[table_a].table_size
        size_b = self.workload_info.wl_tables[table_b].table_size
        # This is a empirical formula on distributed database.
        return min(size_a + size_b, min(size_a, size_b) * self.dn_num)

    def _build_join_graph(self, join_cond_list):
        for join_cond in join_cond_list:
            from_table, to_table, freq = join_cond
            table_a, col_a = from_table
            table_b, col_b = to_table
            cost = self._calculate_edge_weight(table_a, table_b) * freq
            self._build_graph_common(table_a, table_b, col_a, col_b, cost)

    def _build_join_graph_from_plan(self, total_join_cond_list):
        for join_cond in total_join_cond_list:
            table_a, col_a = join_cond[0][0], join_cond[0][1]
            table_b, col_b = join_cond[1][0], join_cond[1][1]
            cost = join_cond[2]
            self._build_graph_common(table_a, table_b, col_a, col_b, cost)

    def naive_maximum_alg(self, dkr, join_cond_list, cost_type):
        """
        This is a maximum spanning tree algorithm.
        According to the constructed join relationship graph,
         find the optimal distribution key configuration according to graph theory.
        """
        if cost_type == 'naive':
            self._build_join_graph(join_cond_list)
        else:
            self._build_join_graph_from_plan(join_cond_list)
        edges = []
        seen_vertex = set()

        # Sort all edges in the graph in descending order of weight value
        for vertex in self._join_graph.keys():
            seen_vertex.add(vertex)
            for edge in self._join_graph[vertex]:
                if edge.vertex_to in seen_vertex:
                    continue
                edges.append(edge)
        edges = sorted(edges, key=lambda _edge: _edge.weight, reverse=True)

        # Process the edges with the highest weight in turn,
        # and add the edges that meet the conditions to the result set.
        for edge in edges:
            if edge.vertex_from not in dkr.keys(
            ) and edge.vertex_to not in dkr.keys():
                dkr[edge.vertex_from] = edge.attr_from
                dkr[edge.vertex_to] = edge.attr_to
            elif edge.vertex_from not in dkr.keys() and edge.vertex_to in dkr.keys():
                if dkr[edge.vertex_to] == edge.attr_to:
                    dkr[edge.vertex_from] = edge.attr_from
            elif edge.vertex_from in dkr.keys() and edge.vertex_to not in dkr.keys():
                if dkr[edge.vertex_from] == edge.attr_from:
                    dkr[edge.vertex_to] = edge.attr_to


class DFSAlg:
    """
    Use deep first Search algorithm to,
    extract all combination of same value and frequent column for a transaction
    """

    def __init__(self, transaction_equal_cond, freq):
        """
        :param transaction_equal_cond: [(t1, {v1: [c1, c2], v2: [c3]}),(t2, {v1:[c1], v2:[c2]})]
        :param freq: sql frequency
        """
        self.transaction_equal_cond = transaction_equal_cond
        self.freq = freq

    def _get_all_combination(self, transaction_table_columns, depth,
                             global_transaction_table_columns):
        """
        extract frequent column combination
        get all combinations of different SQL columns in the same transaction
        """
        if depth == len(self.transaction_equal_cond):
            # Sort by table name
            sort_result = dict(
                sorted(transaction_table_columns, key=lambda elem: elem[0]))
            global_transaction_table_columns.append([sort_result, self.freq])
            return
        table = self.transaction_equal_cond[depth][0]
        columns = set()
        for item in self.transaction_equal_cond[depth][1].values():
            columns.update(item)
        is_invalid = False
        for column in columns:
            for item in transaction_table_columns:
                if table == item[0] and column != item[1]:
                    is_invalid = True
                    break
            if is_invalid:
                continue
            transaction_table_columns.append((table, column))
            self._get_all_combination(transaction_table_columns, depth + 1,
                                      global_transaction_table_columns)
            transaction_table_columns.pop()

    def _get_all_valid_cond(self, value, valid_equal_cond, depth, global_transaction_equal_cond):
        """
        extract the same value combination
        get all cases where different SQL has the same value in a transaction
        """
        if depth == len(self.transaction_equal_cond):
            if len(self.transaction_equal_cond) == len(valid_equal_cond):
                # sort by table name
                valid_equal_cond = dict(
                    sorted(valid_equal_cond, key=lambda elem: elem[0]))
                global_transaction_equal_cond.append([valid_equal_cond, self.freq])
            return
        if value not in self.transaction_equal_cond[depth][1].keys():
            return
        table = self.transaction_equal_cond[depth][0]
        is_invalid = False
        for column in self.transaction_equal_cond[depth][1][value]:
            for item in valid_equal_cond:
                if table == item[0] and column != item[1]:
                    is_invalid = True
                    break
            if is_invalid:
                continue
            valid_equal_cond.append((table, column))
            self._get_all_valid_cond(value, valid_equal_cond, depth + 1,
                                     global_transaction_equal_cond)
            valid_equal_cond.pop()

    def process_transaction_equal_cond(self, global_transaction_equal_cond,
                                       global_transaction_table_columns):
        """
        :param global_transaction_equal_cond: [[{t1:c1, t2:c1}, freq],[{t1:c2, t2:c1}, freq]]
        :param global_transaction_table_columns: [[{t1:c1, t2:c1}, freq],[{t1:c1, t2:c2},freq],...]
        :return: NA
        """
        # extract the same value combination.
        table = self.transaction_equal_cond[0][0]
        for value, columns in self.transaction_equal_cond[0][1].items():
            for column in columns:
                valid_equal_cond = [(table, column)]
                self._get_all_valid_cond(value, valid_equal_cond, 1, global_transaction_equal_cond)
        # extract frequent column combination.
        self._get_all_combination([], 0, global_transaction_table_columns)


class RepTblAlg(BaseInfo):
    """
    Replication table recommend algorithm
    """

    def __init__(self, dml_workload, workload_info, dn_num):
        super(RepTblAlg, self).__init__(workload_info, dn_num)
        self.dml_workload = dml_workload

    def _compute_group_order_cost(self, tbl_name, group_order, dkr):
        if not group_order or tbl_name != group_order[0].split(' ')[0]:
            return
        self.workload_info.wl_replication_tables[tbl_name].replication_cost += \
            self.workload_info.wl_replication_tables[tbl_name].tuple_count * group_order[1]
        # group by key is distribution key
        if group_order[0].split(' ')[1] == dkr[tbl_name]:
            self.workload_info.wl_replication_tables[tbl_name].distribution_cost += \
                self.workload_info.wl_replication_tables[tbl_name].tuple_count \
                / self.dn_num * group_order[1]
        else:
            self.workload_info.wl_replication_tables[tbl_name].distribution_cost += \
                self.workload_info.wl_replication_tables[tbl_name].tuple_count + \
                self.workload_info.wl_replication_tables[tbl_name].tuple_count \
                / self.dn_num * group_order[1]

    def _compute_iud_cost(self, tbl_name, cost_obj):
        # get the number of occurrences of the table
        # in the dml statement defined in REPL_SUPPORT_TYPE.
        for sql in self.dml_workload:
            if tbl_name not in sql.statement:
                continue
            for repl_type, index in REPL_SUPPORT_TYPE.items():
                segment = sql.statement.split(' ')
                pos = [ind for ind, item in enumerate(segment) if repl_type in item]
                if pos and (pos[0] + index) < len(segment) and tbl_name == segment[pos[0] + index]:
                    cost_obj.replication_cost += cost_obj.tuple_count * sql.frequency
                    cost_obj.distribution_cost += cost_obj.tuple_count / self.dn_num * sql.frequency

    def _determine_base_table(self, assoc_cond, dkr):
        """Determine which table is a replication table"""
        # not support the scenario where both tables are in the candidate replication tables.
        if assoc_cond[0][0] in self.workload_info.wl_replication_tables and \
                assoc_cond[1][0] not in self.workload_info.wl_replication_tables:
            base_table = assoc_cond[0]
            assoc_table = assoc_cond[1]
        elif assoc_cond[1][0] in self.workload_info.wl_replication_tables and \
                assoc_cond[0][0] not in self.workload_info.wl_replication_tables:
            base_table = assoc_cond[1]
            assoc_table = assoc_cond[0]
        else:
            return None

        base_table_size = self.workload_info.wl_replication_tables[base_table[0]].tuple_count
        # get association table size from wl_tables.
        assoc_table_size = 0
        for table_obj in self.workload_info.wl_tables:
            if table_obj == assoc_table[0]:
                assoc_table_size = self.workload_info.wl_tables[table_obj].tuple_count
                break

        if assoc_table[0] not in dkr:
            return None
        return base_table, base_table_size, assoc_table, assoc_table_size

    def _compute_multi_association_cost(self, assoc_cond, dkr):
        """
        :param self.workload_info: workload information
        :param assoc_cond: [[base_table_name, association_key],
        [association_table, association_key], 1]
        :param dkr: distribution key result
        :return: NA
        """
        tables_info = self._determine_base_table(assoc_cond, dkr)
        if not tables_info:
            return
        base_table, base_table_size, assoc_table, assoc_table_size = tables_info
        # when current table is replication table,
        # whether the associated table column is a distributed key has the same cost.
        # M/dn_num + N.
        self.workload_info.wl_replication_tables[base_table[0]].replication_cost += \
            (base_table_size + assoc_table_size /
             self.dn_num) * assoc_cond[2]

        # current table is distribution table.
        # the associated key of the associated table is the distribution key.
        if assoc_table[1] == dkr[assoc_table[0]]:
            # The associated key of the current table is the distribution key.
            # (M + N)/dn_num.
            if base_table[1] == dkr[base_table[0]]:
                self.workload_info.wl_replication_tables[base_table[0]].distribution_cost += \
                    (base_table_size + assoc_table_size) / \
                    self.dn_num * assoc_cond[2]
            else:
                # min(M + (M + N)/dn_num, N*dn_num + (N + M/dn_num)).
                self.workload_info.wl_replication_tables[base_table[0]].distribution_cost += \
                    min(base_table_size + (base_table_size + assoc_table_size) / self.dn_num,
                        assoc_table_size * self.dn_num +
                        assoc_table_size + base_table_size / self.dn_num) * assoc_cond[2]
        # the associated key of the associated table not is the distribution key.
        else:
            # the associated key of the current table is the distribution key.
            # min(M + (M + N)/dn_num, N*dn_num + (N + M/dn_num)).
            if base_table[1] == dkr[base_table[0]]:
                self.workload_info.wl_replication_tables[base_table[0]].distribution_cost += \
                    min(base_table_size + (base_table_size + assoc_table_size) / self.dn_num,
                        assoc_table_size * self.dn_num +
                        assoc_table_size + base_table_size / self.dn_num) * assoc_cond[2]
            else:
                # min(M + N + (M + N)/dn_num, min(M,N)*dn_num + (min(M,N) + max(M,N)/dn_num)).
                self.workload_info.wl_replication_tables[base_table[0]].distribution_cost += \
                    min(base_table_size + assoc_table_size +
                        (base_table_size + assoc_table_size) / self.dn_num,
                        base_table_size * self.dn_num +
                        assoc_table_size / self.dn_num) * assoc_cond[2]

    def replication_table_recommend(self, dkr, replication_size):
        replication = []
        # if the user specifies replication table size, then recommended according to the parameter.
        if replication_size:
            for tbl_name, tbl_item in self.workload_info.wl_tables.items():
                is_valid_rep = tbl_item.tuple_count < replication_size \
                               and self.workload_info.wl_tables[tbl_name].table_size != 0 \
                               and self.workload_info.wl_tables[tbl_name].columns
                if is_valid_rep:
                    replication.append(tbl_name)
            return replication

        for cond in self.workload_info.wl_replication_pairs:
            if isinstance(cond, list):
                # is multi-table association scene.
                self._compute_multi_association_cost(cond, dkr)
            if isinstance(cond, dict):
                # is GROUP/ORDER scene.
                self._compute_group_order_cost(cond[0].split(' ')[0], cond, dkr)
        for tbl_name, cost_obj in self.workload_info.wl_replication_tables.items():
            if self.workload_info.wl_tables[tbl_name].table_size == 0 or not \
                    self.workload_info.wl_tables[tbl_name].columns:
                continue
            self._compute_iud_cost(tbl_name, cost_obj)
            if cost_obj.distribution_cost > cost_obj.replication_cost:
                replication.append(tbl_name)
        return replication
