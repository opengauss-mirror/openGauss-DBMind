#!/usr/bin/env python3
# coding=utf-8
# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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

"""This file contains test cases for distribution key recommendation."""

import importlib
import unittest
from collections import Iterable
import json
import shlex
import sys
import io
import copy

from dbmind.components.dkr import dk_advisor
from dbmind.components.dkr.sqlparse_processing.extract_rules import ExtractPerSQL, WorkloadInfo
from dbmind.components.dkr.dk_advisor_alg import DFSAlg, GraphAlg
from dbmind.components.dkr.utils import *
from dbmind.components.dkr.parsing import Plan
from dbmind.components.dkr.parsing import count_indent

os.putenv('PYTHONHASHSEED', '0')


def hash_any(obj):
    try:
        return hash(obj)
    except:
        h = 0
        for item in obj:
            h = 31 * h + (hash_any(item) & 255)
        return h


def list_equal(list1, list2):
    def is_iterable(L):
        return isinstance(L, Iterable) and not isinstance(L, str)

    def is_nested_list(L):
        if not is_iterable(L):
            return False
        return len(L) > 0 and is_iterable(L[0])

    assert is_iterable(list1)

    list1_copy = sorted(list1, key=hash_any)
    list2_copy = sorted(list2, key=hash_any)
    for a, b in zip(list1_copy, list2_copy):
        if is_nested_list(a):
            return list_equal(a, b)
        if a != b:
            print("False is ", a, b)
            return False

    return True


class Case:
    global_cost_dict1 = {'JOIN': [[[[['dams_wf_task', 'task_id'], ['jbpm4_participation', 'task_'], 1],
                                    [['jbpm4_participation', 'userid_'], ['dams_user', 'ssic_id'], 1]]], [
                                      [[['dams_inner_todo_task', 'ssic_id'], ['dams_user', 'ssic_id'], 1],
                                       [['dams_wf_task', 'prev_task_id'], ['dams_wf_participation', 'pre_task_id'], 1],
                                       [['dams_user', 'ssic_id'], ['dams_inner_todo_task', 'ssic_id'], 1]]], [
                                      [[['dams_user_role_rel', 'stru_id'], ['dams_branch_relation', 'bnch_id'], 1],
                                       [['dams_user_role_rel', 'stru_id'], ['dams_branch_catalog', 'stru_id'], 1],
                                       [['dams_branch_catalog', 'major_stru_id'], ['dams_branch', 'stru_id'], 1],
                                       [['dams_user_role_rel', 'role_id'], ['dams_role', 'role_id'], 1],
                                       [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 1],
                                       [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 1],
                                       [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 1],
                                       [['dams_user_role_rel', 'role_id'], ['dams_role', 'role_id'], 1],
                                       [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 1],
                                       [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 1],
                                       [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 1],
                                       [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 1]]], [
                                      [[['dams_user', 'ssic_id'], ['dams_wf_hist_task', 'ssic_id'], 1],
                                       [['dams_user', 'ssic_id'], ['dams_wf_hist_process', 'creator_id'], 1],
                                       [['dams_branch', 'stru_id'], ['dams_wf_hist_process', 'creator_stru'], 1],
                                       [['dams_wf_hist_task', 'procinst_id'], ['dams_wf_hist_process', 'procinst_id'],
                                        1],
                                       [['dams_wf_process_conf', 'pdid'], ['dams_wf_hist_process', 'process_id'], 1],
                                       [['dams_wf_hist_task', 'procinst_id'], ['dams_wf_participation', 'procinst_id'],
                                        1],
                                       [['dams_wf_hist_task', 'prev_task_id'], ['dams_wf_participation', 'pre_task_id'],
                                        1],
                                       [['dams_wf_hist_task', 'procinst_id'], ['dams_wf_hist_task', 'procinst_id'], 1],
                                       [['dams_wf_hist_task', 'procinst_id'], ['dams_wf_hist_process', 'procinst_id'],
                                        1],
                                       [['dams_user', 'ssic_id'], ['dams_inner_over_task', 'ssic_id'], 1],
                                       [['dams_user', 'ssic_id'], ['dams_inner_over_task', 'prev_ssic_id'], 1],
                                       [['dams_user', 'ssic_id'], ['dams_inner_over_task', 'creator_id'], 1],
                                       [['dams_branch', 'stru_id'], ['dams_inner_over_task', 'creator_stru'], 1]]], [
                                      [[['dams_wf_task', 'task_id'], ['dams_wf_task', 'prev_task_id'], 1],
                                       [['dams_user', 'ssic_id'], ['dams_wf_process', 'creator_id'], 1],
                                       [['dams_branch', 'stru_id'], ['dams_wf_process', 'creator_stru'], 1]]],
                                  [[[['dams_wf_participation', 'ssic_id'], ['dams_user', 'ssic_id'], 1]]],
                                  [[[['dams_wf_participation', 'ssic_id'], ['dams_user', 'ssic_id'], 1]]], [], []],
                         'GROUP_ORDER': [[], [], [], [[['dams_wf_hist_task', 'procinst_id'], 1]],
                                         [[['dams_wf_process_conf', 'pdid'], 1]],
                                         [[['dams_wf_participation', 'pre_task_id'], 1],
                                          [['dams_wf_participation', 'procinst_id'], 1]],
                                         [[['dams_wf_participation', 'pre_task_id'], 1],
                                          [['dams_wf_participation', 'procinst_id'], 1]],
                                         [], []]}
    global_cost_dict2 = {
        'JOIN': [[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []],
        'GROUP_ORDER': [[], [[['nation', 'n_name'], 1], [['supplier', 's_name'], 1], [['part', 'p_partkey'], 1]], [],
                        [], [[['nation', 'n_name'], 1]], [], [], [], [],
                        [[['customer', 'c_custkey'], 1], [['customer', 'c_name'], 1], [['customer', 'c_phone'], 1],
                         [['nation', 'n_name'], 1], [['customer', 'c_address'], 1], [['customer', 'c_comment'], 1]], [],
                        [], [[['customer', 'c_custkey'], 1]], [], [], [], [],
                        [[['customer', 'c_name'], 1], [['customer', 'c_custkey'], 1], [['orders', 'o_orderkey'], 1]],
                        [], [[['supplier', 's_name'], 1]], [[['supplier', 's_name'], 1]], [],
                        [[['customer', 'c_custkey'], 5]]]}

    sample = ["id order by asset;",
              "id, name order by id;",
              "(id, name);",
              "(id), name;"
              ]

    extract_table_sql = \
        "explain analyze select listagg( name ,',' )  within GROUP ( ORDER BY name ) from " \
        "dams_wf_task t,jbpm4_participation p,dams_user u1 ,( (WITH RECURSIVE migora_cte AS " \
        "( SELECT business_id,1 lv,source_business_id FROM dams_wf_sub_process_rel r WHERE " \
        "r.source_business_id = 'SYFX2020032402842363'  UNION ALL " \
        "SELECT r.business_id,( mig_ora_cte_tab_alias.lv + 1 ) lv,r.source_business_id FROM " \
        "migora_cte mig_ora_cte_tab_alias INNER JOIN dams_wf_sub_process_rel r ON " \
        "mig_ora_cte_tab_alias.business_id = r.source_business_id ) SELECT business_id,lv," \
        "source_business_id FROM migora_cte mig_ora_cte_tab_alias ORDER BY lv )UNION ALL SELECT " \
        "'SYFX2020032402842363' ,0 lv ,'') sp WHERE t.business_id = sp.business_id AND " \
        "t.task_state = 'open' AND t.task_id = p.task_ AND p.userid_ = u1.ssic_id;"

    transaction_equal_cond = [('PDCI', {'1': ['PDCICARDNO']}),
                              ('APSM', {'1': ['APSMPROCOD', 'APSMACTNO']}),
                              ('PDCR', {'1': ['PDCRPROCOD'], '2': ['PDCRCARDNO']})]

    transaction_summary = [[[{'APSM': 'APSMPROCOD', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRPROCOD'}, 10],
                [{'APSM': 'APSMACTNO', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRPROCOD'}, 5],
                [{'APSM': 'APSMACTNO', 'PDCR': 'PDCRPROCOD'}, 20]],
               [[{'APSM': 'APSMPROCOD', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRPROCOD'}, 10],
                [{'APSM': 'APSMPROCOD', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRCARDNO'}, 10],
                [{'APSM': 'APSMACTNO', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRPROCOD'}, 10],
                [{'APSM': 'APSMACTNO', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRCARDNO'}, 10]]]

    _explains = None

    @staticmethod
    def explains():
        if Case._explains is None:
            Case._explains = importlib.import_module('.execplan_cases', 'tests')
        return Case._explains


class DK_Tester(unittest.TestCase):
    def test_calc_time_join_cond(self):
        result1 = dk_advisor.count_freq_join_cond(Case.global_cost_dict1['JOIN'])
        expect1 = [[['dams_branch', 'stru_id'], ['dams_inner_over_task', 'creator_stru'], 1],
                   [['dams_branch', 'stru_id'], ['dams_wf_hist_process', 'creator_stru'], 1],
                   [['dams_branch', 'stru_id'], ['dams_wf_process', 'creator_stru'], 1],
                   [['dams_branch_catalog', 'major_stru_id'], ['dams_branch', 'stru_id'], 1],
                   [['dams_branch_relation', 'bnch_id'], ['dams_user_role_rel', 'stru_id'], 7],
                   [['dams_inner_todo_task', 'ssic_id'], ['dams_user', 'ssic_id'], 1],
                   [['dams_user', 'ssic_id'], ['dams_inner_over_task', 'ssic_id'], 1],
                   [['dams_user', 'ssic_id'], ['dams_inner_over_task', 'prev_ssic_id'], 1],
                   [['dams_user', 'ssic_id'], ['dams_inner_over_task', 'creator_id'], 1],
                   [['dams_user', 'ssic_id'], ['dams_inner_todo_task', 'ssic_id'], 1],
                   [['dams_user', 'ssic_id'], ['dams_wf_hist_process', 'creator_id'], 1],
                   [['dams_user', 'ssic_id'], ['dams_wf_hist_task', 'ssic_id'], 1],
                   [['dams_user', 'ssic_id'], ['dams_wf_process', 'creator_id'], 1],
                   [['dams_user_role_rel', 'stru_id'], ['dams_branch_catalog', 'stru_id'], 1],
                   [['dams_user_role_rel', 'stru_id'], ['dams_branch_relation', 'bnch_id'], 1],
                   [['dams_user_role_rel', 'role_id'], ['dams_role', 'role_id'], 2],
                   [['dams_wf_hist_task', 'procinst_id'], ['dams_wf_hist_process', 'procinst_id'], 2],
                   [['dams_wf_hist_task', 'procinst_id'], ['dams_wf_hist_task', 'procinst_id'], 1],
                   [['dams_wf_hist_task', 'procinst_id'], ['dams_wf_participation', 'procinst_id'], 1],
                   [['dams_wf_hist_task', 'prev_task_id'], ['dams_wf_participation', 'pre_task_id'], 1],
                   [['dams_wf_participation', 'ssic_id'], ['dams_user', 'ssic_id'], 2],
                   [['dams_wf_process_conf', 'pdid'], ['dams_wf_hist_process', 'process_id'], 1],
                   [['dams_wf_task', 'prev_task_id'], ['dams_wf_participation', 'pre_task_id'], 1],
                   [['dams_wf_task', 'task_id'], ['dams_wf_task', 'prev_task_id'], 1],
                   [['dams_wf_task', 'task_id'], ['jbpm4_participation', 'task_'], 1],
                   [['jbpm4_participation', 'userid_'], ['dams_user', 'ssic_id'], 1]]
        self.assertTrue(list_equal(result1, expect1))

        result2 = dk_advisor.count_freq_join_cond(Case.global_cost_dict2['JOIN'])
        expect2 = []
        self.assertTrue(list_equal(result2, expect2))

    def test_calc_time_group_cond(self):
        result1 = dk_advisor.count_freq_grp_order_cond(Case.global_cost_dict1['GROUP_ORDER'])
        expect1 = [('dams_wf_participation pre_task_id', 2), ('dams_wf_participation procinst_id', 2),
                   ('dams_wf_hist_task procinst_id', 1), ('dams_wf_process_conf pdid', 1)]
        self.assertTrue(list_equal(result1, expect1))

        result2 = dk_advisor.count_freq_grp_order_cond(Case.global_cost_dict2['GROUP_ORDER'])
        expect2 = [('customer c_custkey', 8), ('nation n_name', 3), ('supplier s_name', 3), ('customer c_name', 2),
                   ('part p_partkey', 1), ('customer c_phone', 1), ('customer c_address', 1), ('customer c_comment', 1),
                   ('orders o_orderkey', 1)]
        self.assertTrue(list_equal(result2, expect2))

    def test_extract_group_order(self):
        expects = [['id'], ['id, name'], ['(id, name)'], ['(id)', 'name']]
        for i, sql in enumerate(Case.sample):
            query = dk_advisor.QueryItem(sql, 1)
            workload_info = WorkloadInfo()
            sql_rules = ExtractPerSQL(query.statement, 1, 3, workload_info)
            parsed = sqlparse.parse(query.statement)[0]
            result = sql_rules.extract_group_order_clause(parsed)
            result = list(map(lambda T: T.value, result))
            self.assertEqual(expects[i], result)

    def test_extract_insert_clause(self):
        sql = "INSERT INTO APSD (APSDTRBK, APSDTRCOD, APSDTRAMT) values  (  '00',  0,'09')"
        expects = {"'00'": ['apsdtrbk'], "'09'": ['apsdtramt'], '0': ['apsdtrcod']}
        workload_info = WorkloadInfo()
        sql_rules = ExtractPerSQL(sql, 1, 3, workload_info)
        sql_rules.extract_insert_clause()
        self.assertDictEqual(expects, sql_rules.equal_const_dict)

    def test_transaction_dk_recommend(self):
        expect = {'APSM': 'APSMACTNO', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRPROCOD'}
        extra_list = copy.deepcopy(Case.transaction_summary[1])
        extra_list.append([{'APSM': 'APSMPROCOD', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRCARDNO'}, 10])
        result = {}
        dk_advisor.transaction_dk_recommand(Case.transaction_summary[0],
                                            extra_list, result)
        self.assertDictEqual(expect, result)

    def test_process_transaction_equal_cond(self):
        expect = [[{'APSM': 'APSMPROCOD', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRPROCOD'}, 10],
                  [{'APSM': 'APSMACTNO', 'PDCI': 'PDCICARDNO', 'PDCR': 'PDCRPROCOD'}, 10]]
        global_transaction_equal_cond = []
        global_transaction_table_columns = []
        dfs_alg = DFSAlg(Case.transaction_equal_cond, 10)
        dfs_alg.process_transaction_equal_cond(global_transaction_equal_cond,
                                               global_transaction_table_columns)
        print(global_transaction_equal_cond)
        self.assertEqual(str(expect), str(global_transaction_equal_cond))
        # self.assertEqual(str(Case.transaction_summary[1]), str(global_transaction_table_columns))

    def test_extract_base_tables(self):
        expects = {'dams_wf_sub_process_rel': ['r'], 'dams_wf_task': ['t'], 'dams_user': ['u1'],
                   'jbpm4_participation': ['p'], 'migora_cte': ['mig_ora_cte_tab_alias'], 'sp': ['sp']}
        query_item = QueryItem(Case.extract_table_sql, 1)
        sql = query_item.statement
        parsed_sql = sqlparse.parse(sql)[0]
        workload_info = WorkloadInfo()
        sql_rules = ExtractPerSQL(sql, 1, 3, workload_info)
        tables = sql_rules.extract_base_tables(parsed_sql)
        print(tables)
        self.assertDictEqual(expects, tables)

    def test_integration(self):
        expected_dkr = {'customer': 'c_custkey', 'lineitem': 'l_orderkey', 'orders': 'o_orderkey',
                        'part': 'p_partkey', 'partsupp': 'ps_partkey', 'supplier': 's_suppkey',
                        'nation': 'n_name', 'region': 'r_regionkey'}
        expected_dkr_exclude_repl = {'customer': 'c_custkey', 'lineitem': 'l_orderkey',
                                     'orders': 'o_orderkey', 'part': 'p_partkey', 'partsupp': 'ps_partkey',
                                     'supplier': 's_suppkey', 'region': 'r_regionkey'}
        expected_repl = ['nation']

        abspath = os.path.abspath(os.path.dirname(__file__))
        os.chdir(abspath)
        dn_num = 6
        with open('tpch.sql') as wl, open('statistics.json') as stat:
            workload, dml_workload = dk_advisor.workload_compression(dk_advisor.get_sqls(wl), None)
            dk_advisor.DATA_NODE_NUM = 6
            dk_advisor.DRIVER = False 
            dkr, replications, workload_info = \
                dk_advisor.generate_distribution_key(workload, dml_workload, 'offline', None,
                                                     json.load(stat), 'naive', None, None)
            self.assertTrue(list_equal(expected_repl, replications))
            self.assertDictEqual(dkr, expected_dkr)
            dkr, replications = dk_advisor.offline_interface('tpch.sql', 'statistics.json', dn_num, pass_filepath=True)
            self.assertTrue(list_equal(expected_repl, replications))
            self.assertDictEqual(dkr, expected_dkr_exclude_repl)

    def test_remote(self):
        if not os.path.exists('remote.json'):
            print("Not found remote.json file so not tested for remote.")
            return
        with open('remote.json') as f:
            config = json.load(f)
            for mode in ['online', 'offline', 'gsql', 'wdr']:
                cmd = config[mode]['cmd']
                pwd = config[mode]['pwd']
                sys.stdin = io.IOBase()
                mock_r, mock_w = os.pipe()
                os.write(mock_w, 'mock text'.encode())
                sys.stdin.fileno = lambda: mock_r
                sys.stdin.readable = lambda: True
                sys.stdin.read = lambda: pwd
                sys.argv[1:] = shlex.split(cmd)
                ret = dk_advisor.main()


class TestParsing(unittest.TestCase):
    def test_recognize_level(self):
        lines = Case.explains().CASE4.strip('\n').splitlines()
        plan = Plan()
        plan.primal_indent_len = count_indent(lines[0])
        expected = [0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 4,
                    5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 8, 9, 9, 10, 10, 11, 11, 12, 12,
                    13, 13, 3, 3, 0]
        for idx, line in enumerate(lines):
            self.assertEqual(plan.recognize_level(line), expected[idx])

    def test_parsing(self):
        plan = Plan()
        expected_heights = [2, 2, 1, 14]
        explains = Case.explains()
        for i, case in enumerate((explains.CASE1, explains.CASE2, explains.CASE3, explains.CASE4)):
            plan.parse(case)
            result_lines = str(plan).strip().splitlines()
            expected_lines = case.strip().splitlines()
            self.assertEqual(len(result_lines), len(expected_lines))
            for result, expected in zip(result_lines, expected_lines):
                self.assertTrue(expected.find(result.strip()) >= 0)
            self.assertEqual(expected_heights[i], plan.height)

    def test_sorted_opts(self):
        plan = Plan()
        plan.parse(Case.explains().CASE4)
        self.assertGreater(plan.sorted_operators[0].exec_cost, 35682.35)

    def test_join_callback(self):
        plan = Plan()
        plan.parse(Case.explains().CASE4)
        result = []
        workload_info = WorkloadInfo()
        plan.traverse(dk_advisor.get_find_join_callback(workload_info, 1, result))
        expect1 = [[('partsupp', 'ps_suppkey'), ('supplier', 's_suppkey'), 64058.11],
                   [('nation', 'n_nationkey'), ('supplier', 's_nationkey'), 768.06],
                   [('nation', 'n_regionkey'), ('region', 'r_regionkey'), 48.9],
                   [('partsupp', 'ps_partkey'), ('part', 'p_partkey'), 34743.33]]
        self.assertIsNotNone(result)

        graph_alg = GraphAlg(workload_info, 6)
        join_graph = graph_alg.naive_maximum_alg([], result, 'optimizer')
        print(join_graph)

    def test_filter_callback(self):
        plan = Plan()
        plan.parse(Case.explains().CASE4)
        result = {}
        workload_info = WorkloadInfo()
        plan.traverse(dk_advisor.get_find_filter_callback(workload_info, result))
        print(result)


if __name__ == '__main__':
    unittest.main()
