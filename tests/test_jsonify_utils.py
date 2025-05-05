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
"""
unittest case for jsonify_utils
"""

from psycopg2.extras import RealDictRow

from dbmind.service.web import context_manager
from dbmind.service.web.jsonify_utils import (
    split_ip_and_port,
    sqlalchemy_query_jsonify_for_multiple_instances,
    psycopg2_dict_jsonify,
    _sqlalchemy_query_records_logic,
    sqlalchemy_query_records_count_logic,
    sqlalchemy_query_union_records_logic,
    format_date_key
)


def test_split_ip_and_port():
    """
    unittest for split_ip_and_port
    """
    instance_ip, instance_port = split_ip_and_port('127.0.0.1')
    assert instance_ip == '127.0.0.1'
    assert instance_port is None
    instance_ip, instance_port = split_ip_and_port('127.0.0.1:8080')
    assert instance_ip == '127.0.0.1'
    assert instance_port == '8080'
    instance_ip, instance_port = split_ip_and_port('')
    assert instance_ip is None
    assert instance_port is None


class TestRow:
    col1 = None
    col2 = None
    col3 = None


def query(instance, **kwargs):
    if instance == '127.0.0.1':
        row1 = TestRow()
        row1.col1 = 'r1_c1_val'
        row1.col2 = 'r1_c2_val'
        row1.col3 = 'r1_c3_val'
        return [row1]
    elif instance == '127.0.0.2':
        row2 = TestRow()
        row2.col1 = 'r2_c1_val'
        row2.col2 = 'r2_c2_val'
        row2.col3 = 'r2_c3_val'
        return [row2]


def test_sqlalchemy_query_jsonify_for_multiple_instances():
    """
     unittest for sqlalchemy_query_jsonify_for_multiple_instances
    """
    assert sqlalchemy_query_jsonify_for_multiple_instances(
        query,
        ['127.0.0.1', '127.0.0.2'],
        field_names=['col1', 'col2', 'col3']) == {'header': ['col1', 'col2', 'col3'],
                                                  'rows': [['r1_c1_val', 'r1_c2_val', 'r1_c3_val'],
                                                           ['r2_c1_val', 'r2_c2_val', 'r2_c3_val']]}


def test_psycopg2_dict_jsonify():
    real_dict_row_1 = RealDictRow()
    real_dict_row_1['col1'] = 'r1_c1_val'
    real_dict_row_1['col2'] = 'r1_c2_val'
    real_dict_row_1['col3'] = 'r1_c3_val'
    real_dict_row_2 = RealDictRow()
    real_dict_row_2['col1'] = 'r2_c1_val'
    real_dict_row_2['col2'] = 'r2_c2_val'
    real_dict_row_2['col3'] = 'r2_c3_val'
    assert psycopg2_dict_jsonify([real_dict_row_1, real_dict_row_2]) == {'header': ['col1', 'col2', 'col3'],
                                                                         'rows':
                                                                             [['r1_c1_val', 'r1_c2_val', 'r1_c3_val'],
                                                                              ['r2_c1_val', 'r2_c2_val', 'r2_c3_val']]}


class TestRowWithStatement:
    col1 = None
    col2 = None
    col3 = None
    statement = None

    def __iter__(cls):
        for i in range(3):
            if i == 0:
                yield cls.col1
            elif i == 1:
                yield cls.col2
            elif i == 2:
                yield cls.col3

    def offset(self):
        return self

    def limit(self):
        return self


class TestStatement:
    columns = None


class TestColumn:
    def __init__(self, keys):
        self.col_keys = keys

    def keys(self):
        return self.col_keys


def query_with_columns(instance):
    if instance == '127.0.0.1':
        row1 = TestRowWithStatement()
        row1.col1 = ['r1_c1_val']
        row1.col2 = ['r1_c2_val']
        row1.col3 = ['r1_c3_val']
        statement1 = TestStatement()
        column1 = TestColumn(['col1', 'col2', 'col3'])
        statement1.columns = column1
        row1.statement = statement1
        return row1
    elif instance == '127.0.0.1:8888':
        row2 = TestRowWithStatement()
        row2.col1 = ['r2_c1_val']
        row2.col2 = ['r2_c2_val']
        row2.col3 = ['r2_c3_val']
        statement2 = TestStatement()
        column2 = TestColumn(['col1', 'col2', 'col3'])
        statement2.columns = column2
        row2.statement = statement2
        return row2


def test_sqlalchemy_query_records_logic():
    context_manager.set_access_context(**{'instance_ip_with_port_list': ['127.0.0.1:8888']})

    assert _sqlalchemy_query_records_logic(
        query_with_columns, ['127.0.0.1:8888']
    ) == {
        'header':
            ['col1', 'col2', 'col3'],
        'rows':
            [['r1_c1_val'], ['r1_c2_val'], ['r1_c3_val'], ['r2_c1_val'], ['r2_c2_val'], ['r2_c3_val']]
    }


def query_count(instance, **kwargs):
    return 3


def test_sqlalchemy_query_records_count_logic():
    assert sqlalchemy_query_records_count_logic(query_count, ['127.0.0.1']) == 3

    context_manager.set_access_context(**{'instance_ip_with_port_list': ['127.0.0.1:8888']})
    context_manager.set_access_context(**{'instance_ip_list': ['127.0.0.1']})
    assert sqlalchemy_query_records_count_logic(query_count, None) == 6
    assert sqlalchemy_query_records_count_logic(query_count, None, only_with_port=True) == 3


def test_sqlalchemy_query_union_records_logic():
    assert sqlalchemy_query_union_records_logic(
        query_with_columns, ['127.0.0.1']
    ) == {
        'header': ['col1', 'col2', 'col3'],
        'rows': [['r1_c1_val'], ['r1_c2_val'], ['r1_c3_val']]
    }

    context_manager.set_access_context(**{'instance_ip_with_port_list': '127.0.0.1:8888'})
    assert sqlalchemy_query_union_records_logic(
        query_with_columns, None, only_with_port=True
    ) == {
        'header': ['col1', 'col2', 'col3'],
        'rows': [['r2_c1_val'], ['r2_c2_val'], ['r2_c3_val']]
    }


def test_format_date_key():
    obj = {'last_updated': '2023-07-23 12:34:56.789000+00:00'}
    assert format_date_key(obj, 'last_updated') == '2023-07-23 12:34:56'
