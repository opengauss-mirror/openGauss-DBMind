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
from dbmind.common.parser.sql_parsing import existing_computation, existing_inequality_compare


def test_get_column():
    sql = "select col1, col2 from tab where id = 1;"
    assert sql_parsing.get_columns(sql) == ['id']
    sql = "select col1, col2 from tab where id + 1 = 1;"
    assert sql_parsing.get_columns(sql) == ['id']
    sql = "select col1, col2 from tab where id = 1 and c = 2;"
    expected_result = {'id', 'c'}
    assert set(sql_parsing.get_columns(sql)) == expected_result
    sql = 'select col1, col2 from tab where id + "1" = 2;'
    assert sql_parsing.get_columns(sql) == ['id']


def test_wdr_sql_processing():
    sql = "insert into tab1 values (1, 'jack', 10);"
    assert sql_parsing.wdr_sql_processing(sql) == 'INSERT INTO tab1 VALUES'


def test_standardize_sql():
    sql = "select    col1, col2 from   tab where id =1;"
    assert sql_parsing.standardize_sql(sql) == 'SELECT col1, col2 FROM tab WHERE id = 1;'


def test_fill_value():
    sql = "update tab1 set name = $1 where id = $2;parameters: $1 = 'jerry', $2 = 1"
    assert sql_parsing.fill_value(sql) == "update tab1 set name = 'jerry' where id = 1"


def test_exist_regular_match():
    sql = "select col1 from tab where name like '%erry'"
    assert sql_parsing.exists_regular_match(sql) == ["like '%erry'"]


def test_exists_regular_match_patterns_no_like():
    query = "SELECT * FROM table WHERE column = 'value'"
    assert sql_parsing.exists_regular_match(query) == []


def test_exists_regular_match_patterns_no_left_match():
    query = "SELECT * FROM table WHERE column LIKE 'value'"
    assert sql_parsing.exists_regular_match(query) == []


def test_exists_regular_match_patterns_left_match():
    query = "SELECT * FROM table WHERE column LIKE '%value'"
    assert sql_parsing.exists_regular_match(query) == ["LIKE '%value'"]


def test_exists_regular_match_patterns_multiple_left_match():
    query = "SELECT * FROM table WHERE column LIKE '%value' AND column2 LIKE '%value2'"
    assert sql_parsing.exists_regular_match(query) == ["LIKE '%value'", "LIKE '%value2'"]


def test_exists_regular_match_patterns_not_like():
    query = "SELECT * FROM table WHERE column NOT LIKE '%value'"
    assert sql_parsing.exists_regular_match(query) == ["NOT LIKE '%value'"]


def test_exists_regular_match_patterns_mixed():
    query = "SELECT * FROM table WHERE column LIKE '%value' AND column2 NOT LIKE '%value2'"
    assert sql_parsing.exists_regular_match(query) == ["LIKE '%value'", "NOT LIKE '%value2'"]


def test_exists_regular_match_with_whitespace():
    query = "SELECT * FROM table WHERE column LIKE '%value' AND column2 NOT LIKE '%value2'"
    assert sql_parsing.exists_regular_match(query) == ["LIKE '%value'", "NOT LIKE '%value2'"]


def test_exists_regular_match_with_parentheses():
    query = "SELECT * FROM table WHERE (column LIKE '%value') AND (column2 NOT LIKE '%value2')"
    assert sql_parsing.exists_regular_match(query) == ["LIKE '%value'", "NOT LIKE '%value2'"]


def test_exist_track_parameter():
    sql = "update tab1 set name = $1 where id = $2; parameters: $1 = 'jerry', $2 = 1"
    assert sql_parsing.exist_track_parameter(sql)


def test_is_query_normalized():
    sql1 = "select    col1, col2 from   tab where id =$1;"
    sql2 = "select    col1, col2 from   tab where id =1;"
    assert sql_parsing.is_query_normalized(sql1)
    assert not sql_parsing.is_query_normalized(sql2)


def test_remove_parameter_part():
    sql = "update tab1 set name = $1 where id = $2;parameters: $1 = 'jerry', $2 = 1"
    assert sql_parsing.remove_parameter_part(sql) == 'update tab1 set name = $1 where id = $2;'


def test_exists_function():
    sql = "select * from table where abs(l_quantity) <= 8;"
    assert sql_parsing.exists_function(sql) == ['abs(l_quantity)']


def test_regular_match():
    assert sql_parsing.regular_match(r"^\d{2}\w{3}$", "12abc")


def test_remove_bracket():
    sql = "insert into tab values ($1, $2)"
    assert sql_parsing.remove_bracket(sql).strip() == 'insert into tab values'


def test_exists_subquery():
    sql = "select id from (select id from table2);"
    assert sql_parsing.exists_subquery(sql) == [('SELECT id FROM table2', 1)]


def test_get_generate_prepare_sqls_function():
    sql = "update tab1 set name = $1 where id = $2;"
    assert sql_parsing.get_generate_prepare_sqls_function()(sql) == \
           ['prepare prepare_0 as update tab1 set name = $1 where id = $2',
            'explain execute prepare_0(NULL,NULL)', 'deallocate prepare prepare_0']


def test_replace_question_mark_with_value():
    sql = "select * from tab1 where date >= date ?"
    assert sql_parsing.replace_question_mark_with_value(sql) == "select * from tab1 where date >= date '1999-01-01'"


def test_replace_question_mark_with_dollar():
    sql = "UPDATE bmsql_customer SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + ? " \
          "WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4 and c_info = ?;"
    assert sql_parsing.replace_question_mark_with_dollar(sql) == "UPDATE bmsql_customer " \
                                                                 "SET c_balance = c_balance + $1, " \
                                                                 "c_delivery_cnt = c_delivery_cnt + $5 " \
                                                                 "WHERE c_w_id = $2 AND c_d_id = $3 " \
                                                                 "AND c_id = $4 and c_info = $6;"


def test_exists_count_operation():
    sql = "select count(1) from tab"
    assert sql_parsing.exists_count_operation(sql)


def test_existing_computation():
    sql = 'select * from t1 left join t2 where t2.c1 + "3" = "123" and t1.id + 1 = 10;'
    actual_flags, actual_columns = existing_computation(sql)
    assert actual_flags == ['t2.c1 + "3"', 't1.id + 1']
    assert set(actual_columns) == {'t2.c1', 't1.id'}
    sql = 'select * from t1 left join t2 where t1.id = 10;'
    actual_flags, actual_columns = existing_computation(sql)
    assert actual_flags == []
    assert actual_columns == []


def test_existing_inequality_compare():
    sql = 'select * from t1 left join t2 where t2.c1 + "3" < "123" and t1.id + 1 <> 10;'
    actual_flags, actual_columns = existing_inequality_compare(sql)
    assert actual_flags == ['t2.c1 + "3" < "123"', 't1.id + 1 <> 10']
    assert set(actual_columns) == {'t2.c1', 't1.id'}
    sql = 'select * from t1 left join t2 where t1.id + 1 = 10;'
    actual_flags, actual_columns = existing_inequality_compare(sql)
    assert actual_flags == []
    assert actual_columns == []


def test_exist_count_operation():
    assert sql_parsing.exists_count_operation("SELECT count(*) FROM table")
    assert sql_parsing.exists_count_operation("SELECT COUNT(1) FROM table")

    # Test the query without count function
    assert not sql_parsing.exists_count_operation("SELECT * FROM table")
    assert not sql_parsing.exists_count_operation("SELECT column FROM table")
    assert not sql_parsing.exists_count_operation("SELECT column FROM table WHERE condition")
    assert not sql_parsing.exists_count_operation("SELECT column FROM table WHERE condition, column2")

    # Test case insensitivity
    assert sql_parsing.exists_count_operation("SELECT COUNT(*) FROM table")
    assert sql_parsing.exists_count_operation("SELECT count(*) FROM table")
