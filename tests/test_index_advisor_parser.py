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
from dbmind.components.index_advisor import parser

sql1 = """
    select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
    from
            lineitem
    where
            l_shipdate <= date '1998-12-01' - interval ':1' day (3)
    group by
            l_returnflag,
            l_linestatus
    order by
            l_returnflag,
            l_linestatus
    limit 10;
"""

sql2 = """
    select
            c_custkey,
            c_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
    from
            customer,
            orders,
            lineitem,
            nation
    where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and o_orderdate >= date '1993-02-01'
            and o_orderdate < date '1993-02-01' + interval '3' month
            and l_returnflag = 'R'
            and c_nationkey = n_nationkey
    group by
            c_custkey,
            c_name,
            c_acctbal,
            c_phone,
            n_name,
            c_address,
            c_comment
    order by
            revenue desc
    LIMIT 20;
"""

sql3 = "select employee_address, phone_number from company t1 join employee t2 on t1.id=t2.id;"
sql4 = "update table1 set name='test' where id>5"


def test_get_query_tables():
    assert parser.get_query_tables(sql1)[0] == 'lineitem'
    assert set(parser.get_query_tables(sql2)) == {'customer', 'orders', 'lineitem', 'nation'}
    assert set(parser.get_query_tables(sql3)) == {'company', 'employee'}


def test_get_potential_columns():
    assert set(parser.get_potential_columns(sql1)) == {'l_linestatus', 'avg_qty', 'l_discount', 'l_tax', 'avg_price',
                                                       'sum_charge', 'l_extendedprice', 'l_quantity', 'sum_qty',
                                                       'avg_disc', 'avg', 'l_shipdate', 'sum_disc_price',
                                                       'sum_base_price', 'count', 'sum', 'count_order', 'lineitem',
                                                       'l_returnflag'}
    assert set(parser.get_potential_columns(sql2)) == {'c_acctbal', 'o_custkey', 'orders', 'o_orderdate', 'l_discount',
                                                       'n_nationkey', 'sum', 'l_orderkey', 'c_custkey', 'nation',
                                                       'l_extendedprice', 'c_name', 'l_returnflag', 'c_nationkey',
                                                       'c_phone', 'c_comment', 'revenue', 'c_address', 'customer',
                                                       'n_name', 'lineitem', 'o_orderkey'}
    assert set(parser.get_potential_columns(sql3)) == {'employee_address', 'phone_number', 'id', 'company', 'employee',
                                                       't1', 't2'}


def test_get_updated_columns():
    assert parser.get_updated_columns(sql4) == ('table1', {'name'})
