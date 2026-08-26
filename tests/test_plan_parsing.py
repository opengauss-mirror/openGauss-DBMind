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

from dbmind.common.parser import plan_parsing

plan0 = """
 Limit  (cost=713126.57..713126.57 rows=1 width=270) (actual time=5508713.094..5508713.112 rows=100 loops=1)
   ->  Sort  (cost=713126.57..713126.57 rows=1 width=270) (actual time=5508713.090..5508713.100 rows=100 loops=1)
         Sort Key: public.supplier.s_acctbal DESC, public.nation.n_name, public.supplier.s_name, public.part.p_partkey
         Sort Method: top-N heapsort  Memory: 78kB
         ->  Nested Loop  (cost=356286.12..713126.56 rows=1 width=270) (actual time=6322.857..5508657.803 rows=4690 loops=1)
               Join Filter: (subquery."?column?" = public.part.p_partkey)
               Rows Removed by Join Filter: 37444960
               ->  Hash Join  (cost=356286.12..641983.72 rows=1 width=248) (actual time=5613.180..13883.154 rows=4690 loops=1)
                     Hash Cond: ((public.partsupp.ps_partkey = subquery."?column?") AND (public.partsupp.ps_supplycost = subquery.min))
                     ->  Hash Join  (cost=3618.17..288764.19 rows=44126 width=250) (actual time=83.731..7317.840 rows=1602960 loops=1)
                           Hash Cond: (public.partsupp.ps_suppkey = public.supplier.s_suppkey)
                           ->  Seq Scan on partsupp  (cost=0.00..254712.08 rows=7998008 width=14) (actual time=0.032..3033.509 rows=8000000 loops=1)
                           ->  Hash  (cost=3611.14..3611.14 rows=562 width=244) (actual time=83.299..83.299 rows=20037 loops=1)
                                  Buckets: 32768  Batches: 1  Memory Usage: 3957kB
                                 ->  Hash Join  (cost=24.46..3611.14 rows=562 width=244) (actual time=0.780..68.145 rows=20037 loops=1)
                                       Hash Cond: (public.supplier.s_nationkey = public.nation.n_nationkey)
                                       ->  Seq Scan on supplier  (cost=0.00..3221.00 rows=100000 width=144) (actual time=0.022..31.977 rows=100000 loops=1)
                                       ->  Hash  (cost=24.45..24.45 rows=1 width=108) (actual time=0.348..0.348 rows=5 loops=1)
                                              Buckets: 32768  Batches: 1  Memory Usage: 1kB
                                             ->  Hash Join  (cost=12.24..24.45 rows=1 width=108) (actual time=0.314..0.322 rows=5 loops=1)
                                                   Hash Cond: (public.nation.n_regionkey = public.region.r_regionkey)
                                                   ->  Seq Scan on nation  (cost=0.00..11.76 rows=176 width=112) (actual time=0.020..0.024 rows=25 loops=1)
                                                   ->  Hash  (cost=12.22..12.22 rows=1 width=4) (actual time=0.056..0.056 rows=1 loops=1)
                                                          Buckets: 32768  Batches: 1  Memory Usage: 1kB
                                                         ->  Seq Scan on region  (cost=0.00..12.22 rows=1 width=4) (actual time=0.046..0.048 rows=1 loops=1)
                                                               Filter: (r_name = 'ASIA'::bpchar)
                                                               Rows Removed by Filter: 4
                     ->  Hash  (cost=352655.93..352655.93 rows=801 width=36) (actual time=5527.882..5527.882 rows=4690 loops=1)
                            Buckets: 32768  Batches: 1  Memory Usage: 197kB
                           ->  Subquery Scan on subquery  (cost=352639.91..352655.93 rows=801 width=36) (actual time=5519.513..5524.554 rows=4690 loops=1)
                                 ->  HashAggregate  (cost=352639.91..352647.92 rows=801 width=42) (actual time=5519.505..5523.292 rows=4690 loops=1)
                                       Group By Key: public.partsupp.ps_partkey
                                       ->  Hash Join  (cost=74761.00..352635.91 rows=801 width=10) (actual time=1241.987..5507.099 rows=6337 loops=1)
                                             Hash Cond: (public.partsupp.ps_suppkey = public.supplier.s_suppkey)
                                             ->  Hash Semi Join  (cost=71142.84..348465.07 rows=145203 width=14) (actual time=1175.586..5423.387 rows=31940 loops=1)
                                                   Hash Cond: (public.partsupp.ps_partkey = public.part.p_partkey)
                                                   ->  Seq Scan on partsupp  (cost=0.00..254712.08 rows=7998008 width=14) (actual time=0.007..2536.689 rows=8000000 loops=1)
                                                   ->  Hash  (cost=71046.38..71046.38 rows=7717 width=4) (actual time=1175.341..1175.341 rows=7985 loops=1)
                                                          Buckets: 32768  Batches: 1  Memory Usage: 281kB
                                                         ->  Seq Scan on part  (cost=0.00..71046.38 rows=7717 width=4) (actual time=0.204..1170.896 rows=7985 loops=1)
                                                               Filter: (((p_type)::text ~~ '%TIN'::text) AND (p_size = 37))
                                                               Rows Removed by Filter: 1992015
                                             ->  Hash  (cost=3611.14..3611.14 rows=562 width=4) (actual time=65.884..65.884 rows=20037 loops=1)
                                                    Buckets: 32768  Batches: 1  Memory Usage: 705kB
                                                   ->  Hash Join  (cost=24.46..3611.14 rows=562 width=4) (actual time=0.816..57.604 rows=20037 loops=1)
                                                         Hash Cond: (public.supplier.s_nationkey = public.nation.n_nationkey)
                                                         ->  Seq Scan on supplier  (cost=0.00..3221.00 rows=100000 width=8) (actual time=0.018..24.292 rows=100000 loops=1)
                                                         ->  Hash  (cost=24.45..24.45 rows=1 width=4) (actual time=0.391..0.391 rows=5 loops=1)
                                                                Buckets: 32768  Batches: 1  Memory Usage: 1kB
                                                               ->  Hash Join  (cost=12.24..24.45 rows=1 width=4) (actual time=0.351..0.360 rows=5 loops=1)
                                                                     Hash Cond: (public.nation.n_regionkey = public.region.r_regionkey)
                                                                     ->  Seq Scan on nation  (cost=0.00..11.76 rows=176 width=8) (actual time=0.006..0.008 rows=25 loops=1)
                                                                     ->  Hash  (cost=12.22..12.22 rows=1 width=4) (actual time=0.025..0.025 rows=1 loops=1)
                                                                            Buckets: 32768  Batches: 1  Memory Usage: 1kB
                                                                           ->  Seq Scan on region  (cost=0.00..12.22 rows=1 width=4) (actual time=0.017..0.019 rows=1 loops=1)
                                                                                 Filter: (r_name = 'ASIA'::bpchar)
                                                                                 Rows Removed by Filter: 4
               ->  Seq Scan on part  (cost=0.00..71046.38 rows=7717 width=30) (actual time=922.685..5484593.680 rows=37449650 loops=4690)
                     Filter: (((p_type)::text ~~ '%TIN'::text) AND (p_size = 37))
                     Rows Removed by Filter: 9342550350
 Total runtime: 5508715.462 ms
(61 rows)
"""

plan1 = """
 [Bypass]
 Index Scan using t1_c1_idx on t1  (cost=0.00..2.28 rows=1 width=70)
   Index Cond: (c1 = 'asfsdf'::text)
(3 rows)

"""

plan2 = """
 Partition Iterator  (cost=0.00..2.28 rows=1 width=90) (actual time=0.027..0.029 rows=1 loops=1)
   Iterations: 1
   ->  Index Scan using customer_address_ca_address_sk_idx on customer_address  (cost=0.00..2.28 rows=1 width=90) (actual time=0.022..0.023 rows=1 loops=1)
         Index Cond: (ca_address_sk = 100)
         Selected Partitions:  1
 Total runtime: 0.110 ms
(6 rows)

"""
plan3 = """
 Partition Iterator  (cost=0.00..2.28 rows=1 width=90) (actual time=0.027..0.028 rows=1 loops=1)
   Iterations: 1
   ->  Index Scan using customer_address_ca_address_sk_idx on customer_address a  (cost=0.00..2.28 rows=1 width=90) (actual time=0.020..0.021 rows=1 loops=1)
         Index Cond: (ca_address_sk = 100)
         Selected Partitions:  1
 Total runtime: 0.105 ms
(6 rows)


"""
plan4 = """
 Partition Iterator  (cost=0.00..1.27 rows=1 width=4) (actual time=0.023..0.024 rows=1 loops=1)
   Iterations: 1
   ->  Index Only Scan using customer_address_ca_address_sk_idx on customer_address a  (cost=0.00..1.27 rows=1 width=4) (actual time=0.016..0.017 rows=1 loops=1)
         Index Cond: (ca_address_sk = 100)
         Heap Fetches: 0
         Selected Partitions:  1
 Total runtime: 0.121 ms
(7 rows)

"""

plan5 = """
 Partition Iterator  (cost=9.80..28.75 rows=716 width=8) 
   Iterations: PART 
   ->  Bitmap Heap Scan on t1  (cost=9.80..28.75 rows=716 width=8) 
         Recheck Cond: (c1 < $1) 
         Selected Partitions:  PART 
         ->  Bitmap Index Scan on t1_c1_idx  (cost=0.00..9.62 rows=716 width=0) 
               Index Cond: (c1 < $1) 
               Selected Partitions:  PART 
(8 rows)
"""

plan6 = """
 Partition Iterator  (cost=9.80..28.75 rows=716 width=8) 
   Iterations: PART 
   ->  Partitioned Bitmap Heap Scan on t1  (cost=9.80..28.75 rows=716 width=8) 
         Recheck Cond: (c1 < $1) 
         Selected Partitions:  PART 
         ->  Partitioned Bitmap Index Scan on t1_c1_idx  (cost=0.00..9.62 rows=716 width=0) 
               Index Cond: (c1 < $1) 
               Selected Partitions:  PART 
(8 rows)
"""

plan7 = """
 Partition Iterator  (cost=0.00..31.49 rows=2149 width=8) 
   Iterations: 2 
   ->  Partitioned Seq Scan on t1  (cost=0.00..31.49 rows=2149 width=8) 
         Selected Partitions:  1..2 
(4 rows)
"""

plan8 = """
 Seq Scan on public.t1  (cost=0.00..221163456.00 rows=4550 width=33)
   Output: t1.c1
   Filter: (t1.c1 = (SubPlan 1))
   SubPlan 1
     ->  Seq Scan on public.t2 a  (cost=0.00..243.01 rows=1 width=33)
           Output: t2.c1
           Filter: (t1.c1 = t2.c1)
(7 rows)

    """

plan9 = """
 Seq Scan on public.t1 a  (cost=0.00..221163456.00 rows=4550 width=33)
   Output: a.c1
   Filter: (a.c1 = (SubPlan 1))
   SubPlan 1
     ->  Seq Scan on public.t2 b  (cost=0.00..243.01 rows=1 width=33)
           Output: b.c1
           Filter: (a.c1 = b.c1)
(7 rows)

    
    """

plan10 = """
 Row Adapter  (cost=13.76..13.76 rows=5 width=46)
   ->  CStore Scan on my_table  (cost=0.00..13.76 rows=5 width=46)
         Filter: ((product_name)::text = 'asdas'::text)
(3 rows)

"""

plan11 = """
 Row Adapter  (cost=13.76..13.76 rows=5 width=46)
   ->  CStore Scan on my_table1 aa  (cost=0.00..13.76 rows=5 width=46)
         Filter: ((product_name)::text = 'asdas'::text)
   ->  CStore Scan on public.my_table2 aa  (cost=0.00..13.76 rows=5 width=46)
         Filter: ((product_name)::text = 'asdas'::text)
(3 rows)

"""

plan12 = """
 Row Adapter  (cost=13.76..13.76 rows=5 width=46)
   ->  Vector Partition Iterator  (cost=0.00..13.76 rows=5 width=46)
         Iterations: 4
         ->  Partitioned CStore Scan on public.my_table aa  (cost=0.00..13.76 rows=5 width=46)
               Filter: ((product_name)::text = 'asdas'::text)
               Selected Partitions:  1..4
(6 rows)


"""


def test_plan_parser1():
    plan_parser = plan_parsing.Plan()
    plan_parser.parse(plan0)
    assert plan_parser.height == 14
    assert [item.name for item in plan_parser.sorted_operators] == ['Sort', 'Hash Join', 'Hash Join', 'Hash Join',
                                                                    'Hash Semi Join', 'Seq Scan on partsupp',
                                                                    'Seq Scan on partsupp', 'Seq Scan on part',
                                                                    'Seq Scan on part', 'Hash Join', 'Hash Join',
                                                                    'Seq Scan on supplier', 'Seq Scan on supplier',
                                                                    'Subquery Scan on subquery', 'Seq Scan on region',
                                                                    'Seq Scan on region', 'Hash Join', 'Hash Join',
                                                                    'Seq Scan on nation', 'Seq Scan on nation',
                                                                    'HashAggregate', 'Limit', 'Nested Loop', 'Hash',
                                                                    'Hash', 'Hash', 'Hash', 'Hash', 'Hash', 'Hash',
                                                                    'Hash']
    assert [item.name for item in plan_parser.find_operators("Sort")] == ['Sort']
    assert [item.name for item in plan_parser.find_operators("Nested Loop", accurate=True)] == ['Nested Loop']
    assert [item.name for item in plan_parser.find_operators("Seq Scan")] == ['Seq Scan on partsupp',
                                                                              'Seq Scan on supplier',
                                                                              'Seq Scan on nation',
                                                                              'Seq Scan on region',
                                                                              'Seq Scan on partsupp',
                                                                              'Seq Scan on part',
                                                                              'Seq Scan on supplier',
                                                                              'Seq Scan on nation',
                                                                              'Seq Scan on region', 'Seq Scan on part']
    assert [item.properties for item in plan_parser.find_operators("Seq Scan")] == [{}, {}, {},
                                                    {
                                                        'Filter': "r_name = 'ASIA'::bpchar",
                                                        'Rows Removed by Filter': '4'},
                                                    {}, {
                                                        'Filter': "p_type)::text ~~ '%TIN'::text) AND (p_size = 37",
                                                        'Rows Removed by Filter': '1992015'},
                                                    {}, {}, {
                                                        'Filter': "r_name = 'ASIA'::bpchar",
                                                        'Rows Removed by Filter': '4'},
                                                    {
                                                        'Filter': "p_type)::text ~~ '%TIN'::text) AND (p_size = 37",
                                                        'Rows Removed by Filter': '9342550350',
                                                        'Total runtime': '5508715.462 ms'}]
    assert [item.name for item in plan_parser.find_properties("public.nation.n_regionkey")] == ['Hash Join',
                                                                                                'Hash Join']
    assert str(plan_parser).strip()


def test_plan_parser2():
    plan_parser = plan_parsing.Plan()
    plan_parser.parse(plan1)
    node = plan_parser.find_operators("Index Scan", accurate=False)
    assert node[0].table == 't1'
    assert node[0].index == 't1_c1_idx'

    plan_parser.parse(plan2)
    node = plan_parser.find_operators("Index Scan", accurate=False)
    assert node[0].table == 'customer_address'
    assert node[0].index == 'customer_address_ca_address_sk_idx'

    plan_parser.parse(plan3)
    node = plan_parser.find_operators("Index Scan", accurate=False)
    assert node[0].table == 'customer_address'
    assert node[0].index == 'customer_address_ca_address_sk_idx'

    plan_parser.parse(plan4)
    node = plan_parser.find_operators("Index Scan", accurate=False)
    assert node[0].table == 'customer_address'
    assert node[0].index == 'customer_address_ca_address_sk_idx'

    plan_parser.parse(plan5)
    node1 = plan_parser.find_operators("Bitmap Index Scan", accurate=False)
    node2 = plan_parser.find_operators("Bitmap Heap Scan", accurate=False)
    assert node1[0].index == 't1_c1_idx'
    assert node2[0].table == 't1'

    plan_parser.parse(plan6)
    node1 = plan_parser.find_operators("Partitioned Bitmap Heap Scan", accurate=False)
    node2 = plan_parser.find_operators("Bitmap Heap Scan", accurate=False)
    assert node1[0].table == 't1'
    assert node2[0].table == 't1'
    node3 = plan_parser.find_operators("Partitioned Bitmap Index Scan on t1_c1_idx", accurate=False)
    node4 = plan_parser.find_operators("Index Scan", accurate=False)
    assert node3[0].index == 't1_c1_idx'
    assert node4[0].index == 't1_c1_idx'

    plan_parser.parse(plan7)
    node1 = plan_parser.find_operators("Partitioned Seq Scan", accurate=False)
    node2 = plan_parser.find_operators("Seq Scan", accurate=False)
    assert node1[0].table == 't1'
    assert node2[0].table == 't1'

    plan_parser.parse(plan8)
    node = plan_parser.find_operators("Seq Scan", accurate=False)
    assert len(node) == 2
    assert sorted([node[0].table, node[1].table]) == sorted(['t1', 't2'])

    plan_parser.parse(plan9)
    node = plan_parser.find_operators("Seq Scan", accurate=False)
    assert len(node) == 2
    assert sorted([node[0].table, node[1].table]) == sorted(['t1', 't2'])

    plan_parser.parse(plan10)
    node = plan_parser.find_operators("CStore Scan", accurate=False)
    assert node[0].table == 'my_table'

    plan_parser.parse(plan11)
    node = plan_parser.find_operators("CStore Scan", accurate=False)
    assert len(node) == 2
    assert sorted([node[0].table, node[1].table]) == sorted(['my_table1', 'my_table2'])

    plan_parser.parse(plan12)
    node = plan_parser.find_operators("Partitioned CStore Scan", accurate=False)
    assert node[0].table == 'my_table'
