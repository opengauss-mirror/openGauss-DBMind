#!/usr/bin/env python3
# coding=utf-8
"""
Copyright (c) 2021 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.

This file contains test cases for parsing function of execution plan.

"""

CASE1 = """
[Bypass]
Limit  (cost=0.00..0.07 rows=5 width=8)
  ->  Seq Scan on atest12  (cost=0.00..145.00 rows=10000 width=8)
    """
CASE2 = """
Sort  (cost=198.32..198.57 rows=100 width=8)
  Sort Key: a
  ->  Seq Scan on atest12  (cost=0.00..195.00 rows=100 width=8)
        Filter: ((a > 10) AND (b < 100))
    """
CASE3 = """
Index Scan using atest12_a_idx on atest12  (cost=0.00..357.18 rows=9991 width=8)
  Filter: ((a > 10) OR ((b < 100) AND ((a / b) > 1::double precision)))
   """
CASE4 = """
Limit  (cost=71209.47..71209.47 rows=1 width=270)
  ->  Sort  (cost=71209.47..71209.47 rows=1 width=270)
        Sort Key: public.supplier.s_acctbal DESC, public.nation.n_name, public.supplier.s_name, public.part.p_partkey
        ->  Nested Loop  (cost=35527.11..71209.46 rows=1 width=270)
              Join Filter: (subquery."?column?" = public.part.p_partkey)
              ->  Hash Join  (cost=35527.11..64092.62 rows=1 width=248)
                    Hash Cond: ((public.partsupp.ps_partkey = subquery."?column?") AND (public.partsupp.ps_supplycost = subquery.min))
                    ->  Hash Join  (cost=384.73..28916.53 rows=4494 width=250)
                          Hash Cond: (public.partsupp.ps_suppkey = public.supplier.s_suppkey)
                          ->  Seq Scan on partsupp  (cost=0.00..25487.00 rows=800000 width=14)
                          ->  Hash  (cost=384.03..384.03 rows=56 width=244)
                                ->  Hash Join  (cost=24.46..384.03 rows=56 width=244)
                                      Hash Cond: (public.supplier.s_nationkey = public.nation.n_nationkey)
                                      ->  Seq Scan on supplier  (cost=0.00..323.00 rows=10000 width=144)
                                      ->  Hash  (cost=24.45..24.45 rows=1 width=108)
                                            ->  Hash Join  (cost=12.24..24.45 rows=1 width=108)
                                                  Hash Cond: (public.nation.n_regionkey = public.region.r_regionkey)
                                                  ->  Seq Scan on nation  (cost=0.00..11.76 rows=176 width=112)
                                                  ->  Hash  (cost=12.22..12.22 rows=1 width=4)
                                                        ->  Seq Scan on region  (cost=0.00..12.22 rows=1 width=4)
                                                              Filter: (r_name = 'ASIA'::bpchar)
                    ->  Hash  (cost=35142.08..35142.08 rows=20 width=36)
                          ->  Subquery Scan on subquery  (cost=35141.68..35142.08 rows=20 width=36)
                                ->  HashAggregate  (cost=35141.68..35141.88 rows=20 width=42)
                                      Group By Key: public.partsupp.ps_partkey
                                      ->  Hash Join  (cost=7501.57..35141.58 rows=20 width=10)
                                            Hash Cond: (public.partsupp.ps_suppkey = public.supplier.s_suppkey)
                                            ->  Hash Semi Join  (cost=7116.84..34743.33 rows=3550 width=14)
                                                  Hash Cond: (public.partsupp.ps_partkey = public.part.p_partkey)
                                                  ->  Seq Scan on partsupp  (cost=0.00..25487.00 rows=800000 width=14)
                                                  ->  Hash  (cost=7106.00..7106.00 rows=867 width=4)
                                                        ->  Seq Scan on part  (cost=0.00..7106.00 rows=867 width=4)
                                                              Filter: (((p_type)::text ~~ '%TIN'::text) AND (p_size = 37))
                                            ->  Hash  (cost=384.03..384.03 rows=56 width=4)
                                                  ->  Hash Join  (cost=24.46..384.03 rows=56 width=4)
                                                        Hash Cond: (public.supplier.s_nationkey = public.nation.n_nationkey)
                                                        ->  Seq Scan on supplier  (cost=0.00..323.00 rows=10000 width=8)
                                                        ->  Hash  (cost=24.45..24.45 rows=1 width=4)
                                                              ->  Hash Join  (cost=12.24..24.45 rows=1 width=4)
                                                                    Hash Cond: (public.nation.n_regionkey = public.region.r_regionkey)
                                                                    ->  Seq Scan on nation  (cost=0.00..11.76 rows=176 width=8)
                                                                    ->  Hash  (cost=12.22..12.22 rows=1 width=4)
                                                                          ->  Seq Scan on region  (cost=0.00..12.22 rows=1 width=4)
                                                                                Filter: (r_name = 'ASIA'::bpchar)
              ->  Seq Scan on part  (cost=0.00..7106.00 rows=867 width=30)
                    Filter: (((p_type)::text ~~ '%TIN'::text) AND (p_size = 37))
   """

CASE5 = """
Limit  (cost=71180.13..71180.13 rows=1 width=270)
  ->  Sort  (cost=71180.13..71180.13 rows=1 width=270)
        Sort Key: public.supplier.s_acctbal DESC, public.nation.n_name, public.supplier.s_name, public.part.p_partkey
        ->  Hash Join  (cost=42643.94..71180.12 rows=1 width=270)
              Hash Cond: ((public.part.p_partkey = subquery."?column?") AND (public.partsupp.ps_supplycost = subquery.min))
              ->  Hash Join  (cost=7501.57..36037.58 rows=20 width=280)
                    Hash Cond: (public.partsupp.ps_suppkey = public.supplier.s_suppkey)
                    ->  Hash Join  (cost=7116.84..35639.34 rows=3550 width=44)
                          Hash Cond: (public.partsupp.ps_partkey = public.part.p_partkey)
                          ->  Seq Scan on partsupp  (cost=0.00..25487.00 rows=800000 width=14)
                          ->  Hash  (cost=7106.00..7106.00 rows=867 width=30)
                                ->  Seq Scan on part  (cost=0.00..7106.00 rows=867 width=30)
                                      Filter: (((p_type)::text ~~ '%TIN'::text) AND (p_size = 37))
                    ->  Hash  (cost=384.03..384.03 rows=56 width=244)
                          ->  Hash Join  (cost=24.46..384.03 rows=56 width=244)
                                Hash Cond: (public.supplier.s_nationkey = public.nation.n_nationkey)
                                ->  Seq Scan on supplier  (cost=0.00..323.00 rows=10000 width=144)
                                ->  Hash  (cost=24.45..24.45 rows=1 width=108)
                                      ->  Hash Join  (cost=12.24..24.45 rows=1 width=108)
                                            Hash Cond: (public.nation.n_regionkey = public.region.r_regionkey)
                                            ->  Seq Scan on nation  (cost=0.00..11.76 rows=176 width=112)
                                            ->  Hash  (cost=12.22..12.22 rows=1 width=4)
                                                  ->  Seq Scan on region  (cost=0.00..12.22 rows=1 width=4)
                                                        Filter: (r_name = 'ASIA'::bpchar)
              ->  Hash  (cost=35142.08..35142.08 rows=20 width=36)
                    ->  Subquery Scan on subquery  (cost=35141.68..35142.08 rows=20 width=36)
                          ->  HashAggregate  (cost=35141.68..35141.88 rows=20 width=42)
                                Group By Key: public.partsupp.ps_partkey
                                ->  Hash Join  (cost=7501.57..35141.58 rows=20 width=10)
                                      Hash Cond: (public.partsupp.ps_suppkey = public.supplier.s_suppkey)
                                      ->  Hash Semi Join  (cost=7116.84..34743.33 rows=3550 width=14)
                                            Hash Cond: (public.partsupp.ps_partkey = public.part.p_partkey)
                                            ->  Seq Scan on partsupp  (cost=0.00..25487.00 rows=800000 width=14)
                                            ->  Hash  (cost=7106.00..7106.00 rows=867 width=4)
                                                  ->  Seq Scan on part  (cost=0.00..7106.00 rows=867 width=4)
                                                        Filter: (((p_type)::text ~~ '%TIN'::text) AND (p_size = 37))
                                      ->  Hash  (cost=384.03..384.03 rows=56 width=4)
                                            ->  Hash Join  (cost=24.46..384.03 rows=56 width=4)
                                                  Hash Cond: (public.supplier.s_nationkey = public.nation.n_nationkey)
                                                  ->  Seq Scan on supplier  (cost=0.00..323.00 rows=10000 width=8)
                                                  ->  Hash  (cost=24.45..24.45 rows=1 width=4)
                                                        ->  Hash Join  (cost=12.24..24.45 rows=1 width=4)
                                                              Hash Cond: (public.nation.n_regionkey = public.region.r_regionkey)
                                                              ->  Seq Scan on nation  (cost=0.00..11.76 rows=176 width=8)
                                                              ->  Hash  (cost=12.22..12.22 rows=1 width=4)
                                                                    ->  Seq Scan on region  (cost=0.00..12.22 rows=1 width=4)
                                                                          Filter: (r_name = 'ASIA'::bpchar)
   """
