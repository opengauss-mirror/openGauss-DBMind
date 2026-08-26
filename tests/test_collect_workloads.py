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
from dbmind.components.fetch_statement import collect_workloads


def test_collect_statement_from_asp():
    databases = "db1, db2"
    start_time = "2023-08-01 10:00:00"
    end_time = "2023-08-02 10:00:00"
    db_users = "user1, user2"
    sql_types = "select, insert, update, delete"
    stmts = collect_workloads.collect_statement_from_asp(databases, start_time, end_time, db_users, sql_types)
    assert [item.strip() for item in stmts.split()] == [
        item.strip() for item in """
        SELECT distinct S.user_name, D.datname, G.query_id, S.unique_sql_id, S.n_calls,
        S.min_elapse_time, S.max_elapse_time, S.n_returned_rows / (S.n_calls + 1) as
        avg_returned_rows, S.n_tuples_fetched / (S.n_calls + 1) as avg_tuples_fetched,
        S.n_tuples_returned / (S.n_calls + 1) as avg_tuples_returned,
        S.n_tuples_inserted / (S.n_calls + 1) as avg_tuples_inserted,
        S.n_tuples_updated / (S.n_calls + 1) as avg_tuples_updated,
        S.n_tuples_deleted / (S.n_calls + 1) as avg_tuples_deleted,
        S.n_soft_parse, S.n_hard_parse, S.db_time / (S.n_calls + 1) as avg_db_time,
        S.cpu_time / (S.n_calls + 1) as avg_cpu_time, S.parse_time / (S.n_calls + 1) as
        avg_parse_time, S.plan_time / (S.n_calls + 1) as avg_plan_time,
        S.data_io_time / (S.n_calls + 1) as avg_data_io_time, S.sort_spill_count,
        S.hash_spill_count,
        query
        FROM dbe_perf.statement S INNER JOIN pg_catalog.gs_asp G ON G.unique_query_id = S.unique_sql_id
        INNER JOIN pg_catalog.pg_database D ON G.databaseid = D.oid where
        G.sample_time <= '2023-08-02 10:00:00' and G.sample_time >= '2023-08-01 10:00:00' and
        D.datname in ('db1','db2') and pg_catalog.upper(pg_catalog.split_part(trim(query), ' ', 1)) in
        ('select','insert','update','delete') and S.user_name in
        ('user1','user2') limit 1000;""".split()
    ]


def test_collect_statement_from_activity():
    databases = "db1, db2"
    db_users = "user1, user2"
    sql_types = "select, insert, update, delete"
    stmts = collect_workloads.collect_statement_from_activity(databases, db_users, sql_types)
    assert [item.strip() for item in stmts.split()] == [
        item.strip() for item in """
    SELECT usename, datname, application_name, sessionid, query_id, unique_sql_id, 
    extract(epoch from pg_catalog.now() - query_start)
    as duration,
    query FROM pg_catalog.pg_stat_activity
    WHERE state != 'idle' and application_name not in ('DBMind-openGauss-exporter', 'DBMind-Service') 
    and query_id != 0 and duration >= 60
 and datname in ('db1','db2') and usename in ('user1','user2') and pg_catalog.upper(pg_catalog.split_part(trim(query), ' ', 1)) 
 in ('select','insert','update','delete') limit 1000;""".split()
    ]


def test_collect_statement_from_statement_history():
    databases = "db1, db2"
    start_time = "2023-08-01 10:00:00"
    end_time = "2023-08-02 10:00:00"
    db_users = "user1, user2"
    schemas = "schema1,schema2"
    sql_types = "select, insert, update, delete"
    stmts = collect_workloads.collect_statement_from_statement_history(databases,
                                                                       schemas,
                                                                       start_time,
                                                                       end_time,
                                                                       db_users,
                                                                       sql_types,
                                                                       template_id=None)
    assert [item.strip() for item in stmts.split()] == [
        item.strip() for item in """
    select user_name, db_name, schema_name, application_name, debug_query_id as query_id, unique_query_id,
    start_time, finish_time, extract(epoch from finish_time - start_time) as duration,
    n_returned_rows, n_tuples_fetched, n_tuples_returned, n_tuples_inserted, n_tuples_updated,
    n_tuples_deleted, n_blocks_fetched, n_blocks_hit, n_soft_parse, n_hard_parse, db_time,
    cpu_time, parse_time, plan_time, data_io_time, lock_wait_time, lwlock_wait_time,
    case when (client_addr is null) then '127.0.0.1' else client_addr end as client_addr,
    query,
    query_plan from dbe_perf.statement_history
    where application_name not in ('DBMind-openGauss-exporter', 'DBMind-Service') and duration >= 60
     and start_time >= '2023-08-01 10:00:00' and finish_time <= '2023-08-02 10:00:00' and db_name in ('db1','db2') 
     and ((pg_catalog.string_to_array(pg_catalog.replace(schema_name, ' ', ''), ',') && ARRAY['schema1','schema2'])
        or (pg_catalog.regexp_like(schema_name,'"\$user"') and ARRAY[user_name::TEXT] && ARRAY['schema1','schema2']))  
        and pg_catalog.upper(pg_catalog.split_part(trim(query), ' ', 1)) in ('select','insert','update','delete') and user_name 
        in ('user1','user2') limit 1000;""".split()
    ]
