# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
import configparser
import datetime
import logging
import os
import subprocess
import time
from victim_models.ICDE_2021.psql import PostgreSQL as pg
from collections import defaultdict
import copy

import constants
from database.query_plan import QueryPlan
from database.column import Column
from database.table import Table
import json
import sys

db_config = configparser.ConfigParser()
db_config.read(constants.ROOT_DIR + constants.DB_CONFIG)
db_type = db_config['SYSTEM']['db_type']
database = db_config[db_type]['database']

table_scan_times_hyp = copy.deepcopy(constants.TABLE_SCAN_TIMES[database[:-4]])
table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES[database[:-4]])

tables_global = None
pk_columns_dict = {}


def create_index_v1(connection, schema_name, tbl_name, col_names, idx_name, include_cols=()):
    """
    Create an index on the given table

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param tbl_name: name of the database table
    :param col_names: string list of column names
    :param idx_name: name of the index
    :param include_cols: columns that needed to added as includes
    """
    if include_cols:
        query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)})" \
            f" INCLUDE ({', '.join(include_cols)})"
    else:
        query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)})"
    cursor = connection.cursor()
    cursor.execute("SET STATISTICS XML ON")
    cursor.execute(query)
    stat_xml = cursor.fetchone()[0]
    cursor.execute("SET STATISTICS XML OFF")
    logging.info(f"Added: {idx_name}")

    # Return the current reward
    query_plan = QueryPlan(stat_xml)
    if constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_ELAPSED_TIME:
        return float(query_plan.elapsed_time)
    elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_CPU_TIME:
        return float(query_plan.cpu_time)
    elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_SUB_TREE_COST:
        return float(query_plan.est_statement_sub_tree_cost)
    else:
        return float(query_plan.est_statement_sub_tree_cost)


"""Below 2 functions are used by DTARunner"""


def create_index_v2(connection, query):
    """
    Create an index on the given table

    :param connection: sql_connection
    :param query: query for index creation
    """
    cursor = connection.cursor()
    cursor.execute("SET STATISTICS XML ON")
    cursor.execute(query)
    stat_xml = cursor.fetchone()[0]
    cursor.execute("SET STATISTICS XML OFF")

    # Return the current reward
    query_plan = QueryPlan(stat_xml)
    if constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_ELAPSED_TIME:
        return float(query_plan.elapsed_time)
    elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_CPU_TIME:
        return float(query_plan.cpu_time)
    elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_SUB_TREE_COST:
        return float(query_plan.est_statement_sub_tree_cost)
    else:
        return float(query_plan.est_statement_sub_tree_cost)


def create_statistics(connection, query):
    """
    Create an index on the given table

    :param connection: sql_connection
    :param query: query for index creation
    """
    cursor = connection.cursor()
    start_time_execute = datetime.datetime.now()
    cursor.execute(query)
    end_time_execute = datetime.datetime.now()
    time_apply = (end_time_execute - start_time_execute).total_seconds()

    # Return the current reward
    return time_apply


def bulk_create_indexes_v1(connection, bandit_arm_list, quries):
    """
    This uses create_index method to create multiple indexes at once. This is used when a super arm is pulled

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: list of BanditArm objects
    :return: cost (regret)
    """
    create_cost = {}
    for index_name, bandit_arm in bandit_arm_list.items():
        pg_client = pg.PGHypo(connection)
        strs = ""
        for col in bandit_arm.index_cols:
            strs = strs + ', ' + col
        strs = strs[2::]
        create_cost[index_name] = pg_client.execute_create_hypo(bandit_arm.table_name + "#" + strs, bandit_arm)
        cost = 0
        for query in quries:
            cost += execute_query_v1(connection, query.query_string)
        bandit_arm.execute_time = cost
        pg_client.execute_delete_hypo(bandit_arm.index_oid)
        '''cost[index_name] = create_index_v1(connection, schema_name, bandit_arm.table_name, bandit_arm.index_cols, bandit_arm.index_name,
                                           bandit_arm.include_cols)'''
    return create_cost


def bulk_create_indexes_v2(connection, bandit_arm_list):
    """
    This uses create_index method to create multiple indexes at once. This is used when a super arm is pulled

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: list of BanditArm objects
    :return: cost (regret)
    """
    for index_name, bandit_arm in bandit_arm_list.items():
        pg_client = pg.PGHypo(connection)
        strs = ""
        for col in bandit_arm.index_cols:
            strs = strs + ', ' + col
        strs = strs[2::]
        pg_client.execute_create_hypo(bandit_arm.table_name + "#" + strs, bandit_arm)
        set_arm_size(connection, bandit_arm)

def drop_index(connection, schema_name, tbl_name, idx_name):
    """
    Drops the index on the given table with given name

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param tbl_name: name of the database table
    :param idx_name: name of the index
    :return:
    """
    query = f"DROP INDEX {schema_name}.{tbl_name}.{idx_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    logging.info(f"removed: {idx_name}")
    logging.debug(query)


def bulk_drop_index(connection, bandit_arm_list):
    """
    Drops the index for all given bandit arms

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: list of bandit arms
    :return:
    """
    for index_name, bandit_arm in bandit_arm_list.items():
        pg_client = pg.PGHypo(connection)
        pg_client.execute_delete_hypo(bandit_arm.index_oid)
        # drop_index(connection, schema_name, bandit_arm.table_name, bandit_arm.index_name)

def simple_execute(connection, query):
    """
    Drops the index on the given table with given name

    :param connection: sql_connection
    :param query: query to execute
    :return:
    """
    cursor = connection.cursor()
    cursor.execute(query)
    logging.debug(query)


def execute_query_v1(connection, query):
    """
    This executes the given query and return the time took to run the query. This Clears the cache and executes
    the query and return the time taken to run the query. This return the 'elapsed time' by default.
    However its possible to get the cpu time by setting the is_cpu_time to True

    :param connection: sql_connection
    :param query: query that need to be executed
    :return: time taken for the query
    """

    pg_client = pg.PGHypo(connection)
    cost = pg_client.get_queries_cost([query,])
    return cost[0]

def get_table_row_count(connection, tbl_name):
    name = "'" + str.lower(tbl_name) + "_pkey'"
    row_query = "select reltuples from pg_class where relname = " + name
    cursor = connection.cursor()
    cursor.execute(row_query)
    row_count = cursor.fetchone()[0]
    return row_count

def get_init_query_cost(connection, bandit_arm_list, queries):
    bulk_drop_index(connection, bandit_arm_list)
    execute_cost = 0
    for query in queries:
        execute_cost += execute_query_v1(connection, query.query_string)
    bulk_create_indexes_v2(connection, bandit_arm_list)
    return execute_cost

def create_query_drop_v3(connection, bandit_arm_list, arm_list_to_add, arm_list_to_delete, queries, init_time):
    """
    This method aggregate few functions of the sql helper class.
        1. This method create the indexes related to the given bandit arms
        2. Execute all the queries in the given list
        3. Clean (drop) the created indexes
        4. Finally returns the time taken to run all the queries

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: arms considered in this round
    :param arm_list_to_add: arms that need to be added in this round
    :param arm_list_to_delete: arms that need to be removed in this round
    :param queries: queries that should be executed
    :return:
    """
    logging.info("arm number = " + str(len(bandit_arm_list)) + ":" + str(len(arm_list_to_add)) + ":" + str(len(arm_list_to_delete)))
    bulk_drop_index(connection, arm_list_to_delete)
    creation_cost = bulk_create_indexes_v1(connection, arm_list_to_add, queries)
    bulk_create_indexes_v2(connection, arm_list_to_add)
    for index_name, arm in arm_list_to_add.items():
        logging.info("oid for index " + str(index_name) + " = " + str(arm.index_oid))
    arm_rewards = {}
    if tables_global is None:
        get_tables(connection)
    execute_cost_end = 0
    for query in queries:
        time = execute_query_v1(connection, query.query_string)
        logging.info(f"Query {query.id} cost: {time}")
        execute_cost_end += time
    for index_name, arm in bandit_arm_list.items():
        # arm_rewards[index_name] = [(init_time - bandit_arm_list[index_name].execute_time)/bandit_arm_list[index_name].execute_time, 0]
        arm_rewards[index_name] = [
            (init_time - bandit_arm_list[index_name].execute_time), 0]
    for key in creation_cost:
        if key in arm_rewards:
            arm_rewards[key][1] += 0
        else:
            arm_rewards[key][1] = 0
    logging.info(f"Index creation cost: {sum(creation_cost.values())}")
    logging.info(f"Time taken to run the queries: {execute_cost_end}")
    return execute_cost_end, creation_cost, arm_rewards


def merge_index_use(index_uses):
    d = defaultdict(list)
    for index_use in index_uses:
        if index_use[0] not in d:
            d[index_use[0]] = [0] * (len(index_use) - 1)
        d[index_use[0]] = [sum(x) for x in zip(d[index_use[0]], index_use[1:])]
    return [tuple([x]+y) for x, y in d.items()]


def get_selectivity_list(query_obj_list):
    selectivity_list = []
    for query_obj in query_obj_list:
        selectivity_list.append(query_obj.selectivity)
    return selectivity_list


def hyp_create_index_v1(connection, schema_name, tbl_name, col_names, idx_name, include_cols=()):
    """
    Create an hypothetical index on the given table

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param tbl_name: name of the database table
    :param col_names: string list of column names
    :param idx_name: name of the index
    :param include_cols: columns that needed to be added as includes
    """
    if include_cols:
        query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)}) " \
                f"INCLUDE ({', '.join(include_cols)}) WITH STATISTICS_ONLY = -1"
    else:
        query = f"CREATE NONCLUSTERED INDEX {idx_name} ON {schema_name}.{tbl_name} ({', '.join(col_names)}) " \
                f"WITH STATISTICS_ONLY = -1"
    cursor = connection.cursor()
    cursor.execute(query)
    logging.debug(query)
    logging.info(f"Added HYP: {idx_name}")
    return 0


def hyp_bulk_create_indexes(connection, schema_name, bandit_arm_list):
    """
    This uses hyp_create_index method to create multiple indexes at once. This is used when a super arm is pulled

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: list of BanditArm objects
    :return: index name list
    """
    cost = {}
    for index_name, bandit_arm in bandit_arm_list.items():
        pg_client = pg.PGHypo(connection)
        oid = pg_client.execute_create_hypo(bandit_arm.include_cols)
        bandit_arm.index_oid = oid
        '''cost[index_name] = hyp_create_index_v1(connection, schema_name, bandit_arm.table_name, bandit_arm.index_cols,
                                               bandit_arm.index_name, bandit_arm.include_cols)'''
    return cost
        
        
def hyp_enable_index(connection):
    """
    This enables the hypothetical indexes for the given connection. This will be enabled for a given connection and all
    hypothetical queries must be executed via the same connection
    :param connection: connection for which hypothetical indexes will be enabled
    """
    query = f'''SELECT dbid = Db_id(),
                    objectid = object_id,
                    indid = index_id
                FROM   sys.indexes
                WHERE  is_hypothetical = 1;'''
    cursor = connection.cursor()
    cursor.execute(query)
    result_rows = cursor.fetchall()
    for result_row in result_rows:
        query_2 = f"DBCC AUTOPILOT(0, {result_row[0]}, {result_row[1]}, {result_row[2]})"
        cursor.execute(query_2)


def hyp_execute_query(connection, query):
    """
    This hypothetically executes the given query and return the estimated sub tree cost. If required we can add the
    operation cost as well. However, most of the cases operation cost at the top level is 0.

    :param connection: sql_connection
    :param query: query that need to be executed
    :return: estimated sub tree cost
    """
    hyp_enable_index(connection)
    cursor = connection.cursor()
    cursor.execute("SET AUTOPILOT ON")
    cursor.execute(query)
    stat_xml = cursor.fetchone()[0]
    cursor.execute("SET AUTOPILOT OFF")
    query_plan = QueryPlan(stat_xml)
    return float(query_plan.est_statement_sub_tree_cost), query_plan.non_clustered_index_usage, query_plan.clustered_index_usage


def hyp_create_query_drop_v1(connection, schema_name, bandit_arm_list, arm_list_to_add, arm_list_to_delete, queries):
    """
    This method aggregate few functions of the sql helper class.
        1. This method create the hypothetical indexes related to the given bandit arms
        2. Execute all the queries in the given list
        3. Clean (drop) the created hypothetical indexes
        4. Finally returns the sum of estimated sub tree cost for all queries

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: contains the information related to indexes that is considered in this round
    :param arm_list_to_add: arms that need to be added in this round
    :param arm_list_to_delete: arms that need to be removed in this round
    :param queries: queries obj list that should be executed
    :return: sum of estimated sub tree cost for all queries
    """
    bulk_drop_index(connection, arm_list_to_delete)
    creation_cost = hyp_bulk_create_indexes(connection, schema_name, arm_list_to_add)
    estimated_sub_tree_cost = 0
    arm_rewards = {}
    for query in queries:
        cost, index_seeks, clustered_index_scans = hyp_execute_query(connection, query.query_string)
        estimated_sub_tree_cost += float(cost)
        if clustered_index_scans:
            for index_scan in clustered_index_scans:
                if len(table_scan_times_hyp[index_scan[0]]) < constants.TABLE_SCAN_TIME_LENGTH:
                    table_scan_times_hyp[index_scan[0]].append(index_scan[3])

        if index_seeks:
            for index_seek in index_seeks:
                table_scan_time_hyp = table_scan_times_hyp[bandit_arm_list[index_seek[0]].table_name]
                arm_rewards[index_seek[0]] = max(table_scan_time_hyp) - index_seek[3]

    for key in creation_cost:
        creation_cost[key] = max(table_scan_times_hyp[bandit_arm_list[key].table_name])
        if key in arm_rewards:
            arm_rewards[key] += -1 * creation_cost[key]
        else:
            arm_rewards[key] = -1 * creation_cost[key]
    logging.info(f"Time taken to run the queries: {estimated_sub_tree_cost}")
    return estimated_sub_tree_cost, creation_cost, arm_rewards


def hyp_create_query_drop_v2(connection, schema_name, bandit_arm_list, arm_list_to_add, arm_list_to_delete, queries):
    """
    This method aggregate few functions of the sql helper class.
        1. This method create the indexes related to the given bandit arms ok
        2. Execute all the queries in the given list
        3. Clean (drop) the created indexes
        4. Finally returns the time taken to run all the queries

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: arms considered in this round
    :param arm_list_to_add: arms that need to be added in this round
    :param arm_list_to_delete: arms that need to be removed in this round
    :param queries: queries that should be executed
    :return:
    """
    bulk_drop_index(connection, arm_list_to_delete)
    creation_cost = hyp_bulk_create_indexes(connection, schema_name, arm_list_to_add)
    execute_cost = 0
    arm_rewards = {}
    if tables_global is None:
        get_tables(connection)
    for query in queries:
        time, non_clustered_index_usage, clustered_index_usage = hyp_execute_query(connection, query.query_string)
        execute_cost += time
        if clustered_index_usage:
            for index_scan in clustered_index_usage:
                table_name = index_scan[0]
                if len(query.table_scan_times_hyp[table_name]) < constants.TABLE_SCAN_TIME_LENGTH:
                    query.table_scan_times_hyp[table_name].append(index_scan[constants.COST_TYPE_SUB_TREE_COST])
                    table_scan_times_hyp[table_name].append(index_scan[constants.COST_TYPE_SUB_TREE_COST])
        if non_clustered_index_usage:
            for index_use in non_clustered_index_usage:
                index_name = index_use[0]
                table_name = bandit_arm_list[index_name].table_name
                if len(query.table_scan_times_hyp[table_name]) < constants.TABLE_SCAN_TIME_LENGTH:
                    query.index_scan_times_hyp[table_name].append(index_use[constants.COST_TYPE_SUB_TREE_COST])
                table_scan_time = query.table_scan_times_hyp[table_name]
                if len(table_scan_time) > 0:
                    temp_reward = max(table_scan_time) - index_use[constants.COST_TYPE_SUB_TREE_COST]
                elif len(table_scan_times_hyp[table_name]) > 0:
                    temp_reward = max(table_scan_times_hyp[table_name]) - index_use[constants.COST_TYPE_SUB_TREE_COST]
                else:
                    logging.error(f"Queries without index scan information {query.id}")
                    raise Exception
                if index_name not in arm_rewards:
                    arm_rewards[index_name] = [temp_reward, 0]
                else:
                    arm_rewards[index_name][0] += temp_reward

    for key in creation_cost:
        if key in arm_rewards:
            arm_rewards[key][1] += -1 * creation_cost[key]
        else:
            arm_rewards[key] = [0, -1 * creation_cost[key]]
    logging.info(f"Time taken to run the queries: {execute_cost}")
    return execute_cost, creation_cost, arm_rewards


def get_all_columns(connection):
    """
    Get all column in the database of the given connection. Note that the connection here is directly pointing to a
    specific database of interest

    :param connection: Sql connection
    :return: dictionary of lists - columns, number of columns
    """
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' limit 8"
    cursor = connection.cursor()
    cursor.execute(query)
    tables = cursor.fetchall()
    query = '''SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = 'public';'''
    columns = defaultdict(list)
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    count = 0
    for result in results:
        if (result[0],) in tables:
            columns[str.upper(result[0])].append(str.upper(result[1]))
            count += 1
    return columns, count


def get_all_columns_v2(connection):
    """
    Return all columns in table above 100 rows

    :param connection: Sql connection
    :return: dictionary of lists - columns, number of columns
    """
    query = '''SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS;'''
    columns = defaultdict(list)
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    # print("==========2==========")
    # print(results)
    count = 0
    for result in results:
        row_count = get_table_row_count(connection, result[0])
        if row_count >= constants.SMALL_TABLE_IGNORE:
            columns[result[0]].append(result[1])
            count += 1

    return columns, count


def get_current_pds_size(connection):
    """
    Get the current size of all the physical design structures
    :param connection: SQL Connection
    :return: size of all the physical design structures in MB
    """
    connection.commit()
    database_name = json.load(open(sys.argv[1]))['dataset']
    query = f"select datname, pg_size_pretty (pg_database_size(datname)) AS size from pg_database where datname = '{database_name}'"
    # query = f"select datname, pg_size_pretty (pg_database_size(datname)) AS size from pg_database"
    cursor = connection.cursor()
    cursor.execute(query)
    result = float(cursor.fetchall()[0][1].split(" ")[0])
    return result


def get_primary_key(connection, table_name):
    """
    Get Primary key of a given table. Note tis might not be in order (not sure)
    :param connection: SQL Connection
    :param schema_name: schema name of table
    :param table_name: table name which we want to find the PK
    :return: array of columns
    """
    if table_name in pk_columns_dict:
        pk_columns = pk_columns_dict[table_name]
    else:
        pk_columns = []
        query = f"""select pg_constraint.conname as pk_name,pg_attribute.attname as colname,pg_type.typname as typename from 
                    pg_constraint  inner join pg_class 
                    on pg_constraint.conrelid = pg_class.oid 
                    inner join pg_attribute on pg_attribute.attrelid = pg_class.oid 
                    and  pg_attribute.attnum = pg_constraint.conkey[1]
                    inner join pg_type on pg_type.oid = pg_attribute.atttypid
                    where pg_class.relname = '{str.lower(str.lower(table_name))}' 
                    and pg_constraint.contype='p'"""
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        pk_columns.append(results[0][1])
        pk_columns_dict[table_name] = pk_columns
    return pk_columns


def get_column_data_length_v2(connection, table_name, col_names):
    """
    get the data length of given set of columns
    :param connection: SQL Connection
    :param table_name: Name of the SQL table
    :param col_names: array of columns
    :return:
    """
    tables = get_tables(connection)
    varchar_count = 0
    column_data_length = 0
    table_name = str.lower(table_name)
    for column_name in col_names:
        column = tables[str.lower(table_name)].columns[str.lower(column_name)]
        if column.column_type == 'varchar':
            varchar_count += 1
        column_data_length += column.column_size if column.column_size else 0

    if varchar_count > 0:
        variable_key_overhead = 2 + varchar_count * 2
        return column_data_length + variable_key_overhead
    else:
        return column_data_length


def get_columns(connection, table_name):
    """
    Get all the columns in the given table

    :param connection: sql connection
    :param table_name: table name
    :return: dictionary of columns column name as the key
    """
    columns = {}
    cursor = connection.cursor()
    data_type_query = f"""SELECT COLUMN_NAME, DATA_TYPE
                          FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE 
                            TABLE_NAME = '{table_name}'"""
    cursor.execute(data_type_query)
    results = cursor.fetchall()
    # print("==========3==========")
    # print(results)
    variable_len_query = 'SELECT '
    variable_len_select_segments = []
    variable_len_inner_segments = []
    varchar_ids = []
    for result in results:
        col_name = result[0]
        column = Column(table_name, col_name, result[1])
        column.set_max_column_size(get_table_row_count(connection, table_name))
        if result[1] != 'varchar':
            column.set_column_size(get_table_row_count(connection, table_name))
        else:
            varchar_ids.append(col_name)
            variable_len_select_segments.append(f'''AVG(DL_{col_name})''')
            variable_len_inner_segments.append(f'''DATALENGTH({col_name}) DL_{col_name}''')
        columns[col_name] = column

    if len(varchar_ids) > 0:
        variable_len_query = variable_len_query + ', '.join(
            variable_len_select_segments) + ' FROM (SELECT TOP (1000) ' + ', '.join(
            variable_len_inner_segments) + f' FROM {table_name}) T'
        cursor.execute(variable_len_query)
        result_row = cursor.fetchone()
        for i in range(0, len(result_row)):
            columns[varchar_ids[i]].set_column_size(result_row[i])
    return columns


def get_tables(connection):
    """
    Get all tables as Table objects
    :param connection: SQL Connection
    :return: Table dictionary with table name as the key
    """
    global tables_global
    if tables_global is not None:
        return tables_global
    else:
        tables = {}
        pg_client = pg.PGHypo(connection)
        results = pg_client.get_tables("public")
        # print("==========1==========")
        # print(results)
        for result in results:
            table_name = result
            row_count = get_table_row_count(connection, table_name)
            pk_columns = get_primary_key(connection, table_name)
            tables[table_name] = Table(table_name, row_count, pk_columns)
            tables[table_name].set_columns(get_columns(connection, table_name))
        tables_global= tables
    return tables_global


def get_estimated_size_of_index_v1(connection, tbl_name, col_names):
    """
    This helper method can be used to get a estimate size for a index. This simply multiply the column sizes with a
    estimated row count (need to improve further)

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param tbl_name: name of the database table
    :param col_names: string list of column names
    :return: estimated size in MB
    """
    table = get_tables(connection)[str.lower(tbl_name)]
    header_size = 6
    nullable_buffer = 2
    primary_key = get_primary_key(connection, tbl_name)
    primary_key_size = get_column_data_length_v2(connection, tbl_name, primary_key)
    col_not_pk = tuple(set(col_names) - set(primary_key))
    key_columns_length = get_column_data_length_v2(connection, tbl_name, col_not_pk)
    index_row_length = header_size + primary_key_size + key_columns_length + nullable_buffer
    row_count = table.table_row_count
    estimated_size = row_count * index_row_length
    estimated_size = estimated_size/float(1024*1024)
    max_column_length = get_max_column_data_length_v2(connection, tbl_name, col_names)
    '''if max_column_length > 1700:
        print(f'Index going past 1700: {col_names}')
        estimated_size = 99999999'''
    logging.debug(f"{col_names} : {estimated_size}")
    return estimated_size


def get_max_column_data_length_v2(connection, table_name, col_names):
    tables = get_tables(connection)
    column_data_length = 0
    for column_name in col_names:
        column = tables[str.lower(table_name)].columns[str.lower(column_name)]
        column_data_length += column.max_column_size if column.max_column_size else 0
    return column_data_length


def get_query_plan(connection, query):
    """
    This returns the XML query plan of  the given query

    :param connection: sql_connection
    :param query: sql query for which we need the query plan
    :return: XML query plan as a String
    """
    cursor = connection.cursor()
    cursor.execute("SET SHOWPLAN_XML ON;")
    cursor.execute(query)
    query_plan = cursor.fetchone()[0]
    cursor.execute("SET SHOWPLAN_XML OFF;")
    return query_plan


def get_selectivity_v3(connection, query, predicates):
    """
    Return the selectivity of the given query

    :param connection: sql connection
    :param query: sql query for which predicates will be identified
    :param predicates: predicates of that query
    :return: Predicates list
    """
    # print("#################")
    # print(predicates)
    #query_plan_string = get_query_plan(connection, query)
    selectivity = {}
    if query+"" != "":
        #query_plan = QueryPlan(query_plan_string)

        tables = predicates.keys()
        '''for table in tables:
            read_rows[table] = 1000000000'''

        '''for index_scan in query_plan.clustered_index_usage:
            if index_scan[0] not in read_rows:
                read_rows[index_scan[0]] = 1000000000
            read_rows[index_scan[0]] = min(float(index_scan[5]), read_rows[index_scan[0]])'''

        for table in tables:
            selectivity[table] = 0.0
            table_name = str.lower(table)
            columns = predicates[table].keys()
            count = 0
            for column in columns:
                column = str.lower(column)
                if table_name == "nation" or table_name == "region":
                    sql = f"select count(distinct ('{column}')) from nation;"
                    cursor = connection.cursor()
                    cursor.execute(sql)
                    data = cursor.fetchall()[0]
                    selectivity[table] += data[0] / get_table_row_count(connection, table)
                else:
                    sql = f"select tablename,attname,n_distinct from pg_stats where schemaname = 'public' AND tablename = '{table_name}' AND attname = '{column}'"
                    cursor = connection.cursor()
                    try:
                        cursor.execute(sql)
                        data = cursor.fetchall()[0]
                    except Exception as e:
                        print(table_name)
                        print(column)
                    if data[2] < 0:
                        selectivity[table] -= data[2]
                    else:
                        selectivity[table] += data[2] / get_table_row_count(connection, table)
                count += 1
            selectivity[table] /= count
        # print(selectivity)
        return selectivity
    else:
        return 1


def remove_all_non_clustered(connection, schema_name):
    """
    Removes all non-clustered indexes from the database
    :param connection: SQL Connection
    :param schema_name: schema name related to the index
    """
    query = """select i.name as index_name, t.name as table_name
                from sys.indexes i, sys.tables t
                where i.object_id = t.object_id and i.type_desc = 'NONCLUSTERED'"""
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for result in results:
        drop_index(connection, schema_name, result[1], result[0])


def get_table_scan_times(connection, query_string):
    query_table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES[database])
    time, index_seeks, clustered_index_scans = execute_query_v1(connection, query_string)
    if clustered_index_scans:
        for index_scan in clustered_index_scans:
            table_name = index_scan[0]
            if len(query_table_scan_times[table_name]) < constants.TABLE_SCAN_TIME_LENGTH:
                query_table_scan_times[table_name].append(index_scan[constants.COST_TYPE_CURRENT_EXECUTION])
    return query_table_scan_times


def get_table_scan_times_structure():
    query_table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES[database[:-4]])
    return query_table_scan_times


def drop_all_dta_statistics(connection):
    query_get_stat_names = """SELECT OBJECT_NAME(s.[object_id]) AS TableName, s.[name] AS StatName
                                FROM sys.stats s
                                WHERE OBJECTPROPERTY(s.OBJECT_ID,'IsUserTable') = 1 AND s.name LIKE '_dta_stat%';"""
    cursor = connection.cursor()
    cursor.execute(query_get_stat_names)
    results = cursor.fetchall()
    for result in results:
        drop_statistic(connection, result[0], result[1])
    logging.info("Dropped all dta statistics")


def drop_statistic(connection, table_name, stat_name):
    query = f"DROP STATISTICS {table_name}.{stat_name}"
    cursor = connection.cursor()
    cursor.execute(query)


def set_arm_size(connection, bandit_arm):
    pg_client = pg.PGHypo(connection)
    result = pg_client.get_storage_cost_single(bandit_arm.index_oid)/1024
    bandit_arm.memory = result
    return bandit_arm


def restart_sql_server():
    command1 = f"pg_ctl restart"
    command2 = f"net start mssqlserver"
    with open(os.devnull, 'w') as devnull:
        subprocess.run(command1, shell=True, stdout=devnull)
        time.sleep(60)
        subprocess.run(command2, shell=True, stdout=devnull)
    logging.info("Server Restarted")
    return


def get_database_size(connection):
    database_size = 10240
    try:
        database_name = json.load(open(sys.argv[1]))['dataset']
        query = f"select datname, pg_size_pretty (pg_database_size(datname)) AS size from pg_database where datname = '{database_name}'"
        cursor = connection.cursor()
        cursor.execute(query)
        result = float(cursor.fetchall()[0][1].split(" ")[0])
        # print(result)
        return result
    except Exception as e:
        logging.error("Exception when get_database_size: " + str(e))
    return database_size
