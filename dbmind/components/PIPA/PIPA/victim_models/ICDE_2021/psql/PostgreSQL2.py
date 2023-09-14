import os
from configparser import ConfigParser
from typing import List
import psycopg2 as pg
import pandas as pd
import time
import sys
import json
CONFIGURATION_FILE = json.load(open(sys.argv[1]))

class PGHypo:
    def __init__(self):
        database = json.load(open(sys.argv[1]))["psql_connect"]
        self.conn = pg.connect(database=json.load(open(sys.argv[1]))["dataset"], user=database["pg_user"], password=database["pg_password"],
                              host=database["pg_ip"], port=database["pg_port"])


    def close(self):
        self.conn.close()

    def execute_create_hypo(self, index):
        schema = index.split("#")
        sql = "SELECT indexrelid FROM hypopg_create_index('CREATE INDEX ON " + schema[0] + "(" + schema[1] + ")') ;"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return int(rows[0][0])

    def execute_delete_hypo(self, oid):
        sql = "select * from hypopg_drop_index(" + str(oid) + ");"
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        flag = str(rows[0][0])
        if flag == "t":
            return True
        return False

    def get_queries_cost(self, query_list):
        cost_list: List[float] = list()
        cur = self.conn.cursor()
        for i, query in enumerate(query_list):
            query = "explain " + query
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
            cost_info = str(df[0][0])
            cost_list.append(float(cost_info[cost_info.index("..") + 2:cost_info.index(" rows=")]))
        return cost_list

    def get_storage_cost(self, oid_list):
        costs = list()
        cur = self.conn.cursor()
        for i, oid in enumerate(oid_list):
            if oid == 0:
                continue
            sql = "select * from hypopg_relation_size(" + str(oid) +");"
            cur.execute(sql)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
            cost_info = str(df[0][0])
            cost_long = int(cost_info)
            costs.append(cost_long)
            # print(cost_long)
        return costs

    def execute_sql(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def delete_indexes(self):
        sql = 'select * from hypopg_reset();'
        self.execute_sql(sql)

    def get_sel(self, table_name, condition):
        cur = self.conn.cursor()
        totalQuery = "select * from " + table_name + ";"
        cur.execute("EXPLAIN " + totalQuery)
        rows = cur.fetchall()[0][0]
        #     print(rows)
        #     print(rows)
        total_rows = int(rows.split("rows=")[-1].split(" ")[0])

        resQuery = "select * from " + table_name + " Where " + condition + ";"
        # print(resQuery)
        cur.execute("EXPLAIN  " + resQuery)
        rows = cur.fetchall()[0][0]
        #     print(rows)
        select_rows = int(rows.split("rows=")[-1].split(" ")[0])
        return select_rows/total_rows

    def get_rel_cost(self, query_list):
        print("real")
        cost_list: List[float] = list()
        cur = self.conn.cursor()
        cost_sum = 0
        for i, query in enumerate(query_list):
            print("Query :" + str(i))
            print(query)
            _start = time.time()
            cur.execute(query)
            _end = time.time()
            cost_list.append(_end-_start)
            cost_sum += (_end-_start)
        return cost_sum

    def create_indexes(self, indexes):
        i = 0
        for index in indexes:
            schema = index.split("#")
            sql = 'CREATE INDEX START_X_IDx' + str(i) + ' ON ' + schema[0] + "(" + schema[1] + ');'
            print(sql)
            self.execute_sql(sql)
            i += 1

    def delete_t_indexes(self):
        sql = "SELECT relname from pg_class where relkind = 'i' and relname like 'start_x_idx%';"
        print(sql)
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        indexes = []
        for row in rows:
            indexes.append(row[0])
        print(indexes)
        for index in indexes:
            sql = 'drop index ' + index + ';'
            print(sql)
            self.execute_sql(sql)

    def get_tables(self):
        tables_sql = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%'ORDER BY tablename;"
        cur = self.conn.cursor()
        cur.execute(tables_sql)
        rows = cur.fetchall()
        table_names = list()
        for i, table_name in enumerate(rows):
            table_names.append(table_name[0])
        return table_names

    def get_attributes(self, table_name):
        attrs_sql = f"select COLUMN_NAME from information_schema.columns where table_schema='public' and table_name='{table_name}';"
        cur = self.conn.cursor()
        cur.execute(attrs_sql)
        rows = cur.fetchall()
        attrs = list()
        for i, attr in enumerate(rows):
            attrs.append(attr[0])
        return attrs
