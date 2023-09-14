import os
import time
from random import random
import sys
import traceback
import json
import pickle
import json
import psqlparse
import numpy as np
import logging

from victim_models.ICDE_2021.psql.PostgreSQL2 import PGHypo
CONFIGURATION_FILE = json.load(open(sys.argv[1]))
gen_config = CONFIGURATION_FILE["SWIRL_2022"]["generate_wl"]

def gen_attack_bad_suboptimal(dic, genmodel):
    workload = list()
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=True))
    pg = PGHypo()
    z1 = z[int(len(z)/2):]
    z2 = z[int(len(z)/16):int(len(z)/4):]
    while len(workload) < 9:
        print(len((workload)))
        choose1 = z2[0]
        choose2 = z2[1]
        choose = [choose1, choose2]
        sql = genmodel.generate_sql_suboptimal(choose)
        if not sql:
            continue
        if "order by" in sql and "select" in sql:
            orderby = "order by " + (sql.split("order by")[1]).split("select")[0]
            ex_orderby = sql.split("order by")[0]
            select = "select " + ex_orderby.split("select")[1]
            sql = select + ex_orderby.split("select")[0] + orderby
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                continue

        elif "select" in sql:
            sql = "select " + sql.split("select")[1] + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                continue
    while len(workload) < 18:
        print(len((workload)))
        choose1 = z1[np.random.randint(0, len(z1) - 1)]
        choose2 = z1[np.random.randint(0, len(z1) - 1)]
        choose3 = z1[np.random.randint(0, len(z1) - 1)]
        choose = [choose1, choose2, choose3]
        sql = genmodel.generate_sql_bad(choose)
        if not sql:
            continue
        if "30" in sql:
            sql.replace("is 30", "is 5")

        if "order by" in sql and "select" in sql:
            orderby = "order by " + (sql.split("order by")[1]).split("select")[0]
            ex_orderby = sql.split("order by")[0]
            select = "select " + ex_orderby.split("select")[1]
            sql = select + ex_orderby.split("select")[0] + orderby
            try:
                pg.execute_sql(sql)
                workload.append(sql)
                continue
            except:
                pg = PGHypo()
                continue

        elif "select" in sql:
            sql = "select " + sql.split("select")[1] + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
                continue
            except:
                pg = PGHypo()
                continue
    return workload


def gen_attack_bad(dic, genmodel):
    workload = list()
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=True))
    pg = PGHypo()
    print("======================")
    print(z)
    print("======================")
    print("gen_attack_bad")
    print("======================")
    z1 = z[int(len(z) / 2):]
    z2 = z[int(len(z) / 8):int(len(z) / 4):]
    while len(workload) < 18:
        choose1 = z1[np.random.randint(0, len(z1) - 1)]
        choose2 = z1[np.random.randint(0, len(z1) - 1)]
        choose3 = z1[np.random.randint(0, len(z1) - 1)]
        choose = [choose1, choose2, choose3]
        sql = genmodel.generate_sql(choose)
        if not sql:
            continue
        if "30" in sql:
            sql.replace("is 30", "is 5")

        if "order by" in sql and "select" in sql:
            orderby = "order by " + (sql.split("order by")[1]).split("select")[0]
            ex_orderby = sql.split("order by")[0]
            select = "select " + ex_orderby.split("select")[1]
            sql = select + ex_orderby.split("select")[0] + orderby
            try:
                pg.execute_sql(sql)
                workload.append(sql)
                continue
            except:
                pg = PGHypo()
                continue

        elif "select" in sql:
            sql = "select " + sql.split("select")[1] + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
                continue
            except:
                pg = PGHypo()
                continue
    return workload


def gen_attack_suboptimal(dic, genmodel):
    workload = list()
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=True))
    pg = PGHypo()
    print("======================")
    print(z)
    print("======================")
    print("gen_attack_suboptimal")
    print("======================")
    z1 = z[int(len(z) / 2):]
    z2 = z[int(len(z) / 16):int(len(z) / 4):]
    while len(workload) < 18:
        print(len(workload))
        choose1 = z2[0]
        choose2 = z2[1]
        choose = [choose1, choose2]
        sql = genmodel.generate_sql(choose)
        if not sql:
            continue
        if "order by" in sql and "select" in sql:
            orderby = "order by " + (sql.split("order by")[1]).split("select")[0]
            ex_orderby = sql.split("order by")[0]
            select = "select " + ex_orderby.split("select")[1]
            sql = select + ex_orderby.split("select")[0] + orderby
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                continue

        elif "select" in sql:
            sql = "select " + sql.split("select")[1] + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                continue
    return workload


def gen_attack_random_ood(dic, genmodel):
    workload = list()
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=True))
    pg = PGHypo()
    while len(workload) < 18:
        choose1 = z[np.random.randint(0, len(z) - 1)]
        choose2 = z[np.random.randint(0, len(z) - 1)]
        choose3 = z[np.random.randint(0, len(z) - 1)]
        choose = [choose1, choose2, choose3]
        sql = genmodel.generate_sql(choose)
        if not sql:
            continue
        if "30" in sql:
            sql.replace("is 30", "is 5")

        if "order by" in sql and "select" in sql:
            orderby = "order by " + (sql.split("order by")[1]).split("select")[0]
            ex_orderby = sql.split("order by")[0]
            select = "select " + ex_orderby.split("select")[1]
            sql = select + ex_orderby.split("select")[0] + orderby
            try:
                pg.execute_sql(sql)
                workload.append(sql)
                continue
            except:
                pg = PGHypo()
                continue

        elif "select" in sql:
            sql = "select " + sql.split("select")[1] + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
                continue
            except:
                pg = PGHypo()
                continue
    return workload


def gen_attack_not_ood():
    wd_generator = ds.TPCH(gen_config['work_dir'], int(gen_config['w_size']))
    workload = wd_generator.gen_workloads()
    return workload

def isfloat(str):
    try:
        float(str)
        return True
    except ValueError:
        return False

def gen_probing(dic):
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z))
    wd_generator = ds.TPCH(gen_config['work_dir'], int(gen_config['w_size']))
    workload = wd_generator.gen_workloads_probing(z)

    return workload


def gen_probing2(dic):
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z))
    wd_generator = ds.TPCH(gen_config['work_dir'], int(gen_config['w_size']))
    workload = wd_generator.gen_workloads_probing(z[:5])

    return workload


def gen_probingv3(dic, genmodel, zero):
    workload = []
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=False))
    z = z[zero:-3]
    weight = []
    k = 0
    biao = []
    for i in z:
        weight.append(1 / i[0])
        biao.append(k)
        k += 1
    total = sum(weight)
    weight_normalization = [x / total for x in weight]

    pg = PGHypo()
    while len(workload) < 18:
        choose = np.random.choice(biao, size=3, replace=False, p=weight_normalization)
        choose1 = z[choose[0]]
        choose2 = z[choose[1]]
        choose3 = z[choose[2]]
        choose = [choose1, choose2, choose3]
        sql = genmodel.generate_sql(choose)
        if not sql:
            continue
        if "order by" in sql and "select" in sql:
            orderby = "order by " + (sql.split("order by")[1]).split("select")[0]
            ex_orderby = sql.split("order by")[0]
            select = "select" + ex_orderby.split("select")[1]
            sql = select + ex_orderby.split("select")[0] + orderby
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                continue

        elif "select" in sql:
            sql = "select" + sql.split("select")[1] + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                logging.error("wrong sql:" + sql)
                continue
    return workload


def gen_workload_SWIRL():
    fread = open(os.getcwd() + "/victim_models/ICDE_2021/resources/workloads/tpc_h_static_100.json", 'r')
    lines = fread.readlines()
    workload = []
    for i in range(42):
        workload.append(json.loads(lines[i]))
    return workload

def gen_columns_SWIRL():
    columns = []
    pg = PGHypo()
    table_names = pg.get_tables()
    for table in table_names:
        attrs = pg.get_attributes(table)
        for attr in attrs:
            columns.append(table + "#" + attr)
    return columns

