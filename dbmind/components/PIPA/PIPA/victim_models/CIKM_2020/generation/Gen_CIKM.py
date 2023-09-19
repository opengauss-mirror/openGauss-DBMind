from random import random
import sys
import traceback

import pickle
import json
import psqlparse
import numpy as np
import generation.Dataset as ds
import psql.Encoding as en
import psql.ParserForIndex as pi
import logging

from victim_models.CIKM_2020.psql.PostgreSQL import PGHypo
CONFIGURATION_FILE = json.load(open(sys.argv[1]))
gen_config = CONFIGURATION_FILE["CIKM_2020"]["generate_wl"]


def gen_attack_bad_suboptimal(dic, genmodel):
    workload = list()
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=True))
    pg = PGHypo()
    z1 = z[5:]
    z2 = [z[1], z[2]]
    while len(workload) < 7:
        choose1 = z2[0]
        choose2 = z2[1]
        choose = [choose1, choose2]
        sql = None
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
    while len(workload) < 18:
        choose1 = z1[np.random.randint(0, len(z1) - 1)]
        choose2 = z1[np.random.randint(0, len(z1) - 1)]
        choose3 = z1[np.random.randint(0, len(z1) - 1)]
        choose = [choose1, choose2, choose3]

        sql = None
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


def gen_attack_bad(dic, genmodel):
    workload = list()
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=True))
    pg = PGHypo()
    z1 = z[int(1 * len(z) / 8):]
    z2 = [z[1], z[2]]
    while len(workload) < 18:
        choose1 = z1[np.random.randint(0, len(z1) - 1)]
        choose2 = z1[np.random.randint(0, len(z1) - 1)]
        choose3 = z1[np.random.randint(0, len(z1) - 1)]
        choose = [choose1, choose2, choose3]
        sql = None
        sql = genmodel.generate_sql(choose)
        if not sql:
            continue
        if choose1[1].replace("#", ".") not in sql and choose2[1].replace("#", ".") not in sql and choose3[1].replace("#", ".") not in sql:
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
    z1 = z[5:]
    z2 = z[4:int(1 * len(z) / 4):]
    print(len(z2))
    while len(workload) < 18:
        choose1 = z2[np.random.randint(0, len(z2) - 1)]
        choose2 = z2[np.random.randint(0, len(z2) - 1)]
        choose = [choose1, choose2]
        sql = genmodel.generate_sql(choose)
        if not sql:
            continue
        if choose1[1].replace("#", ".") not in sql and choose2[1].replace("#", ".") not in sql:
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

        sql = None
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
    probing_num = 0
    while len(workload) < 18:
        probing_num += 1
        choose = np.random.choice(biao, size=3, replace=False, p=weight_normalization)
        choose1 = z[choose[0]]
        choose2 = z[choose[1]]
        choose3 = z[choose[2]]
        choose = [choose1, choose2, choose3]
        print("=========choose index==========")
        print(choose)
        print("=========choose index==========")
        sql = None
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
            sql = "select" + sql.split("select")[1] + " " + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                logging.error("wrong sql:" + sql)
                continue
    print(probing_num/18)
    return workload


def gen_workload_CIKM():
    wd_generator = ds.TPCH(gen_config['work_dir'], int(gen_config['w_size']))
    workload = wd_generator.gen_workloads()
    return workload


def gen_candidate_CIKM(workload):
    enc = en.encoding_schema()
    parser = pi.Parser(enc['attr'])
    candidate = list()
    for i in range(0, int(gen_config['w_size'])):
        f_i = gen_i(i, parser, workload)
        for j in f_i:
            if j in candidate:
                continue
            else:
                candidate.append(j)
    return candidate


def gen_i(__x, parser, workload):
    added_i = set()
    for i in range(len(workload)):
        if i > __x:
            continue
        b = psqlparse.parse_dict(workload[i])
        parser.parse_stmt(b[0])
        parser.gain_candidates()
        if i == 8:
            added_i.add('lineitem#l_shipmode')
            added_i.add('lineitem#l_orderkey,l_shipmode')
            added_i.add('lineitem#l_shipmode,l_orderkey')
    f_i = parser.index_candidates | added_i
    f_i = list(f_i)
    f_i.sort()
    return f_i