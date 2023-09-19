from random import random
import sys
import traceback

import logging
import pickle
import json
import psqlparse
import numpy as np
import victim_models.ICDE_2020.generation.Dataset as ds
import psql.Encoding as en
import psql.ParserForIndex as pi

from victim_models.ICDE_2020.psql.PostgreSQL import PGHypo
CONFIGURATION_FILE = json.load(open(sys.argv[1]))
config = CONFIGURATION_FILE["ICDE_2020"]["generate_wl"]

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
    while len(workload) < 14:
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


def gen_attack_bad(dic, genmodel):
    workload = list()
    z = zip(dic.values(), dic.keys())
    z = list(sorted(z, reverse=True))
    pg = PGHypo()
    z1 = z[5:]
    z2 = [z[1], z[2]]
    while len(workload) < 14:
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
    z1 = z[5:]
    z2 = z[int(len(z) / 16):int(len(z) / 4):]
    while len(workload) < 14:
        choose1 = z2[0]
        choose2 = z2[1]
        choose3 = z2[2]
        a = np.random.rand()
        if a < 0.33:
            choose = [choose1,choose2]
        elif a < 0.66:
            choose = [choose1,choose3]
        else:
            choose = [choose2,choose3]
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
    while len(workload) < 14:
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
    wd_generator = ds.TPCH(config['work_dir'], int(config['w_size']))
    workload = wd_generator.gen_workloads()
    return workload


def isfloat(str):
    try:
        float(str)
        return True
    except ValueError:
        return False


def gen_probing(dic, genmodel, zero):
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
    while len(workload) < 14:
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
            sql = "select" + sql.split("select")[1] + sql.split("select")[0]
            try:
                pg.execute_sql(sql)
                workload.append(sql)
            except:
                pg = PGHypo()
                logging.error("wrong sql:" + sql)
                continue
    return workload


def gen_workload_ICDE():
    wd_generator = ds.TPCH(config['work_dir'], int(config['w_size']))
    workload = wd_generator.gen_workloads()
    return workload


def gen_candidate_ICDE(workload):
    for i in range(0, int(config['w_size'])):
        enc = en.encoding_schema()
        candidate = list()
        parser = pi.Parser(enc['attr'])
        f_i = gen_i_ICDE(i, parser, workload)
        for j in f_i:
            if j in candidate:
                continue
            # =======修改部分======= #
            # 将一个单/多属性候选索引形如“partsupp#ps_supplycost,ps_partkey,ps_suppkey”
            # 根据“#”拆分成head和tail部分
            head = j.split("#")[0]
            tail = j.split("#")[1]
            # 因为可能有多属性索引所以得循环把多属性索引拆成单属性索引
            for k in range(0, len(tail.split(","))):
                str = head + "#" + tail.split(",")[k]
                # 拆完后生成的单属性索引可能会重复出现在candidate里边，得排除
                if str in candidate:
                    continue
                candidate.append(str)
            # =======修改结束======= #
    return candidate


def gen_i_ICDE(__x, parser, workload):
    added_i = set()
    for i in range(len(workload)):
        if i > __x:
            continue
        b = psqlparse.parse_dict(workload[i])
        parser.parse_stmt(b[0])
        parser.gain_candidates_ICDE()
        if i == 8:
            added_i.add('lineitem#l_shipmode')
            added_i.add('lineitem#l_orderkey,l_shipmode')
            added_i.add('lineitem#l_shipmode,l_orderkey')
    f_i = parser.index_candidates | added_i
    f_i = list(f_i)
    f_i.sort()
    return f_i


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