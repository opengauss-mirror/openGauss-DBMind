import json
import os
import subprocess
import numpy as np


class TPCH:
    def __init__(self, query_root, number):
        self.query_root = os.path.abspath(query_root)
        self.dss_query_path = os.path.join(query_root, "queries")
        self.query_env = os.environ.copy()
        self.dbgen_dir = query_root
        self.query_env['DSS_QUERY'] = self.dss_query_path
        self.number = number

    def gen_one_query(self, i):
        print(os.path)
        print(i)
        print(self.dbgen_dir)
        p = subprocess.Popen([os.path.join(".", "qgen"), str(i)],
                             cwd=self.dbgen_dir, env=self.query_env, stdout=subprocess.PIPE)
        lines = p.stdout.readlines()
        sql = ""
        for i, line in enumerate(lines):
            sql += str(line, encoding="utf-8")
        p.communicate()
        return sql

    def gen_workloads(self):
        results = list()
        actual = 0
        _seq = 0
        # _seq = np.random.randint(0,21)
        while actual < self.number:
            index = _seq % 22 + 1
            if index == 15 or index == 7 or index == 19:
            # if index == 15 or index == 7 or index == 19 or index == 2 or index == 11 \
            #         or index == 13 or index == 16 or index == 22:
                _seq += 1
                continue
            one_query = self.gen_one_query(index)
            results.append(one_query)
            actual += 1
            _seq += 1
        return results


    def gen_workloads_probing(self, dic):
        results = list()
        actual = 0
        _seq = 0       
        while actual < self.number:
            flag = 0
            index = _seq % 22 + 1
            # if index == 2 or index == 15 or index == 17 or index == 20:
            if index == 15 or index == 7 or index == 19:
            # if index == 15 or index == 7 or index == 19 or index == 2 or index == 11 \
            #        or index == 13 or index == 16 or index == 22:
                _seq += 1
                continue
            one_query = self.gen_one_query(index)
            for x in dic:
                if x[1] in one_query or x[1].split("_")[1] in one_query:
                    if x[0] !=0:
                        flag = 1
            if flag == 0:
                results.append(one_query)
                actual += 1
            _seq += 1
        return results

    def gen_workloads2(self):
        results = list()
        query_type = list()
        actual = 0
        _seq = 0
        while actual < self.number:
            index = _seq % 22 + 1
            if index == 15 or index == 7 or index == 19:
                _seq += 1
                continue
            query_type.append(index)
            results.append(self.gen_one_query(index))
            actual += 1
            _seq += 1
        return results, query_type


def load_mode_parameters(model_conf):
    """
   :param model_conf: file name
   :return: parameters in json form
    """
    path = os.path.abspath("..") + "/Conf/"+model_conf
    f = open(path)
    return json.load(f)