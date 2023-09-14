import time

import numpy as np
import pandas as pd
import json
import random
import pickle
import os
import matplotlib.pyplot as plt
import math
import sys
import copy
import logging
from distutils.util import strtobool

sys.path.append(json.load(open(sys.argv[1]))["experiments_root"] + "/victim_models/CIKM_2020")
import Model.Model3CIKM as model
from workload_generation.BartSqlGen.model import GenerationTask

from generation.Gen_CIKM import gen_workload_CIKM
from generation.Gen_CIKM import gen_candidate_CIKM
from generation.Gen_CIKM import gen_probingv3
from generation.Gen_CIKM import gen_attack_bad_suboptimal,gen_attack_bad,gen_attack_suboptimal,gen_attack_random_ood,gen_attack_not_ood

from psql import PostgreSQL as pg

class CIKM_2020(object):
    def __init__(self, configuration_file):
        # get parameters

        self.root_config = configuration_file
        self.config = self.root_config["CIKM_2020"]
        self.probing_config = self.root_config["CIKM_2020"]["probing"]

        # get from prepare()
        self.workload = []
        self.index_candidates = []

        # get from train()
        self.no_index_cost = []

        self.init_rec_index = []
        self.init_rec_cost = []
        self.init_rec_reward = []

        # get from probing()
        self.probility_of_column = {}
        self._create_experiment_folder()
        self.init_reward = 1
        self.indexes_now = []
        self.reward_now = 1
        self.zero = 0

        self.first_probing = True

    def prepare(self):
        if strtobool(self.config["gen_new"]) == False:
            logging.info("Load workload and index candidate...")
            self.workload, self.index_candidates = self._load_wl_ic()
            logging.info("Index Candidate: " + self.index_candidates)
        else:
            logging.info("Generate new workload and index candidate...")
            self.workload, self.index_candidates = self._generate()
        self.original_workload = copy.deepcopy(self.workload)

    def train(self):
        logging.info("Init DQN Agent....")
        self.agent = model.DQN(self.workload[:], self.index_candidates, 'hypo',
                               self.config["model"]["algorithm"][0]["parameters"],
                               self.config["model"]["algorithm"][0]["is_dnn"],
                               self.config["model"]["algorithm"][0]["is_ps"],
                               self.config["model"]["algorithm"][0]["is_double"],
                               whether_first=True,
                               dir = json.load(open(sys.argv[1]))["result_path"] + "/" + self.root_config["experiments_id"] + "/")
        logging.info("Begin to Train Agent")
        cost, reward, _indexes = self.agent.train(False, False)

        indexes = []
        for _i, _idx in enumerate(_indexes):
            if _idx == 1.0:
                indexes.append(self.index_candidates[_i])
        logging.info("Current Best Index: ")
        logging.info(indexes)

        plt.figure(2)
        x = range(len(self.agent.rewards))
        y2 = self.agent.rewards
        plt.plot(x, y2, marker='x')
        plt.savefig(self.experiment_folder_path + '/' + str(self.agent.number) + "_" + str(
            self.config["model"]["algorithm"][0]["parameters"]["number"]) + "rewardfreq.png", dpi=120)
        plt.clf()
        plt.close()

        logging.info(
            "Reward Figures Are Drawed in " + self.experiment_folder_path + '/' + str(self.agent.number) + "_" + str(
                self.config["model"]["algorithm"][0]["parameters"]["number"]) + "rewardfreq.png")

        self.config["model"]["algorithm"][0]["parameters"]["number"] = int(
            self.config["model"]["algorithm"][0]["parameters"]["number"]) + 1

        self.init_rec_index = indexes
        self.init_rec_cost = self.agent.envx.cost_trace_overall
        self.init_rec_reward = self.agent.rewards

        self.indexes_now = indexes
        self.reward_now = reward

    def probing(self):
        _start = time.time()
        self._set_probing_config()
        self._get_columns()
        genmodel = GenerationTask()
        for i in range(self.probing_config["probing_num"]):
            # for i in range(1):
            for x in self.probility_of_column:
                for j in self.indexes_now:
                    if x.split("_")[1] in j:
                        self.probility_of_column[x] = self.probility_of_column[x] + self.probing_config["lr"] * (
                                    self.reward_now - self.init_reward) / 100
                        self._cdf_normalization(self.probing_config["lr"] * (self.reward_now - self.init_reward) / 100)
                    if x.split("#")[1] in j:
                        self.probility_of_column[x] = self.probility_of_column[x] + self.probing_config["lr"] * (
                                    self.reward_now - self.init_reward) / 100
                        self._cdf_normalization(self.probing_config["lr"] * (self.reward_now - self.init_reward) / 100)

            probing_workload = gen_probingv3(self.probility_of_column, genmodel, self.zero)

            self.agent = model.DQN(probing_workload, self.index_candidates, 'hypo',
                                   self.config["model"]["algorithm"][0]["parameters"],
                                   self.config["model"]["algorithm"][0]["is_dnn"],
                                   self.config["model"]["algorithm"][0]["is_ps"],
                                   self.config["model"]["algorithm"][0]["is_double"],
                                   whether_first=False,
                                   dir = json.load(open(sys.argv[1]))["result_path"] + "/" + self.root_config["experiments_id"] + "/")
            self.init_reward, self.reward_now, _indexes = self.agent.train(True, True)

            self.config["model"]["algorithm"][0]["parameters"]["number"] += 1

            indexes = []
            for _i, _idx in enumerate(_indexes):
                if _idx == 1.0:
                    indexes.append(self.index_candidates[_i])
            self.indexes_now = indexes
            logging.warning(self.probility_of_column)

        probility_of_column = sorted(self.probility_of_column.items(), key=lambda x: x[1])

        _end = time.time()
        print(_end - _start)
        logging.warning(probility_of_column)

    def poison(self):
        genmodel = GenerationTask()

        if self.root_config["attack_method"] == "bad&suboptimal":
            attack_workload = gen_attack_bad_suboptimal(self.probility_of_column, genmodel)
        elif self.root_config["attack_method"] == "bad":
            attack_workload = gen_attack_bad(self.probility_of_column, genmodel)
        elif self.root_config["attack_method"] == "PIPA":
            attack_workload = gen_attack_suboptimal(self.probility_of_column, genmodel)
        elif self.root_config["attack_method"] == "random":
            attack_workload = gen_attack_random_ood(self.probility_of_column, genmodel)
        elif self.root_config["attack_method"] == "not_ood":
            attack_workload = self.original_workload

        self._set_poison_config()
        self.config["model"]["algorithm"][0]["parameters"]["EPISODES"] = 400
        self.agent = model.DQN(attack_workload, self.index_candidates, 'hypo',
                               self.config["model"]["algorithm"][0]["parameters"],
                               self.config["model"]["algorithm"][0]["is_dnn"],
                               self.config["model"]["algorithm"][0]["is_ps"],
                               self.config["model"]["algorithm"][0]["is_double"],
                               whether_first=False,
                               dir = json.load(open(sys.argv[1]))["result_path"] + "/" + self.root_config["experiments_id"] + "/")
        self.init_reward, self.reward_now, _indexes = self.agent.train(True, False)

        plt.figure(2)
        x = range(len(self.agent.rewards))
        y2 = self.agent.rewards
        plt.plot(x, y2, marker='x')
        plt.savefig(self.experiment_folder_path + '/' + str(self.agent.number)+"_"+ str(self.config["model"]["algorithm"][0]["parameters"]["number"]) +"poison_rewardfreq.png", dpi=120)
        plt.clf()
        plt.close()

    def evaluation(self):
        self._set_evaluation_config(self.root_config["poison_percentage"])
        self.config["model"]["algorithm"][0]["parameters"]["EPISODES"] = 400
        self.agent = model.DQN(self.original_workload, self.index_candidates, 'hypo',
                               self.config["model"]["algorithm"][0]["parameters"],
                               self.config["model"]["algorithm"][0]["is_dnn"],
                               self.config["model"]["algorithm"][0]["is_ps"],
                               self.config["model"]["algorithm"][0]["is_double"], 
                               whether_first=False,
                               dir = json.load(open(sys.argv[1]))["result_path"] + "/" + self.root_config["experiments_id"] + "/")
        self.init_reward, self.reward_now, _indexes = self.agent.train(True, False)
        indexes = []
        for _i, _idx in enumerate(_indexes):
            if _idx == 1.0:
                indexes.append(self.index_candidates[_i])
        self.indexes_now = indexes

        plt.figure(2)
        x = range(len(self.agent.rewards))
        y2 = self.agent.rewards
        plt.plot(x, y2, marker='x')
        plt.savefig(self.experiment_folder_path + '/' + str(self.agent.number) + "_" + str(
            self.config["model"]["algorithm"][0]["parameters"]["number"]) + "after_poison_rewardfreq.png", dpi=120)
        plt.clf()
        plt.close()

        self.indexes_after_poison = indexes
        self.reward_after_poison = self.reward_now

        length = min(len(self.agent.rewards), len(self.init_rec_reward))
        best_reward_bias = max(self.agent.rewards) - max(self.init_rec_reward)
        avg_reward_bias = (sum(self.agent.rewards[-length:]) - sum(self.init_rec_reward[-length:])) / length

        after = np.array(self.agent.rewards[-length:])
        before = np.array(self.init_rec_reward[-length:])

        after_variance = np.var(after)
        before_variance = np.var(before)

        vmf = after_variance / before_variance

        best_cost_bias = best_reward_bias / (self.reward_now - best_reward_bias)
        avg_cost_bias = avg_reward_bias / (self.reward_now - avg_reward_bias)

        return best_reward_bias, avg_reward_bias, vmf, best_cost_bias, avg_cost_bias

    def finish(self, report):
        best_reward_bias = []
        mean_reward_bias = []
        wmf = []

        best_cost_bias = []
        mean_cost_bias = []

        for i in report:
            best_reward_bias.append(i[0])
            mean_reward_bias.append(i[1])
            wmf.append(i[2])

            best_cost_bias.append(i[3])
            mean_cost_bias.append(i[4])

        best_reward_bias_mean = sum(best_reward_bias) / 5
        mean_reward_bias_mean = sum(mean_reward_bias) / 5
        wmf_mean = sum(wmf) / 5

        best_cost_bias_mean = sum(best_cost_bias) / 5
        mean_cost_bias_mean = sum(mean_cost_bias) / 5

        logging.warning("The best reward bias: " + str(best_reward_bias) + " Mean： " + str(best_reward_bias_mean))
        logging.warning("The mean reward bias: " + str(mean_reward_bias) + " Mean： " + str(mean_reward_bias_mean))
        logging.warning("The wmf: " + str(wmf) + " Mean： " + str(wmf_mean))

        logging.warning("The best bias ratio: " + str(best_cost_bias) + " Mean： " + str(best_cost_bias_mean))
        logging.warning("The mean bias ratio: " + str(mean_cost_bias) + " Mean： " + str(mean_cost_bias_mean))

    def _get_rows(self):
        rows = pd.read_csv("./victim_models/CIKM_2020/Entry/random.csv", dtype=str)
        return rows

    def _set_poison_config(self):
        self.config = self.root_config["CIKM_2020"]
        self.config["model"]["algorithm"][0]["parameters"]["LEARNING_START"] = 0
        

    def _set_evaluation_config(self, percentage):
        self.config["model"]["algorithm"][0]["parameters"]["EPISODES"] = self.probing_config["epoches"]
        self.config["model"]["algorithm"][0]["parameters"]["EPISODES"] = int(
            self.config["model"]["algorithm"][0]["parameters"]["EPISODES"] * (1 / percentage))
        self.config["model"]["algorithm"][0]["parameters"]["LEARNING_START"] = 0

    def _cdf_normalization(self, num):
        zero = 0
        add = 0
        for key, value in self.probility_of_column.items():
            if value <= 0:
                zero = zero + 1
                add = add - value
                self.probility_of_column[key] = 0
        for key, value in self.probility_of_column.items():
            if value > 0:
                self.probility_of_column[key] = value - num / (len(self.probility_of_column) - zero)
                self.probility_of_column[key] = value - add / (len(self.probility_of_column) - zero)
        self.zero = zero

    def _get_columns(self):
        probility_of_column = {}
        for i in self.index_candidates:
            if "," in i:
                if "key" in i.split(",")[0]:
                    i_head = i.split("#")[0]
                    i_tail = i.split(",")[1]
                    i_re = i_head + "#" + i_tail.split(",")[0]
                    one_dic = {i_re: 0}
                else:
                    one_dic = {i.split(",")[0]: 0}
            else:
                one_dic = {i: 0}
            probility_of_column.update(one_dic)
        for key, value in probility_of_column.items():
            probility_of_column[key] = 1.0 / len(probility_of_column)
        self.probility_of_column = probility_of_column

    def _set_probing_config(self):
        self.config["model"]["algorithm"][0]["parameters"]["EPISODES"] = self.probing_config["epoches"]
        self.config["model"]["algorithm"][0]["parameters"]["LEARNING_START"] = self.probing_config["learning_start"]
        self.agent.whether_probing = True

    def _load_wl_ic(self):
        wf = open('Entry/workload.pickle', 'rb')
        workload = pickle.load(wf)
        cf = open('Entry/cands.pickle', 'rb')
        index_candidates = pickle.load(cf)
        logging.info("Load Workload & Candidate Successful")
        return workload, index_candidates

    def _generate(self):
        wf = gen_workload_CIKM()
        with open(self.experiment_folder_path + "/workload.json", "w") as file1:
            json.dump(wf, file1)
        cf = gen_candidate_CIKM(wf)
        with open(self.experiment_folder_path + "/candidate.json", "w") as file:
            json.dump(cf, file)
        logging.info("Generate Workload & Candidate Successfully")
        logging.info("Index Candidate: " + str(cf))
        return wf, cf

    def gen_evaluate(self, sql, mode):
        probing_workload = []
        for _ in range(14):
            probing_workload.append(sql)
        logging.info("++++ set probing config...")
        self._set_probing_config()
        self.agent = model.DQN(probing_workload, self.index_candidates, 'hypo',
                               self.config["model"]["algorithm"][0]["parameters"],
                               self.config["model"]["algorithm"][0]["is_dnn"],
                               self.config["model"]["algorithm"][0]["is_ps"],
                               self.config["model"]["algorithm"][0]["is_double"],
                               whether_first=False,
                               dir = json.load(open(sys.argv[1]))["result_path"] + "/" + self.root_config["experiments_id"] + "/")
        self.init_reward, self.reward_now, _indexes = self.agent.train(True, True)
        logging.info("++++ index_rec_system reward for probing_sql: " + str(self.reward_now))
        if self.reward_now > 0:
            logging.warning("Can find indexes to improve this sql")
            indexes = []
            for _i, _idx in enumerate(_indexes):
                if _idx == 1.0:
                    indexes.append(self.index_candidates[_i])
            self.indexes_now = indexes
        else:
            logging.warning("Cannot find any index to improve this sql")
            indexes = []
            for key, value in self.probility_of_column.items():
                indexes.append(key)
            self.indexes_now = indexes
            return 0

        if mode == "pretrain":
            return self._cal_pretrain_reward(self.reward_now)
        elif mode == "probing":
            return self._cal_probing_reward(self.indexes_now)
        elif mode == "poison":
            return self._cal_poison_reward(self.indexes_now, self.reward_now)

    def _cal_pretrain_reward(self, reward):
        reward = max(reward, 1)
        self.a = 1
        reward = self.a * reward
        return reward

    def _cal_probing_reward(self, indexes):
        reward = 100
        lr = self.probing_config["reward_lr"]
        for i in indexes:
            for key, value in self.probility_of_column.items():
                if "," in i:
                    if i.split(",")[0] in key:
                        reward -= lr * value
                elif i in key:
                    reward -= lr * value
        logging.info("This Sql Reward: " + str(max(reward, 0)))

        return max(reward, 0)

    def _cal_poison_reward(self, indexes):
        reward = 100
        lr = self.probing_config["reward_lr"]
        for i in indexes:
            for key, value in self.probility_of_column.items():
                if "," in i:
                    if i.split(",")[0] in key:
                        reward -= lr * value
                elif i in key:
                    reward -= lr * value
        logging.info("This Sql Reward: " + str(max(reward, 0)))

        return max(reward, 0)

    def _create_experiment_folder(self):
        assert os.path.isdir(
            self.root_config["result_path"]
        ), f"Folder for experiment results should exist at: ./" + self.root_config["result_path"]

        self.experiment_folder_path = self.root_config["result_path"] + "/" + self.root_config["experiments_id_exp"]
        assert os.path.isdir(self.experiment_folder_path) is False, (
            f"Experiment folder already exists."
            "terminating here because we don't want to overwrite anything."
        )
        if os.path.isdir(self.experiment_folder_path.rsplit("/",1)[0]) is False:
            experiment_folder_path = self.experiment_folder_path.rsplit("/",1)[0]
            os.mkdir(experiment_folder_path)
        os.mkdir(self.experiment_folder_path)



