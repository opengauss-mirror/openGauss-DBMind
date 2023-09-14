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

sys.path.append(json.load(open(sys.argv[1]))["experiments_root"] + "/victim_models/ICDE_2021")
from simulator import Simulator
from workload_generation.BartSqlGen.model import GenerationTask

from generation.Gen_ICDE_2021 import gen_workload_ICDE
from generation.Gen_ICDE_2021 import gen_columns_ICDE
from generation.Gen_ICDE_2021 import gen_probingv3
from generation.Gen_ICDE_2021 import gen_attack_bad_suboptimal, gen_attack_bad, gen_attack_suboptimal, \
    gen_attack_random_ood, gen_attack_not_ood

from pandas import DataFrame
import shared.configs_v2 as configs
import constants as constants
import shared.helper as helper
from bandits.experiment_report import ExpReport


class ICDE_2021(object):
    def __init__(self, configuration_file):
        # get parameters
        self.root_config = configuration_file
        self.config = self.root_config["ICDE_2021"]
        self.probing_config = self.root_config["ICDE_2021"]["probing"]
        
        # get from prepare()
        self.workload = []
        self.index_candidates = []

        # get from train()
        self.no_index_cost = []

        self.init_rec_index = []

        # get from probing()
        self.probility_of_column = {}
        self._create_experiment_folder()
        self.init_reward = 1
        self.indexes_now = []
        self.reward_now = 1

        self.first_probing = True
        self.agent = None
        self.zero = 0

    def prepare(self):
        if not strtobool(self.config["gen_new"]):
            logging.info("Load workload and index candidate...")
            self.workload = self._load_wl_ic()
            logging.info("Index Candidate: " + self.index_candidates)
        else:
            logging.info("Generate new workload and index candidate...")
            self.workload = self._generate()
        self.original_workload = copy.deepcopy(self.workload)

    def train(self):
        logging.info("Init Bandits Agent....")
        self.agent = Simulator()
        logging.info("Begin to Train Agent")
        exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB, configs.reps, 50)
        for r in range(configs.reps):
            sim_results, total_workload_time, arms, init_reward, cur_reward, exc_time, reward_trace = self.agent.run(
                self.workload, 50)
            indexes = []
            for value in arms.values():
                index = str.lower(value.table_name) + '.' + str.lower(value.index_cols[0])
                if index not in indexes:
                    indexes.append(index)
            temp = DataFrame(sim_results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
            temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
            temp[constants.DF_COL_REP] = r
            exp_report_mab.add_data_list(temp)

        # plot line graphs
        helper.plot_exp_report(configs.experiment_id, [exp_report_mab],
                               (constants.MEASURE_BATCH_TIME, constants.MEASURE_INDEX_CREATION_COST,
                                constants.MEASURE_QUERY_EXECUTION_COST, constants.MEASURE_INDEX_RECOMMENDATION_COST,
                                constants.MEASURE_MEMORY_COST))
        logging.info("Current Best Index: ")
        logging.info(indexes)
        print(indexes)

        self.config["model"]["algorithm"][0]["parameters"]["number"] = int(
            self.config["model"]["algorithm"][0]["parameters"]["number"]) + 1

        self.init_rec_index = indexes

        self.indexes_before_poison = indexes
        self.reward_before_poison = cur_reward
        self.exc_time_before_poison = exc_time
        self.indexes_now = indexes
        self.train_reward = cur_reward
        self.init_rewards = reward_trace[10:]
        self.before_rewards =reward_trace

    def probing(self):
        self._set_probing_config()
        self._get_columns()
        genmodel = GenerationTask()
        exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB, configs.reps, 100)
        for i in range(self.probing_config["probing_num"]):
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
            probing_workload = helper.get_queries_v3(probing_workload, 1)
            workload = self.workload
            for k in range(4):
                for j in range(len(probing_workload)):
                    workload.append(probing_workload[j])
            self.agent = Simulator()
            for r in range(configs.reps):
                sim_results, total_workload_time, arms, self.init_reward, self.reward_now, exc_time, reward_trace = self.agent.run(
                    workload, 100)
                indexes = []
                for value in arms.values():
                    index = str.lower(value.table_name) + '.' + str.lower(value.index_cols[0])
                    if index not in indexes:
                        indexes.append(index)
                temp = DataFrame(sim_results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                       constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_mab.add_data_list(temp)
            '''helper.plot_exp_report_v3(configs.experiment_id, i, [exp_report_mab],
                                      (constants.MEASURE_BATCH_TIME, constants.MEASURE_INDEX_CREATION_COST,
                                       constants.MEASURE_QUERY_EXECUTION_COST,
                                       constants.MEASURE_INDEX_RECOMMENDATION_COST,
                                       constants.MEASURE_MEMORY_COST))'''
            self.indexes_now = indexes
            print(indexes)

            self.config["model"]["algorithm"][0]["parameters"]["number"] += 1

        probility_of_column = sorted(self.probility_of_column.items(), key=lambda x: x[1])

        print(probility_of_column)

    def poison(self):
        genmodel = GenerationTask()

        workload = self.workload[0:72]
        print(len(workload))
        poison_workload = []
        ep = 0
        while ep < 1:
            ep = ep + 1
            if self.root_config["attack_method"] == "bad&suboptimal":
                attack_workload = gen_attack_bad_suboptimal(self.probility_of_column, genmodel)
            elif self.root_config["attack_method"] == "bad":
                attack_workload = gen_attack_bad(self.probility_of_column, genmodel)
            elif self.root_config["attack_method"] == "PIPA":
                attack_workload = gen_attack_suboptimal(self.probility_of_column, genmodel)
            elif self.root_config["attack_method"] == "random":
                attack_workload = gen_attack_random_ood(self.probility_of_column, genmodel)
            elif self.root_config["attack_method"] == "not_ood":
                attack_workload = gen_attack_not_ood()
            attack_workload = helper.get_queries_v3(attack_workload, ep)
            for t in range(2):
                for i in range(18):
                    poison_workload.append(attack_workload[i])
                for i in range(18):
                    poison_workload.append(workload[i])
        for i in range(2):
            for j in range(36):
                poison_workload.append(self.workload[j])
        print(len(poison_workload))
        for i in range(len(poison_workload)):
            workload.append(poison_workload[i])
        print(len(workload))
        logging.warning(poison_workload)
        exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB, configs.reps, 50+ep*50+50)
        self.agent = Simulator()
        for r in range(configs.reps):
            sim_results, total_workload_time, arms, init_reward, cur_reward, exc_time, reward_trace = self.agent.run(
                workload, 50+ep*50+50)
            indexes = []
            for value in arms.values():
                index = str.lower(value.table_name) + '.' + str.lower(value.index_cols[0])
                if index not in indexes:
                    indexes.append(index)
            temp = DataFrame(sim_results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
            temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
            temp[constants.DF_COL_REP] = r
            exp_report_mab.add_data_list(temp)

        # plot line graphs
        helper.plot_exp_report_v2(configs.experiment_id, [exp_report_mab],
                                  (constants.MEASURE_BATCH_TIME, constants.MEASURE_INDEX_CREATION_COST,
                                   constants.MEASURE_QUERY_EXECUTION_COST, constants.MEASURE_INDEX_RECOMMENDATION_COST,
                                   constants.MEASURE_MEMORY_COST))

        self.indexes_after_poison = indexes
        self.reward_after_poison = cur_reward
        self.exc_time_after_poison = exc_time
        self.cur_rewards = reward_trace[50+ep*50+10:]
        self.after_rewards = reward_trace[50+ep*50:]
        print(self.init_reward)
        print(self.cur_rewards)

        length = min(len(self.cur_rewards), len(self.init_rewards))
        best_reward_bias = max(self.cur_rewards) - max(self.init_rewards)
        avg_reward_bias = (sum(self.cur_rewards[-length:]) - sum(self.init_rewards[-length:])) / length

        after = np.array(self.cur_rewards[-length:])
        before = np.array(self.init_rewards[-length:])

        after_variance = np.var(after)
        before_variance = np.var(before)

        vmf = after_variance / before_variance

        best_cost_bias = best_reward_bias / self.train_reward
        avg_cost_bias = avg_reward_bias / self.train_reward

        return best_reward_bias, avg_reward_bias, vmf, best_cost_bias, avg_cost_bias

    def evaluation(self):
        logging.warning("============Rewards&Indexes============")
        logging.warning(self.indexes_before_poison)
        logging.warning(self.before_rewards)
        logging.warning(self.indexes_after_poison)
        logging.warning(self.after_rewards)
        logging.warning("============Rewards&Indexes============")
        logging.info("=============== Before Poison ===============")
        logging.info("indexes before poison: " + str(self.indexes_before_poison))
        logging.info("reward before poison: " + str(self.reward_before_poison))
        logging.info("exc_time before poison: " + str(self.exc_time_before_poison))
        logging.info("\n\n")
        logging.info("=============== After Poison ===============")
        logging.info("indexes after poison: " + str(self.indexes_after_poison))
        logging.info("reward after poison: " + str(self.reward_after_poison))
        logging.info("exc_time after poison: " + str(self.exc_time_after_poison))

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
        rows = pd.read_csv("./victim_models/ICDE_2021/Entry/random.csv", dtype=str)
        return rows

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
                self.probility_of_column[key] = value + add / (len(self.probility_of_column) - zero)
        self.zero = zero

    def _get_columns(self):
        probility_of_column = {}
        columns = gen_columns_ICDE()
        for i in columns:
            probility_of_column[i] = 1.0 / len(columns)
        self.probility_of_column = probility_of_column

    def _set_probing_config(self):
        self.config["model"]["algorithm"][0]["parameters"]["EPISODES"] = self.probing_config["epoches"]
        self.config["model"]["algorithm"][0]["parameters"]["LEARNING_START"] = self.probing_config["learning_start"]

    def _load_wl_ic(self):
        # print('=====load workload=====')
        wf = open('Entry/workload.pickle', 'rb')
        workload = pickle.load(wf)
        # print('=====load candidate =====')
        cf = open('Entry/cands.pickle', 'rb')
        index_candidates = pickle.load(cf)
        logging.info("Load Workload & Candidate Successful")
        return workload, index_candidates

    def _generate(self):
        wf = gen_workload_ICDE()
        with open(self.experiment_folder_path + "/workload.json", "w") as file1:
            json.dump(wf, file1)
        logging.info("Generate Workload Successfully")
        return wf


    def _create_experiment_folder(self):
        assert os.path.isdir(
            self.root_config["result_path"]
        ), f"Folder for experiment results should exist at: ./" + self.root_config["result_path"]

        self.experiment_folder_path = self.root_config["result_path"] + "/ID_" + self.root_config["experiments_id"]
        assert os.path.isdir(self.experiment_folder_path) is False, (
            f"Experiment folder already exists."
            "terminating here because we don't want to overwrite anything."
        )

        os.mkdir(self.experiment_folder_path)



