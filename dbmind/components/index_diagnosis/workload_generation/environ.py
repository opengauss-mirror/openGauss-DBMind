# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

import os
import copy
import json

import traceback
import logging

from .generation_utils import gen_com
from .generation_utils import constants
from .generation_utils.openGauss_dbms import openGaussDatabaseConnector
from .generation_utils.mod_sql import vec2sql


class DBEnviron:
    def __init__(self, args):
        self.args = args

        self.exp_conf = None
        self.base_exp_conf = None
        self.connector = None
        self.columns = None

        logging.info(f"The workload level is `{self.args.work_level}({self.args.work_type})`.")
        logging.info(f"The index selection victim algorithm is `{self.args.victim}`.")
        logging.info(f"The mode of evaluated reward is `{self.args.reward}"
                     f"-({self.args.reward_form}, base:{self.args.reward_base})`.")
        logging.info(f"The max difference between `src` and `tgt` is `{self.args.max_diff}`.")
        logging.info(f"The perturbation mode between `src` and `tgt` is `{self.args.pert_mode}`.")

    def setup(self, autocommit=True):
        # 1. Set up the database connection.
        self.connector = openGaussDatabaseConnector(self.args, autocommit=autocommit)

        logging.disable(logging.DEBUG)
        self.connector.drop_indexes()
        logging.disable(logging.INFO)

        # 2. Load the configuration of the index advisor.
        if os.path.exists(self.args.exp_file):
            with open(self.args.exp_file, "r") as rf:
                self.exp_conf = json.load(rf)
            logging.disable(logging.DEBUG)
            logging.info(f"Load the exp_conf of victim `{self.args.victim}` "
                         f"from `{self.args.exp_file}`.")
            logging.info(f"The parameters' key of the algorithms is `{self.args.sel_param}`.")
            logging.disable(logging.INFO)

        if self.args.reward_base:
            # load the `baseline` configuration.
            with open(self.args.base_exp_file, "r") as rf:
                self.base_exp_conf = json.load(rf)
            logging.disable(logging.DEBUG)
            logging.info(f"Load the base_exp_conf of heuristic baseline `{self.args.baseline}` "
                         f"from `{self.args.base_exp_file}`.")
            logging.disable(logging.INFO)

        # 3. Get the schema information.
        if os.path.exists(self.args.schema_file):
            tables, self.columns = gen_com.get_columns_from_schema(self.args.schema_file)
            logging.disable(logging.DEBUG)
            logging.info(f"Load the schema from `{self.args.schema_file}`.")
            logging.disable(logging.INFO)
        else:
            tables, self.columns = gen_com.get_columns_from_db(self.connector)
            logging.disable(logging.DEBUG)
            logging.info(f"Load the schema from the database.")
            logging.disable(logging.INFO)

    def step(self, decoded_words, sql_tokens, last_reward, idx2word, col_info):
        rewards = list()
        for qi in range(len(sql_tokens)):
            tgt_vec = copy.deepcopy(sql_tokens[qi]["pno_tokens"])
            tgt_vec[:len(decoded_words[qi])] = decoded_words[qi]

            if sql_tokens[qi]["pno_tokens"] == tgt_vec:
                rewards.append(0.)
            elif len(sql_tokens[qi]["pno_tokens"]) < len(decoded_words[qi]):
                rewards.append(last_reward[qi])
            elif sql_tokens[qi]["pno_tokens"][len(decoded_words[qi]) - 1] == decoded_words[qi][-1]:
                rewards.append(last_reward[qi])
            else:
                try:
                    tgt_sql = vec2sql([sql_tokens[qi]], [tgt_vec], idx2word, col_info)[0]["sql_text"]
                    self.connector.get_ind_cost(tgt_sql, "")
                    rewards.append(self.get_index_reward(sql_tokens[qi], tgt_vec, idx2word, col_info))
                except:
                    rewards.append(0.)

        return rewards

    def get_output_indexes(self, work_list, algo):
        # Get the set of the recommended indexes from the tested index advisor.
        indexes = list()
        return indexes

    def get_advisor_result(self, sql_token, tgt_sql):
        no_cost, ind_cost = list(), list()
        # 1) basic: comparison against the `without index` case.
        if not self.args.reward_base:
            if self.args.reward == "all_dynamic":
                if self.args.work_level == "query":
                    work_list = [sql_token["sql"]]
                elif self.args.work_level == "workload":
                    work_list = [sql_token["sql"]] + sql_token["workload"]["sql"]

                indexes = self.get_output_indexes(work_list)
                if self.args.cost_estimator == "optimizer":
                    no_cost_, ind_cost_ = 0, 0
                    for sql in work_list:
                        no_cost_ += self.connector.get_ind_cost(sql, "")
                        ind_cost_ += self.connector.get_ind_cost(sql, indexes)
                    no_cost.append(no_cost_)
                    ind_cost.append(ind_cost_)
            else:
                if self.args.cost_estimator == "optimizer":
                    if self.args.work_level == "query":
                        no_cost.append(sql_token[self.args.victim].get("no_cost", 0))
                        ind_cost.append(sql_token[self.args.victim].get("ind_cost", 0))
                    elif self.args.work_level == "workload":
                        no_cost.append(sql_token["workload"][self.args.victim].get("no_cost", 0))
                        ind_cost.append(sql_token["workload"][self.args.victim].get("ind_cost", 0))
                    else:
                        raise NotImplementedError

            if self.args.work_level == "query":
                work_list = [tgt_sql]
            elif self.args.work_level == "workload":
                work_list = [tgt_sql] + sql_token["workload"]["sql"]

            try:
                if "dynamic" in self.args.reward:
                    indexes = self.get_output_indexes(work_list)
                elif self.args.reward == "static":
                    if self.args.work_level == "query":
                        indexes = sql_token[self.args.victim]["indexes"]
                    elif self.args.work_level == "workload":
                        indexes = sql_token["workload"][self.args.victim]["indexes"]

                if self.args.cost_estimator == "optimizer":
                    no_cost_, ind_cost_ = 0, 0
                    for sql in work_list:
                        no_cost_ += self.connector.get_ind_cost(sql, "")
                        ind_cost_ += self.connector.get_ind_cost(sql, indexes)
                    no_cost.append(no_cost_)
                    ind_cost.append(ind_cost_)
            except Exception as e:
                if self.args.cost_estimator == "optimizer":
                    no_cost.append(no_cost[0])
                    ind_cost.append(ind_cost[0])
                logging.error(e)
                logging.error(traceback.format_exc())

        # 2) base: comparison against the `baseline` case.
        else:
            if self.args.reward == "all_dynamic":
                if self.args.work_level == "query":
                    work_list = [sql_token["sql"]]
                elif self.args.work_level == "workload":
                    work_list = [sql_token["sql"]] + sql_token["workload"]["sql"]

                base_indexes = self.get_output_indexes(work_list)
                indexes = self.get_output_indexes(work_list)

                if self.args.cost_estimator == "optimizer":
                    no_cost_, ind_cost_ = 0, 0
                    for sql in work_list:
                        no_cost_ += self.connector.get_ind_cost(sql, base_indexes)
                        ind_cost_ += self.connector.get_ind_cost(sql, indexes)
                    no_cost.append(no_cost_)
                    ind_cost.append(ind_cost_)
            else:
                if self.args.cost_estimator == "optimizer":
                    if self.args.work_level == "query":
                        no_cost.append(sql_token[self.args.baseline].get("ind_cost", 0))
                        ind_cost.append(sql_token[self.args.victim].get("ind_cost", 0))
                    elif self.args.work_level == "workload":
                        no_cost.append(sql_token["workload"][self.args.baseline].get("ind_cost", 0))
                        ind_cost.append(sql_token["workload"][self.args.victim].get("ind_cost", 0))
                    else:
                        raise NotImplementedError

            if self.args.work_level == "query":
                work_list = [tgt_sql]
            elif self.args.work_level == "workload":
                work_list = [tgt_sql] + sql_token["workload"]["sql"]

            try:
                if "dynamic" in self.args.reward:
                    base_indexes = self.get_output_indexes(work_list)
                    indexes = self.get_output_indexes(work_list)
                elif self.args.reward == "static":
                    if self.args.work_level == "query":
                        base_indexes = sql_token[self.args.baseline]["indexes"]
                        indexes = sql_token[self.args.victim]["indexes"]
                    elif self.args.work_level == "workload":
                        base_indexes = sql_token["workload"][self.args.baseline]["indexes"]
                        indexes = sql_token["workload"][self.args.victim]["indexes"]

                if self.args.cost_estimator == "optimizer":
                    no_cost_, ind_cost_ = 0, 0
                    for sql in work_list:
                        no_cost_ += self.connector.get_ind_cost(sql, base_indexes)
                        ind_cost_ += self.connector.get_ind_cost(sql, indexes)
                    no_cost.append(no_cost_)
                    ind_cost.append(ind_cost_)
            except Exception as e:
                if self.args.cost_estimator == "optimizer":
                    no_cost.append(no_cost[0])
                    ind_cost.append(ind_cost[0])
                logging.error(e)
                logging.error(traceback.format_exc())

        if self.args.cost_estimator == "optimizer":
            return no_cost, ind_cost

    def get_index_reward(self, src_token, tgt_vec, idx2word, col_info):
        tgt_sql = vec2sql([src_token], [tgt_vec], idx2word, col_info)[0]["sql_text"]

        if self.args.cost_estimator == "optimizer":
            no_cost, ind_cost = self.get_advisor_result(src_token, tgt_sql)

        if self.args.cost_estimator == "optimizer":
            if no_cost[0] == 0 or no_cost[1] == 0:
                reward = 0.
            else:
                if self.args.reward_form == "cost_red_ratio":
                    before, after = 1 - ind_cost[0] / no_cost[0], 1 - ind_cost[1] / no_cost[1]
                    if before <= 0:
                        reward = 0.
                    else:
                        reward = 1 - after / before
                elif self.args.reward_form == "cost_ratio":
                    before, after = ind_cost[0] / no_cost[0], ind_cost[1] / no_cost[1]
                    reward = (after / before)
                elif self.args.reward_form == "cost_ratio_norm":
                    before, after = ind_cost[0] / no_cost[0], ind_cost[1] / no_cost[1]
                    reward = (after / before) - 1.0
                elif self.args.reward_form == "inv_cost_ratio_norm":
                    if ind_cost[1] == 0 or ind_cost[0] == 0:
                        reward = 0.
                    else:
                        before, after = no_cost[0] / ind_cost[0], no_cost[1] / ind_cost[1]
                        reward = 1.0 - (after / before)

        return reward
