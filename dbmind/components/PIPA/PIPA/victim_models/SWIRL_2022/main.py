import sys
import json
import numpy as np

import copy
import importlib
import logging
import pickle
import time
import shutil
import os
from pathlib import Path

sys.path.append(json.load(open(sys.argv[1]))["experiments_root"] + "/victim_models/SWIRL_2022")
sys.path.append(json.load(open(sys.argv[1]))["experiments_root"] + "/victim_models/SWIRL_2022/swirl")

import gym_db  # noqa: F401

from gym_db.common import EnvironmentType
from swirl.experiment import Experiment
from stable_baselines.common.callbacks import EvalCallbackWithTBRunningAverage
from stable_baselines.common.vec_env import DummyVecEnv, SubprocVecEnv, VecNormalize

from generation.Gen_SWIRL import gen_workload_SWIRL
from generation.Gen_SWIRL import gen_columns_SWIRL
from generation.Gen_SWIRL import gen_probingv3
from generation.Gen_SWIRL import gen_attack_bad_suboptimal,gen_attack_bad,gen_attack_suboptimal,gen_attack_random_ood,gen_attack_not_ood

from workload_generation.BartSqlGen.model import GenerationTask


class SWIRL_2022(object):
    def __init__(self, configuration_file):
        self.root_config = configuration_file
        self.base_configuration_file = configuration_file
        if self.base_configuration_file["dataset"].split("1")[0] == "tpch":
            CONFIGURATION_FILE = json.load(open(sys.argv[1]))["SWIRL_2022"]["model"][0]
        if self.base_configuration_file["dataset"].split("1")[0] == "tpcds":
            CONFIGURATION_FILE = json.load(open(sys.argv[1]))["SWIRL_2022"]["model"][1]
        self.experiment = Experiment(CONFIGURATION_FILE, json.load(open(sys.argv[1]))["experiments_id"] + "/before_poison")
        self.init_model_path = self.experiment.experiment_folder_path
        self.probing_config = json.load(open(sys.argv[1]))["SWIRL_2022"]["probing"]
        self.old_sql = []
        self.original_workload = []
        self.init_reward = 1
        self.indexes_now = []
        self.reward_now = 1
        self.zero = 0
       
    def prepare(self):
        self.algorithm_class = getattr(
            importlib.import_module("stable_baselines"), self.experiment.config["rl_algorithm"]["algorithm"]
        )
           
        self.experiment.prepare()

        ParallelEnv = SubprocVecEnv if self.experiment.config["parallel_environments"] > 1 else DummyVecEnv

        self.training_env = ParallelEnv(
            [self.experiment.make_env(env_id) for env_id in range(self.experiment.config["parallel_environments"])]
        )
        self.training_env = VecNormalize(
            self.training_env, norm_obs=True, norm_reward=True, gamma=self.experiment.config["rl_algorithm"]["gamma"], training=True
        )
        self.experiment.model_type = self.algorithm_class
        self._get_columns()

        with open(f"{self.experiment.experiment_folder_path}/experiment_object.pickle", "wb") as handle:
            pickle.dump(self.experiment, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
    def train(self):
        self.model = self.algorithm_class(
            policy=self.experiment.config["rl_algorithm"]["policy"],
            env=self.training_env,
            verbose=2,
            seed=self.experiment.config["random_seed"],
            gamma=self.experiment.config["rl_algorithm"]["gamma"],
            tensorboard_log="tensor_log",
            policy_kwargs=copy.copy(
                self.experiment.config["rl_algorithm"]["model_architecture"]
            ),  # This is necessary because SB modifies the passed dict.
            **self.experiment.config["rl_algorithm"]["args"],
        )
        logging.warning(f"Creating model with NN architecture: {self.experiment.config['rl_algorithm']['model_architecture']}")

        self.experiment.set_model(self.model)

        callback_test_env = VecNormalize(
            DummyVecEnv([self.experiment.make_env(0, EnvironmentType.TESTING)]),
            norm_obs=True,
            norm_reward=False,
            gamma=self.experiment.config["rl_algorithm"]["gamma"],
            training=False,
        )
        test_callback = EvalCallbackWithTBRunningAverage(
            n_eval_episodes=self.experiment.config["workload"]["validation_testing"]["number_of_workloads"],
            eval_freq=round(self.experiment.config["validation_frequency"] / self.experiment.config["parallel_environments"]),
            eval_env=callback_test_env,
            verbose=1,
            name="test",
            deterministic=True,
            comparison_performances=self.experiment.comparison_performances["test"],
        )

        callback_validation_env = VecNormalize(
            DummyVecEnv([self.experiment.make_env(0, EnvironmentType.VALIDATION)]),
            norm_obs=True,
            norm_reward=False,
            gamma=self.experiment.config["rl_algorithm"]["gamma"],
            training=False,
        )
        self.validation_callback = EvalCallbackWithTBRunningAverage(
            n_eval_episodes=self.experiment.config["workload"]["validation_testing"]["number_of_workloads"],
            eval_freq=round(self.experiment.config["validation_frequency"] / self.experiment.config["parallel_environments"]),
            eval_env=callback_validation_env,
            best_model_save_path=self.experiment.experiment_folder_path,
            verbose=1,
            name="validation",
            deterministic=True,
            comparison_performances=self.experiment.comparison_performances["validation"],
        )
        self.callbacks = [self.validation_callback, test_callback]

        if len(self.experiment.multi_validation_wl) > 0:
            callback_multi_validation_env = VecNormalize(
                DummyVecEnv([self.experiment.make_env(0, EnvironmentType.VALIDATION, experiment.multi_validation_wl)]),
                norm_obs=True,
                norm_reward=False,
                gamma=self.experiment.config["rl_algorithm"]["gamma"],
                training=False,
            )
            multi_validation_callback = EvalCallbackWithTBRunningAverage(
                n_eval_episodes=len(self.experiment.multi_validation_wl),
                eval_freq=round(self.experiment.config["validation_frequency"] / experiment.config["parallel_environments"]),
                eval_env=callback_multi_validation_env,
                best_model_save_path=self.experiment.experiment_folder_path,
                verbose=1,
                name="multi_validation",
                deterministic=True,
                comparison_performances={},
            )
            self.callbacks.append(multi_validation_callback)

        self.experiment.start_learning()
        self.model.learn(
            total_timesteps=self.experiment.config["timesteps"],
            callback=self.callbacks,
            tb_log_name=self.experiment.id,
        )
        self.init_rewards = self.experiment.finish_learning(
            self.training_env,
            self.validation_callback.moving_average_step * self.experiment.config["parallel_environments"],
            self.validation_callback.best_model_step * self.experiment.config["parallel_environments"],
        )

        self.experiment.finish()
        indexes, reward_now = self.experiment.pretrain(self.experiment.workload_generator.query_texts)
        self.original_workload = self.experiment.workload_generator.query_texts
        indexes_now = []
        for index in indexes:
            if index not in indexes_now:
                indexes_now.append(index)
        print(reward_now)
        print(indexes_now)
        self.indexes_before_poison = self.indexes_now = indexes_now
        self.reward_before_poison = reward_now


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

    def probing(self):
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
            indexes, self.reward_now = self.experiment.pretrain(probing_workload)
            indexes_now = []
            for index in indexes:
                if index[0] not in indexes_now:
                    indexes_now.append(index[0])
            self.indexes_now = indexes_now
            print(self.reward_now)
            print(self.indexes_now)
            self.init_reward = 0

        probility_of_column = sorted(self.probility_of_column.items(), key=lambda x: x[1])

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
            attack_workload = gen_attack_not_ood()

        if self.base_configuration_file["dataset"].split("1")[0] == "tpch":
            CONFIGURATION_FILE = json.load(open(sys.argv[1]))["SWIRL_2022"]["model"][0]
        if self.base_configuration_file["dataset"].split("1")[0] == "tpcds":
            CONFIGURATION_FILE = json.load(open(sys.argv[1]))["SWIRL_2022"]["model"][1]
        self.experiment = Experiment(CONFIGURATION_FILE, json.load(open(sys.argv[1]))["experiments_id"] + "/after_poison")
        self.algorithm_class = getattr(
            importlib.import_module("stable_baselines"), self.experiment.config["rl_algorithm"]["algorithm"]
        )

        self.experiment.prepare_v2(attack_workload)

        ParallelEnv = SubprocVecEnv if self.experiment.config["parallel_environments"] > 1 else DummyVecEnv

        self.training_env = ParallelEnv(
            [self.experiment.make_env(env_id) for env_id in range(self.experiment.config["parallel_environments"])]
        )
        self.training_env = VecNormalize(
            self.training_env, norm_obs=True, norm_reward=True, gamma=self.experiment.config["rl_algorithm"]["gamma"],
            training=True
        )

        self.experiment.model_type = self.algorithm_class

        with open(f"{self.experiment.experiment_folder_path}/experiment_object.pickle", "wb") as handle:
            pickle.dump(self.experiment, handle, protocol=pickle.HIGHEST_PROTOCOL)

        # self.model = self.experiment.model_type.load(f"{self.init_model_path}/best_mean_reward_model.zip")
        self.model = self.algorithm_class(
            policy=self.experiment.config["rl_algorithm"]["policy"],
            env=self.training_env,
            verbose=2,
            seed=self.experiment.config["random_seed"],
            gamma=self.experiment.config["rl_algorithm"]["gamma"],
            tensorboard_log="tensor_log",
            path=self.init_model_path,
            is_load=True,
            policy_kwargs=copy.copy(
                self.experiment.config["rl_algorithm"]["model_architecture"],
            ),  # This is necessary because SB modifies the passed dict.
            **self.experiment.config["rl_algorithm"]["args"],

        )
        logging.warning(
            f"Creating model with NN architecture: {self.experiment.config['rl_algorithm']['model_architecture']}")

        self.experiment.set_model(self.model)

        callback_test_env = VecNormalize(
            DummyVecEnv([self.experiment.make_env(0, EnvironmentType.TESTING)]),
            norm_obs=True,
            norm_reward=False,
            gamma=self.experiment.config["rl_algorithm"]["gamma"],
            training=False,
        )
        test_callback = EvalCallbackWithTBRunningAverage(
            n_eval_episodes=self.experiment.config["workload"]["validation_testing"]["number_of_workloads"],
            eval_freq=round(
                self.experiment.config["validation_frequency"] / self.experiment.config["parallel_environments"]),
            eval_env=callback_test_env,
            verbose=1,
            name="test",
            deterministic=True,
            comparison_performances=self.experiment.comparison_performances["test"],
        )

        callback_validation_env = VecNormalize(
            DummyVecEnv([self.experiment.make_env(0, EnvironmentType.VALIDATION)]),
            norm_obs=True,
            norm_reward=False,
            gamma=self.experiment.config["rl_algorithm"]["gamma"],
            training=False,
        )
        self.validation_callback = EvalCallbackWithTBRunningAverage(
            n_eval_episodes=self.experiment.config["workload"]["validation_testing"]["number_of_workloads"],
            eval_freq=round(
                self.experiment.config["validation_frequency"] / self.experiment.config["parallel_environments"]),
            eval_env=callback_validation_env,
            best_model_save_path=self.experiment.experiment_folder_path,
            verbose=1,
            name="validation",
            deterministic=True,
            comparison_performances=self.experiment.comparison_performances["validation"],
        )
        self.callbacks = [self.validation_callback, test_callback]

        if len(self.experiment.multi_validation_wl) > 0:
            callback_multi_validation_env = VecNormalize(
                DummyVecEnv([self.experiment.make_env(0, EnvironmentType.VALIDATION, experiment.multi_validation_wl)]),
                norm_obs=True,
                norm_reward=False,
                gamma=self.experiment.config["rl_algorithm"]["gamma"],
                training=False,
            )
            multi_validation_callback = EvalCallbackWithTBRunningAverage(
                n_eval_episodes=len(self.experiment.multi_validation_wl),
                eval_freq=round(
                    self.experiment.config["validation_frequency"] / experiment.config["parallel_environments"]),
                eval_env=callback_multi_validation_env,
                best_model_save_path=self.experiment.experiment_folder_path,
                verbose=1,
                name="multi_validation",
                deterministic=True,
                comparison_performances={},
            )
            self.callbacks.append(multi_validation_callback)

        self.experiment.start_learning()
        self.model.learn(
            total_timesteps=self.experiment.config["timesteps"],
            callback=self.callbacks,
            tb_log_name=self.experiment.id,
        )
        self.cur_rewards = self.experiment.finish_learning(
            self.training_env,
            self.validation_callback.moving_average_step * self.experiment.config["parallel_environments"],
            self.validation_callback.best_model_step * self.experiment.config["parallel_environments"],
        )

        self.experiment.finish()
        indexes, reward_now = self.experiment.pretrain(self.original_workload)
        indexes_now = []
        for index in indexes:
            if index[0] not in indexes_now:
                indexes_now.append(index[0])
        print(reward_now)
        print(indexes_now)
        self.indexes_after_poison = indexes_now
        self.reward_after_poison = reward_now
        
        self.cur_rewards[0:312]
        length = min(len(self.cur_rewards), len(self.init_rewards))
        best_reward_bias = max(self.cur_rewards) - max(self.init_rewards)
        avg_reward_bias = (sum(self.cur_rewards[-length:]) - sum(self.init_rewards[-length:])) / length

        after = np.array(self.cur_rewards[-length:])
        before = np.array(self.init_rewards[-length:])

        after_variance = np.var(after)
        before_variance = np.var(before)

        vmf = after_variance / before_variance

        best_cost_bias = best_reward_bias / self.reward_before_poison
        avg_cost_bias = avg_reward_bias / self.reward_before_poison

        return best_reward_bias, avg_reward_bias, vmf, best_cost_bias, avg_cost_bias

    def _get_columns(self):
        probility_of_column = {}
        for i in self.experiment.globally_indexable_columns[0]:
            i_2 = str(i[0]).split("C ")[1].replace(".", "#")
            one_dic = {i_2: 0}
            probility_of_column.update(one_dic)
        for key, value in probility_of_column.items():
            probility_of_column[key] = 1.0 / len(probility_of_column)
        self.probility_of_column = probility_of_column
    
    def evaluation(self):
        logging.info("=============== Before Poison ===============")
        logging.info("indexes before poison: " + str(self.indexes_before_poison))
        logging.info("reward before poison: " + str(self.reward_before_poison))
        logging.info("\n\n")
        logging.info("=============== After Poison ===============")
        logging.info("indexes after poison: " + str(self.indexes_after_poison))
        logging.info("reward after poison: " + str(self.reward_after_poison))

        