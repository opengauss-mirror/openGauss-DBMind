import copy
import importlib
import logging
import pickle
import sys
import gym_db
from gym_db.common import EnvironmentType
from balance.experiment import Experiment
import os
from stable_baselines.common.callbacks import EvalCallbackWithTBRunningAverage
from stable_baselines.common.vec_env import DummyVecEnv, SubprocVecEnv, VecNormalize
from stable_baselines.ppo2 import ppo2, ppo2_BALANCE
use_gpu = "0"
os.environ["CUDA_VISIBLE_DEVICES"] = use_gpu

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    CONFIGURATION_FILE = "experiments/tpch.json"

    logging.warning("use gpu:" + use_gpu)
    experiment = Experiment(CONFIGURATION_FILE)

    algorithm_class = ppo2_BALANCE.PPO2
    source_algorithm_class = ppo2_BALANCE.PPO2

    experiment.prepare()
    with open(f"{experiment.experiment_folder_path}/experiment_object.pickle",
              "wb") as handle:
        pickle.dump(experiment, handle, protocol=pickle.HIGHEST_PROTOCOL)
    ParallelEnv = SubprocVecEnv if experiment.config[
        "parallel_environments"] > 1 else DummyVecEnv

    training_env = ParallelEnv([
        experiment.make_env(env_id)
        for env_id in range(experiment.config["parallel_environments"])
    ])
    training_env = VecNormalize(
        training_env,
        norm_obs=True,
        norm_reward=True,
        gamma=experiment.config["rl_algorithm"]["gamma"],
        training=True)
    temac = []
    experiment.source_model_type = source_algorithm_class
    experiment.model_type = algorithm_class

    path1 = "./experiment_utils/source"
    path2 = "./experiment_utils/source"
    path3 = "./experiment_utils/source"

    experiment.Smodel_1 = experiment.source_model_type.load(path1 +
                                                            "/f_s1.zip")
    experiment.Smodel_1.training = False
    experiment.Smodel_2 = experiment.source_model_type.load(path2 +
                                                            "/f_s2.zip")
    experiment.Smodel_2.training = False
    experiment.Smodel_3 = experiment.source_model_type.load(path3 +
                                                            "/f_s3.zip")
    experiment.Smodel_3.training = False

    temac.append(experiment.Smodel_1)
    temac.append(experiment.Smodel_2)
    temac.append(experiment.Smodel_3)

    model = algorithm_class(
        policy=experiment.config["rl_algorithm"]["policy"],
        env=training_env,
        verbose=2,
        seed=experiment.config["random_seed"],
        gamma=experiment.config["rl_algorithm"]["gamma"],
        tensorboard_log="tensor_log",
        acc=temac,
        policy_kwargs=copy.copy(
            experiment.config["rl_algorithm"]["model_architecture"]
        ),
        **experiment.config["rl_algorithm"]["args"],
    )
    logging.warning(
        f"Creating model with NN architecture: {experiment.config['rl_algorithm']['model_architecture']}"
    )

    experiment.set_model(model)

    callback_test_env = VecNormalize(
        DummyVecEnv([experiment.make_env(0, EnvironmentType.TESTING)]),
        norm_obs=True,
        norm_reward=False,
        gamma=experiment.config["rl_algorithm"]["gamma"],
        training=False,
    )
    test_callback = EvalCallbackWithTBRunningAverage(
        n_eval_episodes=experiment.config["workload"]["validation_testing"]
        ["number_of_workloads"],
        eval_freq=round(experiment.config["validation_frequency"] /
                        experiment.config["parallel_environments"]),
        eval_env=callback_test_env,
        verbose=1,
        name="test",
        deterministic=True,
        comparison_performances=experiment.comparison_performances["test"],
    )

    callback_validation_env = VecNormalize(
        DummyVecEnv([experiment.make_env(0, EnvironmentType.VALIDATION)]),
        norm_obs=True,
        norm_reward=False,
        gamma=experiment.config["rl_algorithm"]["gamma"],
        training=False,
    )
    validation_callback = EvalCallbackWithTBRunningAverage(
        n_eval_episodes=experiment.config["workload"]["validation_testing"]
        ["number_of_workloads"],
        eval_freq=round(experiment.config["validation_frequency"] /
                        experiment.config["parallel_environments"]),
        eval_env=callback_validation_env,
        best_model_save_path=experiment.experiment_folder_path,
        verbose=1,
        name="validation",
        deterministic=True,
        comparison_performances=experiment.
        comparison_performances["validation"],
    )
    callbacks = [validation_callback, test_callback]

    if len(experiment.multi_validation_wl) > 0:
        callback_multi_validation_env = VecNormalize(
            DummyVecEnv([
                experiment.make_env(0, EnvironmentType.VALIDATION,
                                    experiment.multi_validation_wl)
            ]),
            norm_obs=True,
            norm_reward=False,
            gamma=experiment.config["rl_algorithm"]["gamma"],
            training=False,
        )
        multi_validation_callback = EvalCallbackWithTBRunningAverage(
            n_eval_episodes=len(experiment.multi_validation_wl),
            eval_freq=round(experiment.config["validation_frequency"] /
                            experiment.config["parallel_environments"]),
            eval_env=callback_multi_validation_env,
            best_model_save_path=experiment.experiment_folder_path,
            verbose=1,
            name="multi_validation",
            deterministic=True,
            comparison_performances={},
        )
        callbacks.append(multi_validation_callback)

    experiment.start_learning()

    model.learn(total_timesteps=experiment.config["timesteps"],
                callback=callbacks,
                tb_log_name=experiment.id,
                ids=experiment.config["id"])
    experiment.finish_learning(
        training_env,
        validation_callback.moving_average_step *
        experiment.config["parallel_environments"],
        validation_callback.best_model_step *
        experiment.config["parallel_environments"],
    )

    with open(f"{experiment.experiment_folder_path}/workload_dic.pickle",
              "wb") as handle:
        pickle.dump([
            training_env.venv.envs[0].dic,
            callbacks[0].eval_env.venv.envs[0].dic,
            callbacks[1].eval_env.venv.envs[0].dic
        ],
            handle,
            protocol=pickle.HIGHEST_PROTOCOL)
    experiment.finishmy()

    print()
