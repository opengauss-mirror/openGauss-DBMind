import copy
import importlib
import logging
import pickle
import sys
from shutil import copyfile
# import matplotlib.pyplot as plt

import gym_db  # noqa: F401
from gym_db.common import EnvironmentType

from .experiment import Experiment

import os
os.environ["CUDA_VISIBLE_DEVICES"] = "4"

if __name__ == "__main__":
    
    # 启动log
    logging.basicConfig(level=logging.INFO)

    # plt.figure(1)
    # x = [1,2]
    # y2 = [1,2]
    # plt.plot(x, y2, marker='x')
    # plt.savefig("./result/rewardfreq.png", dpi=120)
    
    # 校验导入参数是否正确，并导入参数路径
    assert len(sys.argv) == 2, "Experiment configuration file must be provided: main.py path_fo_file.json"
    CONFIGURATION_FILE = sys.argv[1]

    # 跳转至swirl/experiment文件夹，进行实验初始化设置（包括导入对应版本的包，设置实验初始参数等）
    experiment = Experiment(CONFIGURATION_FILE)

    if experiment.config["rl_algorithm"]["stable_baselines_version"] == 2:
        from stable_baselines.common.callbacks import EvalCallbackWithTBRunningAverage
        from stable_baselines.common.vec_env import DummyVecEnv, SubprocVecEnv, VecNormalize

        algorithm_class = getattr(
            importlib.import_module("stable_baselines"), experiment.config["rl_algorithm"]["algorithm"]
        )
    elif experiment.config["rl_algorithm"]["stable_baselines_version"] == 3:
        from stable_baselines.common.callbacks import EvalCallbackWithTBRunningAverage
        from stable_baselines.common.vec_env import DummyVecEnv, SubprocVecEnv, VecNormalize

        algorithm_class = getattr(
            importlib.import_module("stable_baselines3"), experiment.config["rl_algorithm"]["algorithm"]
        )
    else:
        raise ValueError

    # 实验准备，进入prepare函数
    experiment.prepare()
    
    # 训练环境初始化
    ParallelEnv = SubprocVecEnv if experiment.config["parallel_environments"] > 1 else DummyVecEnv

    training_env = ParallelEnv(
        [experiment.make_env(env_id) for env_id in range(experiment.config["parallel_environments"])]
    )
    training_env = VecNormalize(
        training_env, norm_obs=True, norm_reward=True, gamma=experiment.config["rl_algorithm"]["gamma"], training=True
    )

    experiment.model_type = algorithm_class

    with open(f"{experiment.experiment_folder_path}/experiment_object.pickle", "wb") as handle:
        pickle.dump(experiment, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # 初始化模型
    model = algorithm_class(
        policy=experiment.config["rl_algorithm"]["policy"],
        env=training_env,
        verbose=2,
        seed=experiment.config["random_seed"],
        gamma=experiment.config["rl_algorithm"]["gamma"],
        tensorboard_log="tensor_log",
        policy_kwargs=copy.copy(
            experiment.config["rl_algorithm"]["model_architecture"]
        ),  # This is necessary because SB modifies the passed dict. 策略
        **experiment.config["rl_algorithm"]["args"],
    )
    
    logging.warning(f"Creating model with NN architecture: {experiment.config['rl_algorithm']['model_architecture']}")

    # 设置模型
    experiment.set_model(model)
    # 选择对比的模型
    experiment.compare()

    # callback 环境初始化
    callback_test_env = VecNormalize(
        DummyVecEnv([experiment.make_env(0, EnvironmentType.TESTING)]),
        norm_obs=True,
        norm_reward=False,
        gamma=experiment.config["rl_algorithm"]["gamma"],
        training=False,
    )
    # 评价结果callback
    test_callback = EvalCallbackWithTBRunningAverage(
        n_eval_episodes=experiment.config["workload"]["validation_testing"]["number_of_workloads"],
        eval_freq=round(experiment.config["validation_frequency"] / experiment.config["parallel_environments"]),
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
        n_eval_episodes=experiment.config["workload"]["validation_testing"]["number_of_workloads"],
        eval_freq=round(experiment.config["validation_frequency"] / experiment.config["parallel_environments"]),
        eval_env=callback_validation_env,
        best_model_save_path=experiment.experiment_folder_path,
        verbose=1,
        name="validation",
        deterministic=True,
        comparison_performances=experiment.comparison_performances["validation"],
    )
    callbacks = [validation_callback, test_callback]

    if len(experiment.multi_validation_wl) > 0:
        callback_multi_validation_env = VecNormalize(
            DummyVecEnv([experiment.make_env(0, EnvironmentType.VALIDATION, experiment.multi_validation_wl)]),
            norm_obs=True,
            norm_reward=False,
            gamma=experiment.config["rl_algorithm"]["gamma"],
            training=False,
        )
        multi_validation_callback = EvalCallbackWithTBRunningAverage(
            n_eval_episodes=len(experiment.multi_validation_wl),
            eval_freq=round(experiment.config["validation_frequency"] / experiment.config["parallel_environments"]),
            eval_env=callback_multi_validation_env,
            best_model_save_path=experiment.experiment_folder_path,
            verbose=1,
            name="multi_validation",
            deterministic=True,
            comparison_performances={},
        )
        callbacks.append(multi_validation_callback)

    
    experiment.start_learning()
    # 开始learn，进入ppo2的learn函数
    model.learn(
        total_timesteps=experiment.config["timesteps"],
        callback=callbacks,
        tb_log_name=experiment.id,
    )
    experiment.finish_learning(
        training_env,
        validation_callback.moving_average_step * experiment.config["parallel_environments"],
        validation_callback.best_model_step * experiment.config["parallel_environments"],
    )

    experiment.finish()

    copyfile("./experiment_results/ID_TPCH_Test_Experiment/best_mean_reward_model.zip","./result/best_mean_reward_model.zip")
    
    indexes = []
    for i in experiment.test_bm[0][0]:
        indexes.append(i["indexes"])
    rating_dic = experiment.probing(indexes)

    experiment.poison()

    model.relearn(
        total_timesteps=experiment.config["timesteps"],
        callback=callbacks,
        tb_log_name=experiment.id,
    )

    experiment.finish_learning(
        training_env,
        validation_callback.moving_average_step * experiment.config["parallel_environments"],
        validation_callback.best_model_step * experiment.config["parallel_environments"],
    )


