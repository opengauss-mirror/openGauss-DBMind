import numpy as np
from stable_baselines.common.vec_env import VecEnv
import datetime
from torch import Tensor
import pickle
import os
import copy


def evaluate_policy(model, env, n_eval_episodes=10, deterministic=True,
                    render=False, callback=None, reward_threshold=None,
                    return_episode_rewards=False, myname="", epi=0):
    """
    Runs policy for `n_eval_episodes` episodes and returns average reward.
    This is made to work only with one env.

    :param model: (BaseRLModel) The RL agent you want to evaluate.
    :param env: (gym.Env or VecEnv) The gym environment. In the case of a `VecEnv`
        this must contain only one environment.
    :param n_eval_episodes: (int) Number of episode to evaluate the agent
    :param deterministic: (bool) Whether to use deterministic or stochastic actions
    :param render: (bool) Whether to render the environment or not
    :param callback: (callable) callback function to do additional checks,
        called after each step.
    :param reward_threshold: (float) Minimum expected reward per episode,
        this will raise an error if the performance is not met
    :param return_episode_rewards: (bool) If True, a list of reward per episode
        will be returned instead of the mean.
    :return: (float, float) Mean reward per episode, std of reward per episode
        returns ([float], [int]) when `return_episode_rewards` is True
    """
    if isinstance(env, VecEnv):
        assert env.num_envs == 1, "You must pass only one environment when using this function"

    episode_rewards, episode_lengths = [], []
    summ = datetime.timedelta(0)
    for _ in range(n_eval_episodes):
        obs = env.reset()
        done, state = False, None
        episode_reward = 0.0
        episode_length = 0
        action_mask = None
        while not done:
            tete = datetime.datetime.now()
            action_mask = [env.get_attr("valid_actions")[0]]
            action, state = model.predict(
                obs, state=state, deterministic=deterministic, action_mask=action_mask)
            if epi > 3000:
                ep = True
            else:
                ep = False
            obs, reward, done, _info = env.step(action, start=ep)
            episode_reward += reward
            if callback is not None:
                callback(locals(), globals())
            episode_length += 1
            if render:
                env.render()
        tete_sum = datetime.datetime.now() - tete
        summ = summ + tete_sum
        episode_rewards.append(episode_reward)
        episode_lengths.append(episode_length)
    print("for ten times: ")
    print(summ.microseconds)
    print("for one times avg: ")
    print(summ.microseconds/10)
    mean_reward = np.mean(episode_rewards)
    std_reward = np.std(episode_rewards)
    if reward_threshold is not None:
        assert mean_reward > reward_threshold, 'Mean reward below threshold: '\
            '{:.2f} < {:.2f}'.format(mean_reward, reward_threshold)
    if return_episode_rewards:
        return episode_rewards, episode_lengths

    return mean_reward, std_reward


def evaluate_policymy(network, env, n_eval_episodes=10, deterministic=True,
                      render=False, callback=None, reward_threshold=None,
                      return_episode_rewards=False, device=None):
    """
    Runs policy for `n_eval_episodes` episodes and returns average reward.
    This is made to work only with one env.

    :param model: (BaseRLModel) The RL agent you want to evaluate.
    :param env: (gym.Env or VecEnv) The gym environment. In the case of a `VecEnv`
        this must contain only one environment.
    :param n_eval_episodes: (int) Number of episode to evaluate the agent
    :param deterministic: (bool) Whether to use deterministic or stochastic actions
    :param render: (bool) Whether to render the environment or not
    :param callback: (callable) callback function to do additional checks,
        called after each step.
    :param reward_threshold: (float) Minimum expected reward per episode,
        this will raise an error if the performance is not met
    :param return_episode_rewards: (bool) If True, a list of reward per episode
        will be returned instead of the mean.
    :return: (float, float) Mean reward per episode, std of reward per episode
        returns ([float], [int]) when `return_episode_rewards` is True
    """
    if isinstance(env, VecEnv):
        assert env.num_envs == 1, "You must pass only one environment when using this function"

    episode_rewards, episode_lengths = [], []
    for _ in range(n_eval_episodes):
        state = env.reset()
        done = False
        episode_reward = 0.0
        episode_length = 0
        while not done:
            action_mean, value = network(
                Tensor(state).unsqueeze(0).to(device=device))

            action_mask = copy.copy(env.envs[0].action_manager.valid_actions)
            action, logproba = network.select_action(
                action_mean, action_mask=action_mask)
            action = action.data.cpu().numpy()[0][0]
            logproba = logproba.data.cpu().numpy()[0][0]
            state, reward, done, _ = env.step(action)

            episode_reward += reward
            if callback is not None:
                callback(locals(), globals())
            episode_length += 1
            if render:
                env.render()
        episode_rewards.append(episode_reward)
        episode_lengths.append(episode_length)

    mean_reward = np.mean(episode_rewards)
    std_reward = np.std(episode_rewards)

    if reward_threshold is not None:
        assert mean_reward > reward_threshold, 'Mean reward below threshold: '\
            '{:.2f} < {:.2f}'.format(mean_reward, reward_threshold)
    if return_episode_rewards:
        return episode_rewards, episode_lengths
    return mean_reward, std_reward


def evaluate_policy_Ptf(network, env, n_eval_episodes=10, deterministic=True,
                        render=False, callback=None, reward_threshold=None,
                        return_episode_rewards=False, device=None):
    """
    Runs policy for `n_eval_episodes` episodes and returns average reward.
    This is made to work only with one env.

    :param model: (BaseRLModel) The RL agent you want to evaluate.
    :param env: (gym.Env or VecEnv) The gym environment. In the case of a `VecEnv`
        this must contain only one environment.
    :param n_eval_episodes: (int) Number of episode to evaluate the agent
    :param deterministic: (bool) Whether to use deterministic or stochastic actions
    :param render: (bool) Whether to render the environment or not
    :param callback: (callable) callback function to do additional checks,
        called after each step.
    :param reward_threshold: (float) Minimum expected reward per episode,
        this will raise an error if the performance is not met
    :param return_episode_rewards: (bool) If True, a list of reward per episode
        will be returned instead of the mean.
    :return: (float, float) Mean reward per episode, std of reward per episode
        returns ([float], [int]) when `return_episode_rewards` is True
    """
    if isinstance(env, VecEnv):
        assert env.num_envs == 1, "You must pass only one environment when using this function"

    episode_rewards, episode_lengths = [], []
    for _ in range(n_eval_episodes):
        state = env.reset()
        done = False
        episode_reward = 0.0
        episode_length = 0
        while not done:

            action_mask = copy.copy(env.envs[0].action_manager.valid_actions)
            act, v_pred, koko = network.choose_action(state, action_mask)

            state, reward, done, _ = env.step([act])

            episode_reward += reward
            if callback is not None:
                callback(locals(), globals())
            episode_length += 1
            if render:
                env.render()
        episode_rewards.append(episode_reward)
        episode_lengths.append(episode_length)

    mean_reward = np.mean(episode_rewards)
    std_reward = np.std(episode_rewards)

    if reward_threshold is not None:
        assert mean_reward > reward_threshold, 'Mean reward below threshold: '\
            '{:.2f} < {:.2f}'.format(mean_reward, reward_threshold)
    if return_episode_rewards:
        return episode_rewards, episode_lengths
    return mean_reward, std_reward


def evaluate_policy_Ptf_PPO(network, env, n_eval_episodes=10, deterministic=True,
                            render=False, callback=None, reward_threshold=None,
                            return_episode_rewards=False, device=None):
    """
    Runs policy for `n_eval_episodes` episodes and returns average reward.
    This is made to work only with one env.

    :param model: (BaseRLModel) The RL agent you want to evaluate.
    :param env: (gym.Env or VecEnv) The gym environment. In the case of a `VecEnv`
        this must contain only one environment.
    :param n_eval_episodes: (int) Number of episode to evaluate the agent
    :param deterministic: (bool) Whether to use deterministic or stochastic actions
    :param render: (bool) Whether to render the environment or not
    :param callback: (callable) callback function to do additional checks,
        called after each step.
    :param reward_threshold: (float) Minimum expected reward per episode,
        this will raise an error if the performance is not met
    :param return_episode_rewards: (bool) If True, a list of reward per episode
        will be returned instead of the mean.
    :return: (float, float) Mean reward per episode, std of reward per episode
        returns ([float], [int]) when `return_episode_rewards` is True
    """
    if isinstance(env, VecEnv):
        assert env.num_envs == 1, "You must pass only one environment when using this function"

    episode_rewards, episode_lengths = [], []
    for _ in range(n_eval_episodes):
        state = env.reset()
        done = False
        episode_reward = 0.0
        episode_length = 0
        while not done:

            action_mask = copy.copy(env.envs[0].action_manager.valid_actions)

            act = network.PPO.choose_action(state[0], action_mask)

            state, reward, done, _ = env.step([act])

            episode_reward += reward
            if callback is not None:
                callback(locals(), globals())
            episode_length += 1
            if render:
                env.render()
        episode_rewards.append(episode_reward)
        episode_lengths.append(episode_length)

    mean_reward = np.mean(episode_rewards)
    std_reward = np.std(episode_rewards)

    if reward_threshold is not None:
        assert mean_reward > reward_threshold, 'Mean reward below threshold: '\
            '{:.2f} < {:.2f}'.format(mean_reward, reward_threshold)
    if return_episode_rewards:
        return episode_rewards, episode_lengths
    return mean_reward, std_reward
