import time

import gym
import numpy as np
import tensorflow as tf
import datetime
from stable_baselines import logger
from stable_baselines.common import explained_variance, ActorCriticRLModel, tf_util, SetVerbosity, TensorboardWriter
from stable_baselines.common.runners import AbstractEnvRunner
from stable_baselines.common.misc_util import flatten_action_mask
from stable_baselines.common.policies import ActorCriticPolicy, RecurrentActorCriticPolicy
from stable_baselines.common.schedules import get_schedule_fn
from stable_baselines.common.tf_util import total_episode_reward_logger
from stable_baselines.common.math_util import safe_mean
from collections import deque
import random

import gym
import numpy as np
import tensorflow as tf


class PPO2(ActorCriticRLModel):
    """
    Proximal Policy Optimization algorithm (GPU version).
    Paper: https://arxiv.org/abs/1707.06347

    :param policy: (ActorCriticPolicy or str) The policy model to use (MlpPolicy, CnnPolicy, CnnLstmPolicy, ...)
    :param env: (Gym environment or str) The environment to learn from (if registered in Gym, can be str)
    :param gamma: (float) Discount factor
    :param n_steps: (int) The number of steps to run for each environment per update
        (i.e. batch size is n_steps * n_env where n_env is number of environment copies running in parallel)
    :param ent_coef: (float) Entropy coefficient for the loss calculation
    :param learning_rate: (float or callable) The learning rate, it can be a function
    :param vf_coef: (float) Value function coefficient for the loss calculation
    :param max_grad_norm: (float) The maximum value for the gradient clipping
    :param lam: (float) Factor for trade-off of bias vs variance for Generalized Advantage Estimator
    :param nminibatches: (int) Number of training minibatches per update. For recurrent policies,
        the number of environments run in parallel should be a multiple of nminibatches.
    :param noptepochs: (int) Number of epoch when optimizing the surrogate
    :param cliprange: (float or callable) Clipping parameter, it can be a function
    :param cliprange_vf: (float or callable) Clipping parameter for the value function, it can be a function.
        This is a parameter specific to the OpenAI implementation. If None is passed (default),
        then `cliprange` (that is used for the policy) will be used.
        IMPORTANT: this clipping depends on the reward scaling.
        To deactivate value function clipping (and recover the original PPO implementation),
        you have to pass a negative value (e.g. -1).
    :param verbose: (int) the verbosity level: 0 none, 1 training information, 2 tensorflow debug
    :param tensorboard_log: (str) the log location for tensorboard (if None, no logging)
    :param _init_setup_model: (bool) Whether or not to build the network at the creation of the instance
    :param policy_kwargs: (dict) additional arguments to be passed to the policy on creation
    :param full_tensorboard_log: (bool) enable additional logging when using tensorboard
        WARNING: this logging can take a lot of space quickly
    :param seed: (int) Seed for the pseudo-random generators (python, numpy, tensorflow).
        If None (default), use random seed. Note that if you want completely deterministic
        results, you must set `n_cpu_tf_sess` to 1.
    :param n_cpu_tf_sess: (int) The number of threads for TensorFlow operations
        If None, the number of cpu of the current machine will be used.
    """

    def __init__(self, policy, env, gamma=0.99, n_steps=128, ent_coef=0.01, learning_rate=2.5e-4, vf_coef=0.5,
                 max_grad_norm=0.5, lam=0.95, nminibatches=4, noptepochs=4, cliprange=0.2, cliprange_vf=None,
                 verbose=0, tensorboard_log=None, _init_setup_model=True, policy_kwargs=None,
                 full_tensorboard_log=False, seed=None, n_cpu_tf_sess=None, acc=None, lens=None):

        self.learning_rate = learning_rate
        self.cliprange = cliprange
        self.cliprange_vf = cliprange_vf
        self.n_steps = n_steps
        self.ent_coef = ent_coef
        self.vf_coef = vf_coef
        self.max_grad_norm = max_grad_norm
        self.gamma = gamma
        self.lam = lam
        self.nminibatches = nminibatches
        self.noptepochs = noptepochs
        self.tensorboard_log = tensorboard_log
        self.full_tensorboard_log = full_tensorboard_log
        self.sum_time_test = datetime.timedelta(0)

        self.action_ph = None
        self.advs_ph = None
        self.rewards_ph = None
        self.old_neglog_pac_ph = None
        self.old_vpred_ph = None
        self.learning_rate_ph = None
        self.clip_range_ph = None
        self.entropy = None
        self.vf_loss = None
        self.pg_loss = None
        self.approxkl = None
        self.clipfrac = None
        self._train = None
        self.loss_names = None
        self.train_model = None
        self.act_model = None
        self.value = None
        self.n_batch = None
        self.summary = None
        self.actor = acc
        self.lens = lens
        self.action_dim = None

        self.pla_sap = []

        n_of_10G = 3532
        n_of_1G = 3376
        self.pla_sa = [[0]*n_of_1G]*16
        for i in range(16):
            self.pla_sap.append(np.array([0]*n_of_1G))
        tf.set_random_seed(seed)
        np.random.seed(seed)
        random.seed(seed)
        # prng was removed in latest gym version
        if hasattr(gym.spaces, 'prng'):
            gym.spaces.prng.seed(seed)

        super().__init__(policy=policy, env=env, verbose=verbose, requires_vec_env=True,
                         _init_setup_model=_init_setup_model, policy_kwargs=policy_kwargs,
                         seed=seed, n_cpu_tf_sess=n_cpu_tf_sess)

        if _init_setup_model:
            self.setup_model()

    def _make_runner(self):
        return Runner(env=self.env, model=self, n_steps=self.n_steps,
                      gamma=self.gamma, lam=self.lam)

    def _get_pretrain_placeholders(self):
        policy = self.act_model
        if isinstance(self.action_space, gym.spaces.Discrete):
            return policy.obs_ph, self.action_ph, policy.policy
        return policy.obs_ph, self.action_ph, policy.deterministic_action

    def setup_model(self, loading=False):
        with SetVerbosity(self.verbose):

            assert issubclass(self.policy, ActorCriticPolicy), "Error: the input policy for the PPO2 model must be " \
                                                               "an instance of common.policies.ActorCriticPolicy."

            self.n_batch = self.n_envs * self.n_steps

            self.graph = tf.Graph()
            with self.graph.as_default():
                self.set_random_seed(self.seed)
                self.sess = tf_util.make_session(
                    num_cpu=self.n_cpu_tf_sess, graph=self.graph)

                if self.action_dim == None:
                    ad = self.env.envs[0].observation_space.shape[0]
                else:
                    ad = self.action_dim
                if self.lens == None:
                    if self.actor == None:
                        asi = 3
                    else:
                        asi = len(self.actor)
                else:
                    asi = self.lens
                self.OT = CAPS(asi, ad, self.graph, self.sess)

                n_batch_step = None
                n_batch_train = None
                if issubclass(self.policy, RecurrentActorCriticPolicy):
                    assert self.n_envs % self.nminibatches == 0, "For recurrent policies, "\
                        "the number of environments run in parallel should be a multiple of nminibatches."
                    n_batch_step = self.n_envs
                    n_batch_train = self.n_batch // self.nminibatches

                act_model = self.policy(self.sess, self.observation_space, self.action_space, self.n_envs, 1,
                                        n_batch_step, loading=loading, reuse=False, **self.policy_kwargs)
                with tf.variable_scope("train_model", reuse=True,
                                       custom_getter=tf_util.outer_scope_getter("train_model")):
                    train_model = self.policy(self.sess, self.observation_space, self.action_space,
                                              self.n_envs // self.nminibatches, self.n_steps, n_batch_train, loading=loading,
                                              reuse=True, **self.policy_kwargs)
                self.train_model = train_model
                with tf.variable_scope("loss", reuse=False):
                    self.action_ph = train_model.pdtype.sample_placeholder(
                        [None], name="action_ph")
                    self.advs_ph = tf.placeholder(
                        tf.float32, [None], name="advs_ph")
                    self.rewards_ph = tf.placeholder(
                        tf.float32, [None], name="rewards_ph")
                    self.old_neglog_pac_ph = tf.placeholder(
                        tf.float32, [None], name="old_neglog_pac_ph")
                    self.old_vpred_ph = tf.placeholder(
                        tf.float32, [None], name="old_vpred_ph")
                    self.learning_rate_ph = tf.placeholder(
                        tf.float32, [], name="learning_rate_ph")
                    self.clip_range_ph = tf.placeholder(
                        tf.float32, [], name="clip_range_ph")
                    self.s_a_prob = tf.placeholder_with_default(tf.convert_to_tensor(
                        np.zeros([1, 1]), dtype=tf.float32), shape=[None, None], name='s_a_prob')
                    self.source_workload_mask = tf.placeholder(
                        dtype=tf.float32, shape=[None, None], name='source_workload_mask')
                    self.neglogpac = train_model.proba_distribution.neglogp(
                        self.action_ph)
                    self.entropy = tf.reduce_mean(
                        train_model.proba_distribution.entropy())
                    self.vpred = train_model.value_flat

                    # Value function clipping: not present in the original PPO
                    if self.cliprange_vf is None:
                        # Default behavior (legacy from OpenAI baselines):
                        # use the same clipping as for the policy
                        self.clip_range_vf_ph = self.clip_range_ph
                        self.cliprange_vf = self.cliprange
                    elif isinstance(self.cliprange_vf, (float, int)) and self.cliprange_vf < 0:
                        # Original PPO implementation: no value function clipping
                        self.clip_range_vf_ph = None
                    else:
                        # Last possible behavior: clipping range
                        # specific to the value function
                        self.clip_range_vf_ph = tf.placeholder(
                            tf.float32, [], name="clip_range_vf_ph")

                    if self.clip_range_vf_ph is None:
                        # No clipping
                        self.vpred_clipped = train_model.value_flat
                    else:
                        # Clip the different between old and new value
                        # NOTE: this depends on the reward scaling
                        self.vpred_clipped = self.old_vpred_ph + \
                            tf.clip_by_value(train_model.value_flat - self.old_vpred_ph,
                                             - self.clip_range_vf_ph, self.clip_range_vf_ph)

                    self.vf_losses1 = tf.square(self.vpred - self.rewards_ph)
                    self.vf_losses2 = tf.square(
                        self.vpred_clipped - self.rewards_ph)
                    self.vf_loss = .5 * \
                        tf.reduce_mean(tf.maximum(
                            self.vf_losses1, self.vf_losses2))

                    self.ratio = tf.exp(
                        self.old_neglog_pac_ph - self.neglogpac)
                    pg_losses = -self.advs_ph * self.ratio
                    pg_losses2 = -self.advs_ph * tf.clip_by_value(self.ratio, 1.0 - self.clip_range_ph, 1.0 +
                                                                  self.clip_range_ph)
                    self.pg_loss = tf.reduce_mean(
                        tf.maximum(pg_losses, pg_losses2))
                    self.approxkl = .5 * \
                        tf.reduce_mean(
                            tf.square(self.neglogpac - self.old_neglog_pac_ph))
                    self.clipfrac = tf.reduce_mean(tf.cast(tf.greater(tf.abs(self.ratio - 1.0),
                                                                      self.clip_range_ph), tf.float32))

                    asl = train_model.proba_distribution.logits
                    one_hot_actions = tf.one_hot(
                        self.action_ph, asl.get_shape().as_list()[-1])
                    one_hot_actions = tf.stop_gradient(one_hot_actions)

                    # Prevent invalid actions backpropagation
                    asl = tf.multiply(asl, self.train_model.action_mask_ph)

                    # Calculate softmax and correct the invalid action probability to 0
                    softmax = tf.nn.softmax(asl)
                    exp_logits = softmax * \
                        tf.reduce_sum(tf.exp(asl), axis=-1, keepdims=True)
                    exp_logits = tf.multiply(
                        exp_logits, self.train_model.action_mask_ph)
                    self.softmax_p = exp_logits / \
                        tf.reduce_sum(exp_logits, axis=-1, keepdims=True)
                    self.softmax_p_m = tf.multiply(
                        self.softmax_p, one_hot_actions)
                    self.softmax_p_m = tf.reduce_sum(tf.multiply(
                        self.softmax_p_m, one_hot_actions), axis=-1)
                    self.e = tf.placeholder(tf.float32, (), 'e')
                    self.op_w = tf.placeholder(
                        tf.float32, shape=[None, None], name='op_w')

                    self.opops1 = [1, 1, 1]
                    self.opops2 = [1/(3-i) for i in self.opops1]

                    self.flag_wlm = False
                    self.ptf = 1

                    self.weight = tf.tanh(tf.nn.relu(9 - (0.1 * self.e)))
                    self.mysof = tf.nn.softmax(
                        self.train_model.proba_distribution.logits)

                    if self.ptf == 1:
                        self.s_a_prob_w = self.s_a_prob
                        self.s_a_prob_wp = tf.nn.softmax(self.s_a_prob_w)
                        self.tempss = tf.multiply(self.s_a_prob_wp, tf.log(
                            tf.div(self.s_a_prob_wp, self.mysof)))
                        if self.flag_wlm:
                            print("with WLM")
                            self.tempsss = self.tempss*self.source_workload_mask
                        else:
                            self.tempsss = self.tempss

                        self.myklloss = tf.reduce_sum(self.tempsss)
                        self.myklloss = self.myklloss * self.weight * 0.05 * self.ptf
                    else:
                        self.myklloss = 0

                    loss = self.pg_loss - self.entropy * self.ent_coef + \
                        self.vf_loss * self.vf_coef + self.myklloss * 0.05

                    loss_fast = self.pg_loss - self.entropy * \
                        self.ent_coef + self.vf_loss * self.vf_coef

                    tf.summary.scalar('entropy_loss', self.entropy)
                    tf.summary.scalar('policy_gradient_loss', self.pg_loss)
                    tf.summary.scalar('value_function_loss', self.vf_loss)
                    tf.summary.scalar(
                        'approximate_kullback-leibler', self.approxkl)
                    tf.summary.scalar('clip_factor', self.clipfrac)
                    tf.summary.scalar('loss', loss)
                    tf.summary.scalar('PTF_loss', self.myklloss)

                    with tf.variable_scope('model'):
                        self.params = tf.trainable_variables()
                        if self.full_tensorboard_log:
                            for var in self.params:
                                tf.summary.histogram(var.name, var)
                    grads = tf.gradients(loss, self.params)

                    grads = list(zip(grads, self.params))

                    grads_fast = tf.gradients(loss_fast, self.params)
                    grads_fast = list(zip(grads_fast, self.params))
                trainer = tf.train.AdamOptimizer(
                    learning_rate=self.learning_rate_ph, epsilon=1e-5)
                self._train = trainer.apply_gradients(grads)

                self._train_fast = trainer.apply_gradients(grads_fast)

                self.loss_names = ['policy_loss', 'value_loss',
                                   'policy_entropy', 'approxkl', 'clipfrac', 'entropyTeach']

                with tf.variable_scope("input_info", reuse=False):
                    tf.summary.scalar('discounted_rewards',
                                      tf.reduce_mean(self.rewards_ph))
                    tf.summary.scalar(
                        'learning_rate', tf.reduce_mean(self.learning_rate_ph))
                    tf.summary.scalar(
                        'advantage', tf.reduce_mean(self.advs_ph))
                    tf.summary.scalar(
                        'clip_range', tf.reduce_mean(self.clip_range_ph))
                    if self.clip_range_vf_ph is not None:
                        tf.summary.scalar(
                            'clip_range_vf', tf.reduce_mean(self.clip_range_vf_ph))

                    tf.summary.scalar(
                        'old_neglog_action_probability', tf.reduce_mean(self.old_neglog_pac_ph))
                    tf.summary.scalar('old_value_pred',
                                      tf.reduce_mean(self.old_vpred_ph))

                    if self.full_tensorboard_log:
                        tf.summary.histogram(
                            'discounted_rewards', self.rewards_ph)
                        tf.summary.histogram(
                            'learning_rate', self.learning_rate_ph)
                        tf.summary.histogram('advantage', self.advs_ph)
                        tf.summary.histogram('clip_range', self.clip_range_ph)
                        tf.summary.histogram(
                            'old_neglog_action_probability', self.old_neglog_pac_ph)
                        tf.summary.histogram(
                            'old_value_pred', self.old_vpred_ph)
                        if tf_util.is_image(self.observation_space):
                            tf.summary.image('observation', train_model.obs_ph)
                        else:
                            tf.summary.histogram(
                                'observation', train_model.obs_ph)

                self.train_model = train_model
                self.act_model = act_model
                self.step = act_model.step
                self.proba_step = act_model.proba_step
                self.value = act_model.value
                self.initial_state = act_model.initial_state
                tf.global_variables_initializer().run(session=self.sess)  # pylint: disable=E1101

                self.summary = tf.summary.merge_all()

                asl = self.train_model.proba_distribution.logits
                self.my_nosoftmax = asl

    def act_prob_nosoftmax(self,  obs,  masks, actions, action_masks, states=None, cliprange_vf=None):
        td_map = {self.train_model.obs_ph: obs, self.action_ph: actions,

                  self.train_model.action_mask_ph: action_masks
                  }
        if states is not None:
            td_map[self.train_model.states_ph] = states
            td_map[self.train_model.dones_ph] = masks

        if cliprange_vf is not None and cliprange_vf >= 0:
            td_map[self.clip_range_vf_ph] = cliprange_vf

        summary = self.sess.run(
            [self.my_nosoftmax],
            td_map)

        return summary

    def _train_step(self, learning_rate, cliprange, obs, returns, masks, actions, values, neglogpacs, action_masks, ops, update,
                    writer, states=None, cliprange_vf=None):
        """
        Training of PPO2 Algorithm

        :param learning_rate: (float) learning rate
        :param cliprange: (float) Clipping factor
        :param obs: (np.ndarray) The current observation of the environment
        :param returns: (np.ndarray) the rewards
        :param masks: (np.ndarray) The last masks for done episodes (used in recurent policies)
        :param actions: (np.ndarray) the actions
        :param values: (np.ndarray) the values
        :param neglogpacs: (np.ndarray) Negative Log-likelihood probability of Actions
        :param update: (int) the current step iteration
        :param writer: (TensorFlow Summary.writer) the writer for tensorboard
        :param states: (np.ndarray) For recurrent policies, the internal state of the recurrent model
        :return: policy gradient loss, value function loss, policy entropy,
                approximation of kl divergence, updated clipping range, training update operation
        :param cliprange_vf: (float) Clipping factor for the value function
        """

        if self.ptf == 1:
            source_actor_prob = []
            source_actor = []
            source_workload_mask = []
            mu = []
            sigma = []
            for i, o in enumerate(ops):
                o = int(o)
                a_prob = self.actor[o].act_prob_nosoftmax(obs, masks, actions,  action_masks,
                                                          states, cliprange_vf)[0][i]
                source_actor_prob.append(a_prob)
                source_actor.append([self.opops2[o]]*action_masks.shape[1])
            advs = returns - values
            advs = (advs - advs.mean()) / (advs.std() + 1e-8)
            td_map = {self.train_model.obs_ph: obs, self.action_ph: actions,
                      self.advs_ph: advs, self.rewards_ph: returns,
                      self.train_model.action_mask_ph: action_masks,
                      self.learning_rate_ph: learning_rate, self.clip_range_ph: cliprange,
                      self.old_neglog_pac_ph: neglogpacs, self.old_vpred_ph: values, self.s_a_prob: source_actor_prob,
                      self.e: self.nowU}
            if states is not None:
                td_map[self.train_model.states_ph] = states
                td_map[self.train_model.dones_ph] = masks

            if cliprange_vf is not None and cliprange_vf >= 0:
                td_map[self.clip_range_vf_ph] = cliprange_vf

            if states is None:
                update_fac = self.n_batch // self.nminibatches // self.noptepochs + 1
            else:
                update_fac = self.n_batch // self.nminibatches // self.noptepochs // self.n_steps + 1

            if writer is not None:
                # run loss backprop with summary, but once every 10 runs save the metadata (memory, compute time, ...)
                if self.full_tensorboard_log and (1 + update) % 10 == 0:
                    run_options = tf.RunOptions(
                        trace_level=tf.RunOptions.FULL_TRACE)
                    run_metadata = tf.RunMetadata()
                    summary, policy_loss, value_loss, policy_entropy, ets, approxkl, clipfrac, weigh, _ = self.sess.run(
                        [self.summary, self.pg_loss, self.vf_loss, self.entropy, self.myklloss,
                            self.approxkl, self.clipfrac, self.weight, self._train],
                        td_map, options=run_options, run_metadata=run_metadata)
                    writer.add_run_metadata(
                        run_metadata, 'step%d' % (update * update_fac))
                else:
                    summary, policy_loss, value_loss, policy_entropy, ets, approxkl, clipfrac, weigh, _ = self.sess.run(
                        [self.summary, self.pg_loss, self.vf_loss, self.entropy, self.myklloss,
                            self.approxkl, self.clipfrac, self.weight, self._train],
                        td_map)
                writer.add_summary(summary, (update * update_fac))
            else:
                policy_loss, value_loss, policy_entropy, ets, approxkl, clipfrac, weigh, _ = self.sess.run(
                    [self.pg_loss, self.vf_loss, self.entropy, self.myklloss, self.approxkl, self.clipfrac, self.weight, self._train], td_map)

            if (weigh < 1e-7):
                self.ptf = 0
            return policy_loss, value_loss, policy_entropy, approxkl, clipfrac, ets
        else:
            source_actor_prob = []
            source_actor = []
            source_workload_mask = []
            mu = []
            sigma = []

            advs = returns - values
            advs = (advs - advs.mean()) / (advs.std() + 1e-8)
            td_map = {self.train_model.obs_ph: obs, self.action_ph: actions,
                      self.advs_ph: advs, self.rewards_ph: returns,
                      self.train_model.action_mask_ph: action_masks,
                      self.learning_rate_ph: learning_rate, self.clip_range_ph: cliprange,
                      self.old_neglog_pac_ph: neglogpacs, self.old_vpred_ph: values, self.s_a_prob: self.pla_sap,
                      self.e: self.nowU}
            if states is not None:
                td_map[self.train_model.states_ph] = states
                td_map[self.train_model.dones_ph] = masks

            if cliprange_vf is not None and cliprange_vf >= 0:
                td_map[self.clip_range_vf_ph] = cliprange_vf

            if states is None:
                update_fac = self.n_batch // self.nminibatches // self.noptepochs + 1
            else:
                update_fac = self.n_batch // self.nminibatches // self.noptepochs // self.n_steps + 1

            if writer is not None:
                # run loss backprop with summary, but once every 10 runs save the metadata (memory, compute time, ...)
                if self.full_tensorboard_log and (1 + update) % 10 == 0:
                    run_options = tf.RunOptions(
                        trace_level=tf.RunOptions.FULL_TRACE)
                    run_metadata = tf.RunMetadata()
                    summary, policy_loss, value_loss, policy_entropy, approxkl, clipfrac, _ = self.sess.run(
                        [self.summary, self.pg_loss, self.vf_loss, self.entropy,
                            self.approxkl, self.clipfrac, self._train_fast],
                        td_map, options=run_options, run_metadata=run_metadata)
                    writer.add_run_metadata(
                        run_metadata, 'step%d' % (update * update_fac))
                else:
                    summary, policy_loss, value_loss, policy_entropy, approxkl, clipfrac, _ = self.sess.run(
                        [self.summary, self.pg_loss, self.vf_loss, self.entropy,
                            self.approxkl, self.clipfrac, self._train_fast],
                        td_map)
                writer.add_summary(summary, (update * update_fac))
            else:
                policy_loss, value_loss, policy_entropy, approxkl, clipfrac, _ = self.sess.run(
                    [self.pg_loss, self.vf_loss, self.entropy, self.approxkl, self.clipfrac, self._train_fast], td_map)

            return policy_loss, value_loss, policy_entropy, approxkl, clipfrac, 999

    def learn(self, total_timesteps, callback=None, log_interval=1, tb_log_name="PPO2",
              reset_num_timesteps=True, ids=None):
        # Transform to callable if needed
        tf.reset_default_graph()
        self.learning_rate = get_schedule_fn(self.learning_rate)
        self.cliprange = get_schedule_fn(self.cliprange)
        cliprange_vf = get_schedule_fn(self.cliprange_vf)
        time_test_sum = datetime.timedelta(0)
        sum_count_test = 0

        new_tb_log = self._init_num_timesteps(reset_num_timesteps)
        callback = self._init_callback(callback)

        with SetVerbosity(self.verbose), TensorboardWriter(self.graph, self.tensorboard_log, tb_log_name, new_tb_log) \
                as writer:
            self._setup_learn()

            t_first_start = time.time()
            n_updates = total_timesteps // self.n_batch

            callback.on_training_start(locals(), globals())

            for update in range(1, n_updates + 1):
                assert self.n_batch % self.nminibatches == 0, ("The number of minibatches (`nminibatches`) "
                                                               "is not a factor of the total number of samples "
                                                               "collected per rollout (`n_batch`), "
                                                               "some samples won't be used."
                                                               )
                batch_size = self.n_batch // self.nminibatches  # 16
                t_start = time.time()
                frac = 1.0 - (update - 1.0) / n_updates
                lr_now = self.learning_rate(frac)
                cliprange_now = self.cliprange(frac)
                cliprange_vf_now = cliprange_vf(frac)
                self.nowU = update
                callback.on_rollout_start()
                # true_reward is the reward without discount
                rollout = self.runner.run(callback)
                self.nowU = update
                t_fin_run = time.time()
                # Unpack
                obs, returns, masks, actions, values, neglogpacs, states, ep_infos, true_reward, action_masks, ops, time_test, count_test = rollout
                time_test_sum = time_test_sum + time_test
                sum_count_test = sum_count_test + count_test

                callback.on_rollout_end()

                # Early stopping due to the callback
                if not self.runner.continue_training:
                    break

                self.ep_info_buf.extend(ep_infos)
                mb_loss_vals = []
                if states is None:  # nonrecurrent version
                    update_fac = self.n_batch // self.nminibatches // self.noptepochs + 1
                    inds = np.arange(self.n_batch)

                    for epoch_num in range(self.noptepochs):
                        np.random.shuffle(inds)
                        for start in range(0, self.n_batch, batch_size):
                            timestep = self.num_timesteps // update_fac + ((self.noptepochs * self.n_batch + epoch_num *
                                                                            self.n_batch + start) // batch_size)
                            end = start + batch_size
                            mbinds = inds[start:end]
                            slices = (arr[mbinds] for arr in (
                                obs, returns, masks, actions, values, neglogpacs, action_masks, ops))
                            mb_loss_vals.append(self._train_step(lr_now, cliprange_now, *slices, writer=writer,
                                                                 update=timestep, cliprange_vf=cliprange_vf_now))
                            t_fin_train = time.time()
                else:  # recurrent version
                    update_fac = self.n_batch // self.nminibatches // self.noptepochs // self.n_steps + 1
                    assert self.n_envs % self.nminibatches == 0
                    env_indices = np.arange(self.n_envs)
                    flat_indices = np.arange(
                        self.n_envs * self.n_steps).reshape(self.n_envs, self.n_steps)
                    envs_per_batch = batch_size // self.n_steps
                    for epoch_num in range(self.noptepochs):
                        np.random.shuffle(env_indices)
                        for start in range(0, self.n_envs, envs_per_batch):
                            timestep = self.num_timesteps // update_fac + ((self.noptepochs * self.n_envs + epoch_num *
                                                                            self.n_envs + start) // envs_per_batch)
                            end = start + envs_per_batch
                            mb_env_inds = env_indices[start:end]
                            mb_flat_inds = flat_indices[mb_env_inds].ravel()
                            slices = (arr[mb_flat_inds] for arr in (
                                obs, returns, masks, actions, values, neglogpacs, action_masks))
                            mb_states = states[mb_env_inds]
                            mb_loss_vals.append(self._train_step(lr_now, cliprange_now, *slices, update=timestep,
                                                                 writer=writer, states=mb_states,
                                                                 cliprange_vf=cliprange_vf_now))

                loss_vals = np.mean(mb_loss_vals, axis=0)
                t_now = time.time()
                fps = int(self.n_batch / (t_now - t_start))

                if writer is not None:
                    total_episode_reward_logger(self.episode_reward,
                                                true_reward.reshape(
                                                    (self.n_envs, self.n_steps)),
                                                masks.reshape(
                                                    (self.n_envs, self.n_steps)),
                                                writer, self.num_timesteps)

                if self.verbose >= 1 and (update % log_interval == 0 or update == 1):
                    explained_var = explained_variance(values, returns)
                    logger.logkv("serial_timesteps", update * self.n_steps)
                    logger.logkv("n_updates", update)
                    logger.logkv("total_timesteps", self.num_timesteps)
                    logger.logkv("fps", fps)
                    logger.logkv("explained_variance", float(explained_var))
                    if len(self.ep_info_buf) > 0 and len(self.ep_info_buf[0]) > 0:
                        logger.logkv('ep_reward_mean', safe_mean(
                            [ep_info['r'] for ep_info in self.ep_info_buf]))
                        logger.logkv('ep_len_mean', safe_mean(
                            [ep_info['l'] for ep_info in self.ep_info_buf]))
                    logger.logkv('time_elapsed', t_start - t_first_start)
                    for (loss_val, loss_name) in zip(loss_vals, self.loss_names):
                        logger.logkv(loss_name, loss_val)
                    t_run = -t_start+t_fin_run
                    t_tra = - t_fin_run + t_fin_train
                    logger.logkv('time_run', t_run)
                    logger.logkv('time_train', t_tra)
                    logger.dumpkvs()

            callback.on_training_end()
            self.callback = callback
            print(" total test time:")
            print(time_test_sum)
            print(" total test count:")
            print(sum_count_test)
            return self

    def save(self, save_path, cloudpickle=False):
        data = {
            "gamma": self.gamma,
            "n_steps": self.n_steps,
            "vf_coef": self.vf_coef,
            "ent_coef": self.ent_coef,
            "max_grad_norm": self.max_grad_norm,
            "learning_rate": self.learning_rate,
            "lam": self.lam,
            "nminibatches": self.nminibatches,
            "noptepochs": self.noptepochs,
            "cliprange": self.cliprange,
            "cliprange_vf": self.cliprange_vf,
            "verbose": self.verbose,
            "policy": self.policy,
            "observation_space": self.observation_space,
            "action_space": self.action_space,
            "n_envs": self.n_envs,
            "n_cpu_tf_sess": self.n_cpu_tf_sess,
            "seed": self.seed,
            "_vectorize_action": self._vectorize_action,
            "policy_kwargs": self.policy_kwargs,
            "act_len": len(self.actor),
            "pact_len": self.env.envs[0].observation_space.shape[0]
        }

        params_to_save = self.get_parameters()

        self._save_to_file(save_path, data=data,
                           params=params_to_save, cloudpickle=cloudpickle)


class Runner(AbstractEnvRunner):
    def __init__(self, *, env, model, n_steps, gamma, lam):
        """
        A runner to learn the policy of an environment for a model

        :param env: (Gym environment) The environment to learn from
        :param model: (Model) The model to learn
        :param n_steps: (int) The number of steps to run for each environment
        :param gamma: (float) Discount factor
        :param lam: (float) Factor for trade-off of bias vs variance for Generalized Advantage Estimator
        """
        super().__init__(env=env, model=model, n_steps=n_steps)
        self.lam = lam
        self.gamma = gamma
        self.action_masks = []

    def _run(self):
        """
        Run a learning step of the model

        :return:
            - observations: (np.ndarray) the observations
            - rewards: (np.ndarray) the rewards
            - masks: (numpy bool) whether an episode is over or not
            - actions: (np.ndarray) the actions
            - values: (np.ndarray) the value function output
            - negative log probabilities: (np.ndarray)
            - states: (np.ndarray) the internal states of the recurrent policies
            - infos: (dict) the extra information of the model
        """
        # mb stands for minibatch
        mb_obs, mb_rewards, mb_actions, mb_values, mb_dones, mb_neglogpacs, mb_action_masks, mb_opt = [
        ], [], [], [], [], [], [], []
        mb_states = self.states
        ep_infos = []
        self.action_masks.clear()
        self.action_masks.append(flatten_action_mask(
            self.env.action_space, self.env.get_attr("valid_actions")))
        assert len(self.action_masks) == 1
        self.action_masks = self.action_masks[-1]
        sum_time_test = datetime.timedelta(0)
        sum_count_test = 0

        option = self.model.OT.choose_o(self.obs)
        termination = self.model.OT.get_t(self.obs, option)

        for koko in range(self.n_steps):
            actions, values, self.states, neglogpacs = self.model.step(
                self.obs, self.states, self.dones, action_mask=self.action_masks)
            opa = np.array(
                [1 if i == option or self.model.actor[i].step(self.obs, self.states, self.dones, action_mask=self.action_masks)[
                    0][0] == actions[0] else 0 for i in range(len(self.model.actor))]
            )

            mb_obs.append(self.obs.copy())
            mb_actions.append(actions)
            mb_values.append(values)
            mb_neglogpacs.append(neglogpacs)
            mb_dones.append(self.dones)
            mb_action_masks.append(self.action_masks.copy())
            mb_opt.append([option])
            clipped_actions = actions
            # Clip the actions to avoid out of bound error
            if isinstance(self.env.action_space, gym.spaces.Box):
                clipped_actions = np.clip(
                    actions, self.env.action_space.low, self.env.action_space.high)

            startflag = False
            self.obs[:], rewards, self.dones, infos = self.env.step(
                clipped_actions, start=startflag)

            self.model.OT.store_transition(
                mb_obs[-1], actions, rewards, self.dones, self.obs, opa)
            if (self.model.nowU > 1 and (koko % self.n_steps / 4) == 0):
                self.model.OT.update(mb_obs[-1], option, self.dones,  self.obs)
            termination = self.model.OT.get_t(self.obs, option)
            if np.random.uniform() < termination:
                option = self.model.OT.choose_o(self.obs)

            self.model.num_timesteps += self.n_envs

            if self.callback is not None:
                # Abort training early
                start_time_test = datetime.datetime.now()
                if self.callback.on_step() is False:
                    self.continue_training = False
                    # Return dummy values
                    return [None] * 10
                end_time_test = datetime.datetime.now()
                tem = end_time_test - start_time_test
                if ((tem) > datetime.timedelta(seconds=2)):
                    sum_time_test = sum_time_test + tem
                    sum_count_test = sum_count_test + 1
                    print("new sum_time_test:")
                    print(sum_time_test)

            self.action_masks.clear()
            for info in infos:
                maybe_ep_info = info.get('episode')
                if maybe_ep_info is not None:
                    ep_infos.append(maybe_ep_info)

            self.action_masks.append(flatten_action_mask(
                self.env.action_space, self.env.get_attr("valid_actions")))
            assert len(self.action_masks) == 1
            self.action_masks = self.action_masks[-1]
            mb_rewards.append(rewards)
        # batch of steps to batch of rollouts
        mb_obs = np.asarray(mb_obs, dtype=self.obs.dtype)
        mb_rewards = np.asarray(mb_rewards, dtype=np.float32)
        mb_actions = np.asarray(mb_actions)
        mb_opt = np.asarray(mb_opt)
        mb_values = np.asarray(mb_values, dtype=np.float32)
        mb_neglogpacs = np.asarray(mb_neglogpacs, dtype=np.float32)
        mb_dones = np.asarray(mb_dones, dtype=np.bool)
        mb_action_masks = np.asfarray(mb_action_masks, dtype=np.float32)
        last_values = self.model.value(self.obs, self.states, self.dones)
        # discount/bootstrap off value fn
        mb_advs = np.zeros_like(mb_rewards)
        true_reward = np.copy(mb_rewards)
        last_gae_lam = 0
        for step in reversed(range(self.n_steps)):
            if step == self.n_steps - 1:
                nextnonterminal = 1.0 - self.dones
                nextvalues = last_values
            else:
                nextnonterminal = 1.0 - mb_dones[step + 1]
                nextvalues = mb_values[step + 1]
            delta = mb_rewards[step] + self.gamma * \
                nextvalues * nextnonterminal - mb_values[step]
            mb_advs[step] = last_gae_lam = delta + self.gamma * \
                self.lam * nextnonterminal * last_gae_lam
        mb_returns = mb_advs + mb_values

        mb_obs, mb_returns, mb_dones, mb_actions, mb_values, mb_neglogpacs, true_reward, mb_action_masks, mb_opt = \
            map(swap_and_flatten, (mb_obs, mb_returns, mb_dones, mb_actions,
                mb_values, mb_neglogpacs, true_reward, mb_action_masks, mb_opt))

        return mb_obs, mb_returns, mb_dones, mb_actions, mb_values, mb_neglogpacs, mb_states, ep_infos, true_reward, mb_action_masks, mb_opt, sum_time_test, sum_count_test


# obs, returns, masks, actions, values, neglogpacs, states = runner.run()
def swap_and_flatten(arr):
    """
    swap and then flatten axes 0 and 1

    :param arr: (np.ndarray)
    :return: (np.ndarray)
    """
    shape = arr.shape
    return arr.swapaxes(0, 1).reshape(shape[0] * shape[1], *shape[2:])


class Optimizer:
    def __init__(
            self,
            optimizer,
            learning_rate,
            momentum=None
    ):
        self.opt = None
        if str(optimizer).lower() == "grad":
            self.opt = tf.train.GradientDescentOptimizer(
                learning_rate=learning_rate)
        elif str(optimizer).lower() == "momentum":
            self.opt = tf.train.MomentumOptimizer(
                learning_rate=learning_rate, momentum=momentum)
        elif str(optimizer).lower() == 'rmsprop':
            self.opt = tf.train.RMSPropOptimizer(learning_rate=learning_rate)
        elif str(optimizer).lower() == 'adam':
            self.opt = tf.train.AdamOptimizer(
                learning_rate=learning_rate, epsilon=1e-5)

    def get_optimizer(self):
        return self.opt


class ReplayBuffer(object):

    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.num_experiences = 0
        self.buffer = deque()

    def get_batch(self, batch_size):
        # Randomly sample batch_size examples
        return random.sample(self.buffer, batch_size)

    def size(self):
        return self.buffer_size

    def add(self, state, action, reward, done, new_state, opa):
        experience = (state, action, reward, done, new_state, opa)
        if self.num_experiences < self.buffer_size:
            self.buffer.append(experience)
            self.num_experiences += 1
        else:
            self.buffer.popleft()
            self.buffer.append(experience)

    def count(self):
        # if buffer is full, return buffer size
        # otherwise, return experience counter
        return self.num_experiences

    def erase(self):
        self.buffer = deque()
        self.num_experiences = 0


class CAPS:
    def __init__(self, option_dim, n_features, graph, sess=None):
        with graph.as_default():
            self.args = args = {'replace_target_iter': 1000, 'e_greedy': 0.95, 'e_greedy_increment': 0.0005, 'start_greedy': 0.0,
                                'optimizer': 'adam', 'learning_rate_o': 0.001, 'learning_rate_t': 0.001, 'memory_size': 200000, 'reward_decay': 0.99, 'clip_value': 0.2,
                                'xi': 0, 'option_batch_size': 16, 'option_layer_1': 32}
            self.option_dim = option_dim
            self.n_features = n_features

            self.update_step = 0
            self.replace_target_iter = args['replace_target_iter']
            self.e_greedy = args['e_greedy']
            self.epsilon_increment = args['e_greedy_increment']
            self.epsilon = args['start_greedy'] if args['e_greedy_increment'] != 0 else self.e_greedy

            opt0 = Optimizer(args['optimizer'], args['learning_rate_o'])
            self.Opt_O = opt0.get_optimizer()
            opt1 = Optimizer(args['optimizer'], args['learning_rate_t'])
            self.Opt_T = opt1.get_optimizer()

            self.replay_buffer = ReplayBuffer(args['memory_size'])

            with tf.variable_scope('train_input'):
                self.s = tf.placeholder(
                    tf.float32, [None, self.n_features], name='s')
                self.s_ = tf.placeholder(
                    tf.float32, [None, self.n_features], name='s_')
                self.option_o = tf.placeholder(tf.int32, [None])
                self.option_a_t = tf.placeholder(
                    tf.float32, [None, self.option_dim])
                self.reward = tf.placeholder(tf.float32, [None])
                self.done = tf.placeholder(tf.float32, [None])

            self.q_omega_current, self.term_current = self._build_net(
                'q_net', self.s)
            self.q_omega_target, self.term_target = self._build_net(
                'q_target', self.s_)
            self.q_omega_next_current, self.term_next_current = self._build_net(
                'q_net', self.s_, reuse=True)

            self.q_func_vars = tf.get_collection(
                tf.GraphKeys.GLOBAL_VARIABLES, scope='q_net')
            self.target_q_func_vars = tf.get_collection(
                tf.GraphKeys.GLOBAL_VARIABLES, scope='q_target')

            with tf.variable_scope('q_omega_value'):
                term_val_next = tf.reduce_sum(
                    self.term_next_current * tf.one_hot(self.option_o, self.option_dim), axis=-1)
                q_omega_val_next = tf.reduce_sum(
                    self.q_omega_next_current * tf.one_hot(self.option_o, self.option_dim), axis=-1)
                max_q_omega_next = tf.reduce_max(
                    self.q_omega_next_current, axis=-1)
                max_q_omega_next_targ = tf.reduce_sum(
                    self.q_omega_target * tf.one_hot(tf.argmax(self.q_omega_next_current, axis=-1), self.option_dim), axis=-1)

            with tf.variable_scope('q_omega_loss'):
                u_next_raw = (1 - self.term_next_current) * self.q_omega_target + \
                    self.term_next_current * max_q_omega_next_targ[..., None]
                u_next = tf.stop_gradient(
                    u_next_raw * (1 - self.done)[..., None])
                self.q_omega_loss = tf.reduce_mean(tf.reduce_sum(self.option_a_t
                                                                 * tf.losses.mean_squared_error(self.reward[..., None] + self.args['reward_decay'] * u_next, self.q_omega_current, reduction=tf.losses.Reduction.NONE), axis=-1))

            with tf.variable_scope('term_loss'):
                if self.args['xi'] == 0:
                    if (option_dim == 1):
                        xi = 0.8 * \
                            (max_q_omega_next -
                             tf.nn.top_k(self.q_omega_next_current, 1)[0][:, 0])
                    else:
                        xi = 0.8 * \
                            (max_q_omega_next -
                             tf.nn.top_k(self.q_omega_next_current, 2)[0][:, 1])
                else:
                    xi = self.args['xi']
                advantage_go = q_omega_val_next - max_q_omega_next + xi
                advantage = tf.stop_gradient(advantage_go)
                self.total_error_term = term_val_next * advantage

            with tf.name_scope('grad'):
                gradients = self.Opt_O.compute_gradients(
                    self.q_omega_loss, var_list=self.q_func_vars)
                for i, (grad, var) in enumerate(gradients):
                    if grad is not None:
                        gradients[i] = (tf.clip_by_norm(
                            grad, args['clip_value']), var)
                self.update_o = self.Opt_O.apply_gradients(gradients)
                gradients_t = self.Opt_T.compute_gradients(
                    self.total_error_term, var_list=self.q_func_vars)
                for i, (grad, var) in enumerate(gradients_t):
                    if grad is not None:
                        gradients_t[i] = (tf.clip_by_norm(
                            grad, args['clip_value']), var)
                self.update_t = self.Opt_T.apply_gradients(gradients_t)

            self.replace_target_op = [tf.assign(t, e) for t, e in zip(
                self.target_q_func_vars, self.q_func_vars)]

            self.sess = sess

    def _build_net(self, scope, s, reuse=False):
        w_init = tf.random_normal_initializer(0., .01)
        with tf.variable_scope(scope, reuse=reuse):
            l_a = tf.layers.dense(
                s, self.args['option_layer_1'], tf.nn.relu6, kernel_initializer=w_init, name='la')
            with tf.variable_scope("option_value"):
                q_omega = tf.layers.dense(
                    l_a, self.option_dim, tf.nn.tanh, kernel_initializer=w_init, name='omega_value')

            with tf.variable_scope("termination_prob"):
                term_prob = tf.layers.dense(l_a, self.option_dim, tf.sigmoid, kernel_initializer=w_init,
                                            name='term_prob')

        return q_omega, term_prob

    def store_transition(self, observation, action, reward, done, observation_, opa):
        self.replay_buffer.add(observation, action,
                               reward, done, observation_, opa)

    def update_e(self):
        self.epsilon = self.epsilon + \
            self.epsilon_increment if self.epsilon < self.e_greedy else self.e_greedy

    def choose_o(self, s):
        if np.random.uniform() < self.epsilon:
            options = self.sess.run(
                self.q_omega_current, feed_dict={self.s: s})
            options = options[0]

            return np.argmax(options)
        else:
            return np.random.randint(0, self.option_dim)

    def get_t(self, s_, option):
        terminations = self.sess.run(
            self.term_next_current, feed_dict={self.s_: s_})
        return terminations[0][option]

    def get_term_prob(self, s):
        return self.sess.run(self.term_current, feed_dict={self.s: s})

    def update(self, observation, option, done, observation_):
        if self.update_step % self.replace_target_iter == 0:
            self.sess.run(self.replace_target_op)
        self.update_step += 1

        if not done:
            loss_term, _ = self.sess.run([self.total_error_term, self.update_t], feed_dict={
                self.s: observation,
                self.option_o: [option],

                self.s_: observation_,
                self.done: [1.0 if done is True else 0.0]
            })

        minibatch = self.replay_buffer.get_batch(
            self.args['option_batch_size'])
        state_batch = np.asarray([data[0][0] for data in minibatch])
        action_batch = np.asarray([data[1] for data in minibatch])
        reward_batch = np.asarray([data[2][0] for data in minibatch])
        done_batch = np.array(
            [1.0 if data[3] else 0.0 for data in minibatch], dtype=np.float32)
        next_state_batch = np.asarray([data[4][0] for data in minibatch])
        opa_batch = np.asarray([data[5] for data in minibatch])

        loss_q_omega, _ = self.sess.run([self.q_omega_loss, self.update_o], feed_dict={
            self.s: state_batch,
            self.reward: reward_batch,
            self.s_: next_state_batch,
            self.option_o: [option],
            self.done: done_batch,
            self.option_a_t: opa_batch
        })
