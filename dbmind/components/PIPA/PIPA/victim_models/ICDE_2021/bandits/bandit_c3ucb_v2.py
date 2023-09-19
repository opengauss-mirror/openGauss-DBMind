import logging
import time
from abc import abstractmethod

import numpy

import constants


class C3UCBBaseBandit:

    def __init__(self, context_size, hyper_alpha, hyper_lambda, oracle):
        self.arms = []
        self.alpha_original = hyper_alpha
        self.hyper_alpha = hyper_alpha
        self.hyper_lambda = hyper_lambda        # lambda in C2CUB
        self.v = hyper_lambda * numpy.identity(context_size)    # identity matrix of n*n
        self.b = numpy.zeros((context_size, 1))  # [0, 0, ..., 0]T (column matrix) size = number of arms
        self.oracle = oracle
        self.context_vectors = []
        self.upper_bounds = []
        self.context_size = context_size

    @abstractmethod
    def select_arm(self, context_vectors, current_round):
        pass

    @abstractmethod
    def update(self, played_arms, reward, index_use):
        pass


class C3UCB(C3UCBBaseBandit):

    def select_arm(self, context_vectors, current_round):
        pass

    def select_arm_v2(self, context_vectors):
        """
        This method is responsible for returning the super arm

        :param context_vectors: context vector for this round
        :param current_round: current round number
        :return: selected set of arms
        """
        v_inverse = numpy.linalg.inv(self.v)
        weight_vector = v_inverse @ self.b
        logging.info(f"================================\n{weight_vector.transpose().tolist()[0]}")
        self.context_vectors = context_vectors

        # find the upper bound for every arm
        for i in range(len(self.arms)):
            creation_cost = weight_vector[1] * self.context_vectors[i][1]
            average_reward = numpy.asscalar(weight_vector.transpose() @ self.context_vectors[i]) - creation_cost
            temp_upper_bound = average_reward + self.hyper_alpha * numpy.sqrt(
                numpy.asscalar(self.context_vectors[i].transpose() @ v_inverse @ self.context_vectors[i]))
            temp_upper_bound = temp_upper_bound + (creation_cost/constants.CREATION_COST_REDUCTION_FACTOR)
            self.upper_bounds.append(temp_upper_bound)
        self.hyper_alpha = self.hyper_alpha / constants.ALPHA_REDUCTION_RATE
        return self.oracle.get_super_arm(self.upper_bounds, self.context_vectors, self.arms)

    def update(self, played_arms, reward, index_use):
        pass

    def update_v4(self, played_arms, arm_rewards):
        """
        This method can be used to update the reward after each play (improvements required)

        :param played_arms: list of played arms (super arm)
        :param arm_rewards: tuple (gains, creation cost) reward got form playing each arm
        """
        for i in played_arms:
            if self.arms[i].index_name in arm_rewards:
                arm_reward = arm_rewards[self.arms[i].index_name]
            else:
                arm_reward = (0, 0)
            logging.info(f"reward for {self.arms[i].index_name}, {self.arms[i].query_ids_backup} is {arm_reward}")
            self.arms[i].index_usage_last_batch = (self.arms[i].index_usage_last_batch + arm_reward[0]) / 2

            temp_context = numpy.zeros(self.context_vectors[i].shape)
            temp_context[1] = self.context_vectors[i][1]
            self.context_vectors[i][1] = 0

            self.v = self.v + (self.context_vectors[i] @ self.context_vectors[i].transpose())
            self.b = self.b + self.context_vectors[i] * arm_reward[0]

            self.v = self.v + (temp_context @ temp_context.transpose())
            self.b = self.b + temp_context * arm_reward[1]

        self.context_vectors = []
        self.upper_bounds = []

    def set_arms(self, bandit_arms):
        """
        This can be used to initially set the bandit arms in the algorithm

        :param bandit_arms: initial set of bandit arms
        :return:
        """
        self.arms = bandit_arms

    def hard_reset(self):
        """
        Resets the bandit
        """
        self.hyper_alpha = self.alpha_original
        self.v = self.hyper_lambda * numpy.identity(self.context_size)  # identity matrix of n*n
        self.b = numpy.zeros((self.context_size, 1))  # [0, 0, ..., 0]T (column matrix) size = number of arms

    def workload_change_trigger(self, workload_change):
        """
        This forgets history based on the workload change

        :param workload_change: Percentage of new query templates added (0-1) 0: no workload change, 1: 100% shift
        """
        logging.info("Workload change identified " + str(workload_change))
        if workload_change > 0.5:
            self.hard_reset()
        else:
            forget_factor = 1 - workload_change * 2
            if workload_change > 0.1:
                self.hyper_alpha = self.alpha_original
            self.v = self.hyper_lambda * numpy.identity(self.context_size) + forget_factor * self.v
            self.b = forget_factor * self.b

