import random
import numpy
import math
import logging
from abc import abstractmethod

import constants

from keras.models import Sequential
from keras.layers import *
from keras.optimizers import *

# HUBER_LOSS_DELTA = 2.0
# LEARNING_RATE = 0.00025

MEMORY_CAPACITY = 900

BATCH_SIZE = 1

GAMMA = 0.99

MAX_EPSILON = 1
MIN_EPSILON = 0

EXPLORATION_STOP = 1200  # at this step epsilon will be 0.01
LAMBDA = - math.log(0.01) / EXPLORATION_STOP  # speed of decay

# MAX_EPSILON = 1
# MIN_EPSILON = 0.1
#
# EXPLORATION_STOP = 500000  # at this step epsilon will be 0.01
# LAMBDA = - math.log(0.01) / EXPLORATION_STOP  # speed of decay

UPDATE_TARGET_FREQUENCY = 60


class SumTree:
    write = 0

    def __init__(self, capacity):
        self.capacity = capacity
        self.tree = numpy.zeros(2*capacity - 1)
        self.data = numpy.zeros(capacity, dtype=object)

    def _propagate(self, idx, change):
        parent = (idx - 1) // 2

        self.tree[parent] += change     # we have to update it via change since the parent depends on both children

        if parent != 0:
            self._propagate(parent, change)

    def _retrieve(self, idx, s):
        left = 2 * idx + 1              # index id of the node idx's left child
        right = left + 1                # index id of the node idx's right child

        if left >= len(self.tree):      # overbound, so the node has no child
            return idx                  # return the index id of the node, because the node idx is a leaf

        if s <= self.tree[left]:
            return self._retrieve(left, s)
        else:
            return self._retrieve(right, s-self.tree[left])

    def total(self):
        return self.tree[0]             # the root represents the total of the leaves

    def add(self, p, data):
        idx = self.write + self.capacity - 1    # the first available index (since total of its parents are cap - 1)

        self.data[self.write] = data            # update the actual data
        self.update(idx, p)                     # update the priority

        self.write += 1
        if self.write >= self.capacity:
            self.write = 0

    def update(self, idx, p):
        change = p - self.tree[idx]     # need to update via 'change' since a parent depends on BOTH children

        self.tree[idx] = p
        self._propagate(idx, change)

    def get(self, s):
        idx = self._retrieve(0, s)
        dataIdx = idx - self.capacity + 1       # dataIdx = idx - (capacity - 1), since cap - 1 is the tot # of parents

        return idx, self.tree[idx], self.data[dataIdx]  # returns the index, the priority, and the data in that index


# -------------------- UTILITIES -----------------------
def huber_loss(y_true, y_pred):
    err = y_true - y_pred

    cond = K.abs(err) < HUBER_LOSS_DELTA
    L2 = 0.5 * K.square(err)
    L1 = HUBER_LOSS_DELTA * (K.abs(err) - 0.5 * HUBER_LOSS_DELTA)

    loss = tf.where(cond, L2, L1)  # Keras does not cover where function in tensorflow :-(

    return K.mean(loss)


# -------------------- BRAIN ---------------------------
class Brain:
    def __init__(self, stateCnt, actionCnt):
        self.stateCnt = stateCnt
        self.actionCnt = actionCnt

        self.model = self._createModel()
        self.model_ = self._createModel()  # target network

    def _createModel(self):
        model = Sequential()

        model.add(Dense(8, input_dim=self.stateCnt, activation='relu'))
        model.add(Dense(8, activation='relu'))
        model.add(Dense(8, activation='relu'))
        model.add(Dense(8, activation='relu'))
        model.add(Dense(self.actionCnt, activation='softmax'))

        # opt = RMSprop(lr=LEARNING_RATE)
        # model.compile(loss=huber_loss, optimizer=opt)
        model.compile(loss='mse', optimizer='adam')
        return model

    def train(self, x, y, epochs=1, verbose=0):
        self.model.fit(x, y, batch_size=BATCH_SIZE, epochs=epochs, verbose=verbose)

    def predict(self, s, target=False):
        if target:
            return self.model_.predict(s)
        else:
            return self.model.predict(s)

    def predictOne(self, s, target=False):
        return self.predict(s.reshape(1, len(s)), target).flatten()

    def updateTargetModel(self):
        self.model_.set_weights(self.model.get_weights())


# -------------------- MEMORY --------------------------
class Memory:  # stored as ( s, a, r, s_ ) in SumTree
    e = 0.01
    a = 0.6

    def __init__(self, capacity):
        self.tree = SumTree(capacity)

    def _getPriority(self, error):
        return (error + self.e) ** self.a

    def add(self, error, sample):
        p = self._getPriority(error)
        self.tree.add(p, sample)

    def sample(self, n):
        batch = []
        segment = self.tree.total() / n

        for i in range(n):
            a = segment * i
            b = segment * (i + 1)

            s = random.uniform(a, b)
            (idx, p, data) = self.tree.get(s)
            batch.append((idx, data))

        return batch

    def update(self, idx, error):
        p = self._getPriority(error)
        self.tree.update(idx, p)


# -------------------- AGENT ---------------------------

class Agent:

    def __init__(self, stateCnt, actionCnt):
        self.stateCnt = stateCnt
        self.actionCnt = actionCnt

        self.steps = 0
        self.epsilon = MAX_EPSILON

        self.brain = Brain(stateCnt, actionCnt)
        self.memory = Memory(MEMORY_CAPACITY)
        # self.memory = Memory(MEMORY_CAPACITY)

    # def act(self, s):
    #     if random.random() < self.epsilon:
    #         return random.randint(0, self.actionCnt - 1)
    #     else:
    #         return numpy.argmax(self.brain.predictOne(s))

    def observe(self, sample):  # in (s, a, r, s_) format
        x, y, errors = self._getTargets([(0, sample)])
        self.memory.add(errors[0], sample)

        if self.steps % UPDATE_TARGET_FREQUENCY == 0:
            self.brain.updateTargetModel()

        # slowly decrease Epsilon based on our eperience
        self.steps += 1
        self.epsilon = MIN_EPSILON + (MAX_EPSILON - MIN_EPSILON) * math.exp(-LAMBDA * self.steps)

    def _getTargets(self, batch):
        no_state = numpy.zeros(self.stateCnt)

        states = numpy.array([o[1][0] for o in batch])
        states_ = numpy.array([(no_state if o[1][3] is None else o[1][3]) for o in batch])

        # p = agent.brain.predict(states)
        p = self.brain.predict(states)

        # p_ = agent.brain.predict(states_, target=False)
        p_ = self.brain.predict(states_, target=False)
        # pTarget_ = agent.brain.predict(states_, target=True)
        pTarget_ = self.brain.predict(states_, target=True)

        # x = numpy.zeros((len(batch), MATRIX_ROW * MATRIX_COLUMN))
        x = numpy.zeros((len(batch), self.stateCnt))
        y = numpy.zeros((len(batch), self.actionCnt))
        errors = numpy.zeros(len(batch))

        for i in range(len(batch)):
            o = batch[i][1]
            s = o[0]
            a = o[1]
            r = o[2]
            s_ = o[3]

            t = p[i]
            oldVal = t[a]
            if s_ is None:
                t[a] = r
            else:
                t[a] = r + GAMMA * pTarget_[i][numpy.argmax(p_[i])]  # double DQN

            x[i] = s
            y[i] = t
            errors[i] = abs(oldVal - t[a])

        return (x, y, errors)

    def replay(self):
        batch = self.memory.sample(BATCH_SIZE)
        x, y, errors = self._getTargets(batch)

        # update errors
        for i in range(len(batch)):
            idx = batch[i][0]
            self.memory.update(idx, errors[i])

        self.brain.train(x, y)


# class RandomAgent:
#     memory = Memory(MEMORY_CAPACITY)
#     exp = 0
#
#     def __init__(self, actionCnt):
#         self.actionCnt = actionCnt
#
#     def act(self, s):
#         return random.randint(0, self.actionCnt - 1)
#
#     def observe(self, sample):  # in (s, a, r, s_) format
#         error = abs(sample[2])  # reward
#         self.memory.add(error, sample)
#         self.exp += 1
#
#     def replay(self):
#         pass


class DdqnBase:

    def __init__(self, context_size, oracle):
        self.arms = []
        self.oracle = oracle
        self.context_vectors = []
        self.qfunction = []
        self.context_size = context_size
        self.state = numpy.ndarray.flatten(numpy.array(self.context_vectors))
        self.stateCnt = len(self.state)
        self.steps = 0
        self.israndom = True
        self.ddqn_round = 0
        self.actionCnt = None
        self.stateCnt = None
        self.agent = None
        self.randomagent = None
        self.arms_set = False

    @abstractmethod
    def select_arm(self, context_vectors, current_round):
        pass

    @abstractmethod
    def update(self, played_arms, reward):
        pass


class DDQN(DdqnBase):

    def select_arm(self, context_vectors, current_round):
        """
        This method is responsible for returning the super arm

        :param context_vectors: context vector for this round
        :param current_round: current round number
        :return: selected set of arms
        """
        self.context_vectors = context_vectors
        self.state = numpy.ndarray.flatten(numpy.array(context_vectors))

        # estimate the Q-functions
        if len(self.arms) <= 0:
            pass
        else:
            # determine whether we should explore or exploit
            if random.random() < self.agent.epsilon:
                self.israndom = True
                logging.info(f"The action taken is random in this round.")
            else:
                self.israndom = False

            if self.israndom:
                # generate a random number for each index so that the agent picks a set of random indexes
                self.qfunction = [random.random() for _ in range(len(self.arms))]
            else:
                self.qfunction = self.agent.brain.predictOne(self.state)

        logging.debug(self.qfunction)

        if len(self.arms) <= 0:
            return []
        else:
            return self.oracle.get_super_arm(self.qfunction, self.context_vectors, self.arms)

    def update(self, played_arms, arm_rewards):
        """
        This method can be used to update the reward after each play (improvements required)

        :param played_arms: list of played arms (super arm)
        :param arm_rewards: tuple (gains, creation cost) reward got form playing each arm
        """

        if len(self.arms) > 0:
            for i in played_arms:
                if self.arms[i].index_name in arm_rewards:
                    arm_reward = arm_rewards[self.arms[i].index_name]
                else:
                    arm_reward = (0, 0)
                logging.info(f"reward for {self.arms[i].index_name}, {self.arms[i].query_ids_backup} is {arm_reward}")
                # self.arms[i].index_usage_last_batch = (self.arms[i].index_usage_last_batch + arm_reward[0]) / 2

                temp_context = numpy.zeros(self.context_vectors[i].shape)
                temp_context[1] = self.context_vectors[i][1]
                self.context_vectors[i][1] = 0
                self.state_ = numpy.ndarray.flatten(numpy.array(self.context_vectors))

                logging.info(f"Samples observed: {self.ddqn_round}")
                self.agent.observe((self.state, i, sum(arm_reward), self.state_))
                self.agent.replay()
                self.ddqn_round += 1

            self.context_vectors = []
            self.qfunction = []

        else:
            self.context_vectors = []
            self.qfunction = []

    def set_arms(self, bandit_arms):
        """
        This can be used to initially set the bandit arms in the algorithm

        :param bandit_arms: initial set of bandit arms
        :return:
        """
        self.arms = bandit_arms

    def init_agents(self, context_vectors):
        self.context_vectors = context_vectors
        s = numpy.ndarray.flatten(numpy.array(context_vectors))
        self.state = s
        self.actionCnt = len(self.arms)
        self.stateCnt = len(s)
        self.agent = Agent(self.stateCnt, self.actionCnt)
        logging.info("An agent was created.")
