from .Common import Common
from .ExtendEnv import GenQueryEnv

import numpy as np
import logging
import base
import os
import sys

class Env(object):
    def __init__(self,dbname):
        self.c_obj = Common(dbname)
        self.QueryEnv = GenQueryEnv(self.c_obj)
        logging.warning("We only choose to generate Query(select) only.")
        self.Map = {
            'query': self.QueryEnv,
            # 'insert': self.InsertEnv,
            # 'update': self.UpdateEnv,
            # 'delete': self.DeleteEnv
        }
        self.dbname = dbname
        self.observation_space = self.action_space = self.c_obj.action_space
        self.state = self.c_obj.start_word
        self.bug_reward = self.c_obj.bug_reward
        self.max_length = self.c_obj.SEQ_LENGTH
        self.task_name = self.c_obj.task_name
        

    def reset(self):
        if self.state in self.Map:
            self.Map[self.state].reset()
        self.state = self.c_obj.start_word
        return self.c_obj.word_num_map[self.c_obj.start_word]

    def observe(self, observation):
        if observation == self.c_obj.word_num_map[self.c_obj.start_word]:
            candidate_word = np.zeros((self.observation_space,), dtype=int)
            self.c_obj.activate_space(candidate_word, 'query')
            # self.c_obj.activate_space(candidate_word, 'insert')
            # self.c_obj.activate_space(candidate_word, 'update')
            # self.c_obj.activate_space(candidate_word, 'delete')
            return candidate_word
        else:
            return self.Map[self.state].observe(observation)

    def step(self, action):
        if self.c_obj.num_word_map[action] in self.Map:
            self.state = self.c_obj.num_word_map[action]
        return self.Map[self.state].step(action)

    def get_sql(self):
        return self.Map[self.state].get_sql()

    def is_satisfy(self, sql):
        return self.c_obj.is_satisfy(sql)