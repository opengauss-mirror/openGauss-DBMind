import time
from itertools import count
import matplotlib.pyplot as plt
import math
import numpy as np
import os
import pickle
# from tensorboardX import SummaryWriter
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import psycopg2
import logging
import random
import json
import sys

import Enviornment.Model3ICDE as env
import Enviornment.Env3DQNFixStorage as env2
from Model import PR_Buffer as BufferX
from Model import ReplyBuffer as Buffer

device = 'cuda:3' if torch.cuda.is_available() else 'cpu'
script_name = os.path.basename(__file__)
directory = './result/model/' 


class NN(nn.Module):
    def __init__(self, state_dim, action_dim):
        super(NN, self).__init__()

        self.layers = nn.Sequential(
            nn.Linear(state_dim, 512),
            nn.ReLU(),
            nn.Linear(512, 256),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(256, 256),
            nn.ReLU(),
            nn.Linear(256, action_dim),
            # nn.Sigmoid()
        )

    def _init_weights(self):
        for m in self.layers:
            if isinstance(m, nn.Linear):
                m.weight.data.normal_(0.0, 1e-2)
                m.bias.data.uniform_(-0.1, 0.1)

    def forward(self, state):
        actions = self.layers(state)
        return actions


class DNN(nn.Module):
    def __init__(self, state_dim_1,state_dim_2, action_dim):
        super(DNN, self).__init__()
        self.relu = nn.ReLU()
        self.l1 = nn.Linear(state_dim_1*state_dim_2, 512)
        self.l2 = nn.Linear(512, 256)
        # self.l3 = nn.Linear(256, 128)
        self.adv1 = nn.Linear(256, 256)
        self.adv2 = nn.Linear(256, action_dim)
        self.val1 = nn.Linear(256, 64)
        self.val2 = nn.Linear(64, 1)

    def _init_weights(self):
        self.l1.weight.data.normal_(0.0, 1e-2)
        self.l1.weight.data.uniform_(-0.1, 0.1)
        self.l2.weight.data.normal_(0.0, 1e-2)
        self.l2.weight.data.uniform_(-0.1, 0.1)
        # self.l3.weight.data.normal_(0.0, 1e-2)
        # self.l3.weight.data.uniform_(-0.1, 0.1)
        self.adv1.weight.data.normal_(0.0, 1e-2)
        self.adv1.weight.data.uniform_(-0.1, 0.1)
        self.adv2.weight.data.normal_(0.0, 1e-2)
        self.adv2.weight.data.uniform_(-0.1, 0.1)
        self.val1.weight.data.normal_(0.0, 1e-2)
        self.val1.weight.data.uniform_(-0.1, 0.1)
        self.val2.weight.data.normal_(0.0, 1e-2)
        self.val2.weight.data.uniform_(-0.1, 0.1)

    def forward(self, state):
        # actions = self.layers(state)
        x = self.relu(self.l1(state))
        x = self.relu(self.l2(x))
        # x = self.relu(self.l3(x))
        adv = self.relu(self.adv1(x))
        val = self.relu(self.val1(x))
        adv = self.relu(self.adv2(adv))
        val = self.relu(self.val2(val))
        qvals = val + (adv-adv.mean())
        return qvals


class DQN:
    def __init__(self, workload, action, index_mode, conf, is_dnn, is_ps, is_double, whether_first = False, dir = directory):
        setup_seed(88)
        self.conf = conf
        self.workload = workload
        self.action = action
        self.index_mode = index_mode
        self.frequencies = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        # =======修改部分======= #
        # 计算访问向量和索引可选择性向量
        self.access = [0] * len(action)
        self.index_selectivity = [0] * len(action)

        database = json.load(open(sys.argv[1]))["psql_connect"]
        conn = psycopg2.connect(database=json.load(open(sys.argv[1]))["dataset"], user=database["pg_user"], password=database["pg_password"],
                              host=database["pg_ip"], port=database["pg_port"])
        cursor = conn.cursor()
        sql1 = "select relname, reltuples from pg_class where relname like '%_pkey' limit 8"
        sql2 = "select tablename,attname,n_distinct from pg_stats where schemaname = 'public'"
        cursor.execute(sql1)
        data1 = cursor.fetchall()
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        conn.commit()
        conn.close()
        global directory
        directory = dir
        for i in action:
            for j in range(len(workload)):
                if i.split("#")[1] in workload[j]:
                    self.access[action.index(i)] += self.frequencies[j]
        for i in data2:
            if i[2] < 0:
                self.index_selectivity[action.index(i[0] + "#" + i[1])] = -i[2]
            else:
                sum = 0
                for j in data1:
                    if j[0] == (i[0] + "_pkey"):
                        sum = j[1]
                        break
                self.index_selectivity[action.index(i[0] + "#" + i[1])] = i[2] / sum
        # 更新维度
        self.state_dim_1 = len(self.frequencies) + 3
        self.state_dim_2 = len(action)
        # =======修改结束======= #
        # we do not need another flag to indicate 'deletion/creation'
        self.action_dim = len(action)
        self.is_ps = is_ps
        self.is_double = is_double
        self.actor = DNN(self.state_dim_1,self.state_dim_2, self.action_dim).to(device)
        self.actor_target = DNN(self.state_dim_1,self.state_dim_2, self.action_dim).to(device)
        self.actor_target.load_state_dict(self.actor.state_dict())
        self.actor_optimizer = optim.Adam(self.actor.parameters(), conf['LR']) #optim.SGD(self.actor.parameters(), lr=self.conf['LR'], momentum=0.9)#

        self.replay_buffer = None
        # some monitor information
        self.num_actor_update_iteration = 0
        self.num_training = 0
        self.index_mode = index_mode
        self.actor_loss_trace = list()
    
        # environment
        logging.info("Init Environment...")
        self.envx = env.Env(self.workload, self.action, self.frequencies, self.index_mode, self.access, self.index_selectivity)

        # store the parameters
        # self.writer = SummaryWriter(directory)

        self.learn_step_counter = 0
        self.current_best_index = []
        self.orginal_reward = 0
        self.number = conf["number"]
        self.whether_first = whether_first

    def select_action(self, t, state):
        # state_tmp = state
        state = state.reshape(-1)
        if self.whether_first and not self.replay_buffer.can_update():
            action = np.random.randint(0, len(self.action))
            action = [action]
            return action
        state = torch.unsqueeze(torch.FloatTensor(state), 0).to(device)
        if np.random.randn() <= self.conf['EPISILO']: # *(1 - math.pow(0.5, t/50)):  #*(t/MAX_STEP):  # greedy policy
            action_value = self.actor.forward(state)
            '''action_value_list = action_value.tolist()[0]
            action_dict = {}
            for i in range(len(action_value_list)):
                action_dict["" + str(i)] = action_value_list[i]
            action_dict = sorted(action_dict.items(), key=lambda item:item[1])
            k = len(action_value_list) - 1
            action = [int(action_dict[k][1])]
            while state_tmp[self.state_dim_1-1][action[0]] != 0.0:
                k = k - 1
                action = [int(action_dict[k][0])]'''
            action = torch.max(action_value, 1)[1].data.cpu().numpy()
            j = 0
            # if state_tmp[self.state_dim_1-1][action[0]] == 1:
            for i in action_value[0]:
                if i >= action_value[0][action] and np.random.rand()<0.05:
                    action = np.array([j])
                j = j+1
            return action
        else:  # random policy
            action = np.random.randint(0, len(self.action))
            action = [action]
            return action

    def _sample(self):
        batch, idx = self.replay_buffer.sample(self.conf['BATCH_SIZE'])
        # state, next_state, action, reward, np.float(done))
        # batch = self.replay_memory.sample(self.batch_size)
        x, y, u, r, d = [], [], [], [], []
        for _b in batch:
            x.append(np.array(_b[0], copy=False))
            y.append(np.array(_b[1], copy=False))
            u.append(np.array(_b[2], copy=False))
            r.append(np.array(_b[3], copy=False))
            d.append(np.array(_b[4], copy=False))
        return idx, np.array(x), np.array(y), np.array(u), np.array(r).reshape(-1, 1), np.array(d).reshape(-1, 1)

    def adjust_learning_rate(self, optimizer, epoch):
        """Sets the learning rate to the initial LR decayed by 10 every 30 epochs"""
        lr = self.conf['LR'] * (0.1 ** (epoch // 30))
        for param_group in optimizer.param_groups:
            param_group['lr'] = lr

    def update(self, ep):
        print("======= UPDATE =======")
        if self.learn_step_counter % self.conf['Q_ITERATION'] == 0:
            self.actor_target.load_state_dict(self.actor.state_dict())
        self.learn_step_counter += 1
        # self.adjust_learning_rate(self.actor_optimizer, ep)
        for it in range(self.conf['U_ITERATION']):
            idxs = None
            if self.is_ps:
                idxs, x, y, u, r, d = self._sample()
            else:
                x, y, u, r, d = self.replay_buffer.sample(self.conf['BATCH_SIZE'])
            state = torch.FloatTensor(x).to(device)
            action = torch.LongTensor(u).to(device)
            next_state = torch.FloatTensor(y).to(device)
            done = torch.FloatTensor(d).to(device)
            reward = torch.FloatTensor(r).to(device)
            state = state.reshape(64,-1)
            next_state = next_state.reshape(64,-1)
            q_eval = self.actor(state).gather(1, action)

            if self.is_double:
                next_batch = self.actor(next_state)
                nx = next_batch.max(1)[1][:,None]
                # max_act4next = np.argmax(q_eval_next, axis=1)
                q_next = self.actor_target(next_state)
                qx = q_next.gather(1,nx)
                # q_target = reward + (1 - done) * self.conf['GAMMA'] * qx.max(1)[0].view(self.conf['BATCH_SIZE'], 1)
                q_target = reward + (1 - done) * self.conf['GAMMA'] * qx
            else:
                q_next = self.actor_target(next_state).detach()
                q_target = reward + (1-done)*self.conf['GAMMA'] * q_next.max(1)[0].view(self.conf['BATCH_SIZE'], 1)
            actor_loss = F.mse_loss(q_eval, q_target)
            error = torch.abs(q_eval - q_target).data.cpu().numpy()
            if self.is_ps:
                for i in range(self.conf['BATCH_SIZE']):
                    idx = idxs[i]
                    self.replay_buffer.update(idx, error[i][0])

            self.actor_optimizer.zero_grad()
            actor_loss.backward()
            self.actor_optimizer.step()

            self.actor_loss_trace.append(actor_loss.data.item())
            # for item in self.actor.named_parameters():
                # h = item[1].register_hook(lambda grad: print(grad))

    def save(self):
        torch.save(self.actor.state_dict(), directory + 'dqn.pth')
        logging.info('====== Model Saved ======')

    def load(self):
        logging.info('====== Model Loaded ======')
        self.actor.load_state_dict(torch.load(directory + 'dqn.pth'))
    
    def last(self):
        logging.info('====== Model Loaded ======')
        self.actor.load_state_dict(torch.load(directory + 'dqn.pth'))
        current_best_index = self.current_best_index
        return current_best_index


    def train(self, load, probing, __x = 4):
        if load:
            logging.warning("Loading the Agent Actor Parameters Stored Before...")
            self.load()
        is_first = True
        # check whether have an index will 90% improvement
        logging.info("Index Constraint: Numbers of " + str(__x))
        self.envx.max_count = __x
        # pre_create = self.envx.checkout()
        # if not (pre_create is None):
        #     print(pre_create)
        #     if len(pre_create) >= __x:
        #         return pre_create[:__x]
        if self.is_ps:
            self.replay_buffer = BufferX.PrioritizedReplayMemory(self.conf['MEMORY_CAPACITY'], min(self.conf['LEARNING_START'],200*self.envx.max_count))
        else:
            self.replay_buffer = Buffer.ReplayBuffer(self.conf['MEMORY_CAPACITY'], min(self.conf['LEARNING_START'],200*self.envx.max_count))
        current_best_reward = 0
        current_best_index = None
        rewards = []
        __how_m = self.envx.max_count
        orginal_reward = 0
        logging.info("=====Begin Train====")
        for ep in range(self.conf['EPISODES']):
            if ep%20 ==0:
                logging.info("=="+str(ep)+"==")
            state = self.envx.reset()

            t_r = 0
            _state = []
            _next_state = []
            _action = []
            _reward = []
            _done = []
            for t in count():
                action = self.select_action(ep, state)
                if ep%20 ==0:
                    logging.info("Action: " + str(self.action[action[0]]))
                next_state, reward, done = self.envx.step(action)
                if ep%20 ==0:
                    logging.info("Reward: " + str(reward))
                t_r += reward
                if not self.replay_buffer.can_update():
                    orginal_reward = orginal_reward + reward
                '''_state.append(state)
                _next_state.append(next_state)
                _action.append(action)
                _reward.append(reward)
                _done.append(np.float(done))'''
                if self.is_ps:
                    self.replay_buffer.add(1.0, (state, next_state, action, reward, np.float(done)))
                else:
                    self.replay_buffer.push((state, next_state, action, reward, np.float(done)))
                # if self.replay_buffer.can_update():
                #    self.update()
                if done:
                    if ep%20 ==0:
                        logging.info("This Epoch Total_Reward: " + str(t_r))
                        logging.info("======")
                    '''for i in range(len(_state)):
                        if self.isPS:
                            self.replay_buffer.add(1.0, (_state[i], _next_state[i], _action[i], _reward[i]+t_r/__how_m, _done[i]))
                        else:
                            self.replay_buffer.push((_state[i], _next_state[i], _action[i], _reward[i]+t_r/__how_m, _done[i]))'''
                    if ep > (self.conf['EPISODES']-100) and t_r > current_best_reward:
                        current_best_reward = t_r
                        current_best_index = self.envx.index_trace_overall[-1]
                        # print(current_best_index)
                    # self.replay_buffer.add(1.0, (state, next_state, action, reward, np.float(done)))
                    if self.replay_buffer.can_update() and ep % 5 == 0:
                        self.update(ep)
                    break
                state = next_state
            rewards.append(t_r)
        logging.info("Train Successful: ")
        self.rewards = rewards    

        # return self.envx.index_trace_overall[-1]
        logging.info("current_best_reward:" + str(current_best_reward))

        if self.replay_buffer.LEARNING_START != 0:
            self.orginal_reward = self.orginal_reward/self.replay_buffer.LEARNING_START
        else:
            self.orginal_reward = 0
        if current_best_reward == 0:
            return 0,0,current_best_index
        for _i, _idx in enumerate(current_best_index):
            if _idx == 1.0:
                self.current_best_index.append(self.action[_i])
        if not probing:
            self.save()
        return self.orginal_reward,current_best_reward,current_best_index

def setup_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)
    torch.backends.cudnn.deterministic = True


