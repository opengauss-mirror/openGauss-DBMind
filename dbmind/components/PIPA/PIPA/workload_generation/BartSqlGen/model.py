import random

import torch
import gc
import torch.nn as nn
import numpy as np
import logging
import matplotlib.pyplot as plt
import time
import sys
import json
from pathlib import Path
import os

from transformers import DataCollatorWithPadding, AdamW, get_scheduler

from .local_transformers.models.bart.tokenization_bart import BartTokenizer
from .local_transformers.models.bart.modeling_bart import BartModel, BartConfig, BartPretrainedModel, \
    BartForConditionalGeneration
from .processing import DataProcessing
from tqdm.auto import tqdm
from nlgeval import compute_metrics
from .FSM.Env import Env
from random import choice

CONFIGURATION_FILE = json.load(open(sys.argv[1]))
MODEL_PATH = CONFIGURATION_FILE["experiments_root"] + '/workload_generation/BartSqlGen/resource/'
if Path(os.getcwd() + "/result/experiment_full.log").exists():
    os.remove(os.getcwd() + "/result/experiment_full.log")
print(torch.cuda.device_count())
device = torch.device("cuda:3") if torch.cuda.is_available() else torch.device("cpu")


class LoadPretrainedBartModel():
    def __init__(self):
        self.model_config = BartConfig.from_pretrained(MODEL_PATH)
        self.model = BartModel.from_pretrained(MODEL_PATH, config=self.model_config)
        self.task = BartForConditionalGeneration.from_pretrained(MODEL_PATH, config=self.model_config)
        self.tok = BartTokenizer.from_pretrained(MODEL_PATH)

    def get_model(self):
        return self.model

    def get_config(self):
        return self.model_config

    def get_task(self):
        return self.task

    def get_tok(self):
        return self.tok


class BartSqlGenModel(BartPretrainedModel):
    def __init__(self, config: BartConfig, tok: BartTokenizer, model: BartModel, task: BartForConditionalGeneration,
                 data: DataProcessing):
        super().__init__(config)
        self.config = config
        self.batchsize = 1
        self.tok = tok
        self.BartConditionalGeneration = task
        self.model = model
        """self.train_data = data[0]
        self.test_data = data[1]"""
        self.BartSqlGen = self.loadSqlGenModel(task)
        self.env = Env(json.load(open(sys.argv[1]))["dataset"])
        self.table = []
        for k in range(50265):
            key = self.tok.decode([k], skip_special_tokens=True)
            if key not in self.table:
                self.table.append(key)
        self.table.remove(" parts")
        for key in range(len(self.env.c_obj.num_word_map)):
            value = self.env.c_obj.num_word_map[key]
            value_new = []
            if isinstance(value, float) or isinstance(value, int):
                value = str(value)
                value = " " + value
                start = 0
                end = len(str(value))
                value_new = []
                while start < len(value):
                    if value[start:end] in self.table:
                        value_new.append(value[start:end])
                        start = end
                        end = len(value)
                    else:
                        end -= 1
            elif " " == value:
                value_new.append(value)
            elif " " in value:
                value = value.split(" ")
                for i in range(len(value)):
                    value_new.append(" " + value[i])
            else:
                value = " " + value
                start = 0
                end = len(str(value))
                value_new = []
                while start < len(value):
                    if end == 0:
                        value_new = [value]
                        break
                    if value[start:end] in self.table:
                        value_new.append(value[start:end])
                        start = end
                        end = len(value)
                    else:
                        end -= 1
            self.env.c_obj.num_word_map_seperate[key] = value_new
        # 定位表和属性的下标，sql_mask用
        flag_end = False
        flag_number = False
        self.table_begin = 3
        for i in range(3, len(self.env.c_obj.num_word_map_seperate)):
            if not flag_number and not flag_end and "." in self.env.c_obj.num_word_map_seperate[i]:
                self.table_end = (i - 1)
                self.attr_start = i
                flag_end = True
            if not flag_number and flag_end and self.env.c_obj.num_word_map_seperate[i][0].strip().isdigit():
                self.attr_end = (i - 1)
                self.number_start = i
                flag_end = False
                flag_number = True
            if flag_number and not self.env.c_obj.num_word_map_seperate[i][0].strip().isdigit() and not \
            self.env.c_obj.num_word_map_seperate[i][0].strip() == "-":
                self.number_end = (i - 1)
                break
        logging.info(self.env.c_obj.num_word_map_seperate)

    def loadSqlGenModel(self, GenerationModel):
        from .local_transformers.models.bart.modeling_bart import BartForSqlGeneration

        BartSqlGen = BartForSqlGeneration(self.config, GenerationModel.model)
        BartSqlGen.load_lm_head(GenerationModel)

        return BartSqlGen

    def train(self):
        optimizer = AdamW(self.BartSqlGen.parameters(), lr=5e-5)
        self.num_epochs = 50
        num_training_steps = self.num_epochs * len(self.train_data)
        lr_scheduler = get_scheduler(
            "linear",
            optimizer=optimizer,
            num_warmup_steps=0,
            num_training_steps=num_training_steps,
        )
        progress_bar = tqdm(range(num_training_steps))

        total_epoch_loss = []
        self.BartSqlGen.to(device)

        self.BartSqlGen.train()
        for epoch in range(self.num_epochs):
            batch_loss = []
            total_loss = 0
            if epoch < 15:
                for batch in self.train_data:
                    labels, wordmask, indexmask, sqlmask = map(lambda x: x.to(device), batch)
                    input_ids = self.mask_data(labels)

                    output, done = self.BartSqlGen(input_ids=input_ids, labels=labels)
                    logits = output.logits
                    masked_index = (input_ids[0] == self.tok.mask_token_id)
                    masked_index = torch.nonzero(masked_index).item()
                    probs = logits[0, masked_index].softmax(dim=0)
                    logging.info("##############")
                    logging.info("labels:" + str(self.tok.decode(labels[0])))
                    logging.info("input:" + str(self.tok.decode(input_ids[0])))
                    values, predictions = probs.topk(5)
                    logging.info("top-5 predict:" + str(self.tok.decode(predictions).split()))
                    logging.info("##############")

                    loss = output.loss
                    total_loss += loss.item()
                    batch_loss.append(loss.item())

                    loss.backward()

                    optimizer.step()
                    lr_scheduler.step()
                    optimizer.zero_grad()
                    progress_bar.update(1)
                if epoch == 1:
                    self.draw_pictures(batch_loss, 1)
                logging.warning("The No." + str(epoch) + " loss(train single mask):" + str(total_loss))
                total_epoch_loss.append(total_loss)

            elif epoch < 30:
                for batch in self.train_data:
                    labels, wordmask, indexmask, sqlmask = map(lambda x: x.to(device), batch)
                    input_ids = wordmask
                    input = self.tok.decode(input_ids[0])

                    done = 0
                    first2 = True
                    batch_loss_batch = 0
                    label_count = labels.shape[1] - len([x for x in labels[0] if x == 1])
                    input_count = input_ids.shape[1] - len([x for x in input_ids[0] if x == 1])
                    for i in range(label_count - input_count):
                        output, done = self.BartSqlGen(input_ids=input_ids, labels=labels)
                        logits = output.logits

                        if first2 == True:
                            masked_index = (input_ids[0] == self.tok.mask_token_id)
                            masked_index = torch.nonzero(masked_index).item()
                        else:
                            masked_index += 1
                        probs = logits[0, masked_index].softmax(dim=0)
                        new_token = self.tok.decode(probs.topk(5)[1]).split()
                        right_token = self.tok.decode(labels[0, masked_index])
                        logging.info("##############")
                        logging.info("labels:" + str(self.tok.decode(labels[0])))
                        logging.info("input:" + str(input))
                        logging.info("top-5 predict:" + str(new_token))

                        input = input.split("<mask>")[0] + right_token + "<mask>" + input.split("<mask>")[
                            1]  # 不是autoencoder形式
                        if "<pad>" in input:
                            input = input.split("<pad>")[0]
                        if "<s>" in input and "</s>" in input:
                            input = input.rsplit("<s>", 1)[1]
                            input = input.rsplit("</s>", 1)[0]
                        input_ids = self.tok([input], return_tensors="pt")["input_ids"]
                        input_ids = input_ids.to(device)

                        if first2 == True or flag == 0:
                            loss = output.loss.clone()
                            first2 = False
                            flag = 1
                        elif flag < 16:
                            loss += output.loss
                            flag += 1
                        if flag == 16 or i >= (label_count - input_count - 1):
                            loss.backward()
                            batch_loss_batch += loss.item()
                            optimizer.step()
                            optimizer.zero_grad()
                            flag = 0
                        if input_ids.shape[1] >= labels.shape[1] or done == 1:
                            break

                    total_loss += batch_loss_batch
                    batch_loss.append(batch_loss_batch)
                    lr_scheduler.step()
                    progress_bar.update(1)
                logging.warning("The No." + str(epoch) + " loss(train index mask):" + str(total_loss))
                total_epoch_loss.append(total_loss)


            elif epoch < 40:
                for batch in self.train_data:
                    labels, wordmask, indexmask, sqlmask = map(lambda x: x.to(device), batch)
                    input_ids = indexmask
                    input = self.tok.decode(input_ids[0])

                    done = 0
                    first2 = True
                    batch_loss_batch = 0
                    label_count = labels.shape[1] - len([x for x in labels[0] if x == 1])
                    input_count = input_ids.shape[1] - len([x for x in input_ids[0] if x == 1])
                    for i in range(label_count - input_count):
                        output, done = self.BartSqlGen(input_ids=input_ids, labels=labels)
                        logits = output.logits

                        if first2 == True:
                            masked_index = (input_ids[0] == self.tok.mask_token_id)
                            masked_index = torch.nonzero(masked_index).item()
                        else:
                            masked_index += 1
                        probs = logits[0, masked_index].softmax(dim=0)
                        new_token = self.tok.decode(probs.topk(5)[1]).split()
                        right_token = self.tok.decode(labels[0, masked_index])
                        logging.info("##############")
                        logging.info("labels:" + str(self.tok.decode(labels[0])))
                        logging.info("input:" + str(input))
                        logging.info("top-5 predict:" + str(new_token))

                        input = input.split("<mask>")[0] + right_token + "<mask>" + input.split("<mask>")[
                            1]  # 不是autoencoder形式
                        if "<pad>" in input:
                            input = input.split("<pad>")[0]
                        if "<s>" in input and "</s>" in input:
                            input = input.rsplit("<s>", 1)[1]
                            input = input.rsplit("</s>", 1)[0]
                        input_ids = self.tok([input], return_tensors="pt")["input_ids"]
                        input_ids = input_ids.to(device)

                        if first2 == True or flag == 0:
                            loss = output.loss.clone()
                            first2 = False
                            flag = 1
                        elif flag < 16:
                            loss += output.loss
                            flag += 1
                        if flag == 16 or i >= (label_count - input_count - 1):
                            loss.backward()
                            batch_loss_batch += loss.item()
                            optimizer.step()
                            optimizer.zero_grad()
                            flag = 0
                        if input_ids.shape[1] >= labels.shape[1] or done == 1:
                            break

                    total_loss += batch_loss_batch
                    batch_loss.append(batch_loss_batch)
                    lr_scheduler.step()
                    progress_bar.update(1)
                logging.warning("The No." + str(epoch) + " loss(train index mask):" + str(total_loss))
                total_epoch_loss.append(total_loss)

            else:
                for batch in self.train_data:
                    labels, wordmask, indexmask, sqlmask = map(lambda x: x.to(device), batch)
                    input_ids = sqlmask
                    input = self.tok.decode(input_ids[0])

                    done = 0
                    first2 = True
                    batch_loss_batch = 0
                    label_count = labels.shape[1] - len([x for x in labels[0] if x == 1])
                    input_count = input_ids.shape[1] - len([x for x in input_ids[0] if x == 1])
                    for i in range(label_count - input_count):
                        output, done = self.BartSqlGen(input_ids=input_ids, labels=labels)
                        logits = output.logits

                        if first2 == True:
                            masked_index = (input_ids[0] == self.tok.mask_token_id)
                            masked_index = torch.nonzero(masked_index).item()
                        else:
                            masked_index += 1
                        probs = logits[0, masked_index].softmax(dim=0)
                        new_token = self.tok.decode(probs.topk(5)[1]).split()
                        right_token = self.tok.decode(labels[0, masked_index])
                        logging.info("##############")
                        logging.info("labels:" + str(self.tok.decode(labels[0])))
                        logging.info("input:" + str(input))
                        logging.info("top-5 predict:" + str(new_token))

                        input = input.split("<mask>")[0] + right_token + "<mask>" + input.split("<mask>")[
                            1]  # 不是autoencoder形式
                        if "<pad>" in input:
                            input = input.split("<pad>")[0]
                        if "<s>" in input and "</s>" in input:
                            input = input.rsplit("<s>", 1)[1]
                            input = input.rsplit("</s>", 1)[0]
                        input_ids = self.tok([input], return_tensors="pt")["input_ids"]
                        input_ids = input_ids.to(device)

                        if first2 == True or flag == 0:
                            loss = output.loss.clone()
                            first2 = False
                            flag = 1
                        elif flag < 16:
                            loss += output.loss
                            flag += 1
                        if flag == 16 or i >= (label_count - input_count - 1):
                            loss.backward()
                            batch_loss_batch += loss.item()
                            optimizer.step()
                            optimizer.zero_grad()
                            flag = 0
                        if input_ids.shape[1] >= labels.shape[1] or done == 1:
                            break

                    total_loss += batch_loss_batch
                    batch_loss.append(batch_loss_batch)
                    lr_scheduler.step()
                    progress_bar.update(1)
                logging.warning("The No." + str(epoch) + " loss(train index mask):" + str(total_loss))
                total_epoch_loss.append(total_loss)
        logging.warning("total_epoch_loss:" + str(total_epoch_loss))
        torch.save(self.BartSqlGen.state_dict(), "./net_full_final_TPCH.pth")

    def evaluate(self):
        self.evaluate_count = 0.0

        self.single_mask_right_count = 0.0

        self.word_mask_right_count = 0.0
        self.word_mask_similar_right_count = 0.0

        self.index_mask_right_count = 0.0
        self.index_generate_list = []
        self.index_label_list = ["1a"]

        self.sql_label_list = []
        self.sql_generate_list = []

        progress_bar = tqdm(range(len(self.test_data)))

        for batch in self.test_data:
            self.evaluate_count += 1
            labels, wordmask, indexmask, sqlmask = map(lambda x: x, batch)

            self.singlemask(labels)
            self.wordmask(labels, wordmask)
            self.indexmask(labels, indexmask)
            self.sqlmask(labels, sqlmask)
            progress_bar.update(1)

        single_mask_ac = self.single_mask_right_count / self.evaluate_count
        logging.warning("single_mask_ac: " + str(single_mask_ac))

        word_mask_ac = self.word_mask_right_count / self.evaluate_count
        word_mask_similar_ac = self.word_mask_similar_right_count / self.evaluate_count
        logging.warning("word_mask_ac: " + str(word_mask_ac))
        logging.warning("word_mask_similar_ac: " + str(word_mask_similar_ac))

        self.save_txt(self.sql_label_list, "result/sql_label_list.txt")
        self.save_txt(self.sql_generate_list, "result/sql_generate_list.txt")

        sql_metrics = compute_metrics("result/sql_label_list.txt", "result/sql_generate_list.txt")
        logging.warning("sql_metrics: " + str(sql_metrics))

    def save_txt(self, list, name):
        with open(name, 'w') as f:
            for i in list:
                f.write(i + '\n')

    def singlemask(self, labels):
        done = 0
        input_ids = self.mask_data(labels)
        output, done = self.BartSqlGen(input_ids=input_ids, done=done)
        logits = output.logits
        masked_index = (input_ids[0] == self.tok.mask_token_id)
        masked_index = torch.nonzero(masked_index).item()
        probs = logits[0, masked_index].softmax(dim=0)
        if probs.topk(1)[1] == labels[0, masked_index]:
            self.single_mask_right_count += 1

    def wordmask(self, labels, wordmask):
        done = 0
        right = 1
        similar_right = 1
        first = True
        input = self.tok.decode(wordmask[0])
        label = self.tok.decode(labels[0])
        while done != 1 and right == 1:
            output, done = self.BartSqlGen(input_ids=wordmask, done=done)
            logits = output.logits
            if first == True:
                masked_index = (wordmask[0] == self.tok.mask_token_id)
                masked_index = torch.nonzero(masked_index).item()
                first = False
            else:
                masked_index += 1
            probs = logits[0, masked_index].softmax(dim=0)
            new_token = self.tok.decode(probs.topk(1)[1])
            right_token = self.tok.decode(labels[0, masked_index])

            input = input.split("<mask>")[0] + new_token + "<mask>" + input.split("<mask>")[1]  # 不是autoencoder形式
            if "<pad>" in input:
                input = input.split("<pad>")[0]
            if "<s>" in input and "</s>" in input:
                input = input.rsplit("<s>", 1)[1]
                input = input.rsplit("</s>", 1)[0]

            # 三个判出条件
            if wordmask.shape[1] > labels.shape[1] + 1:
                right = 0
                break
            if probs.topk(1)[1] != labels[0, masked_index]:
                right = 0
                similar_right = 0
                if new_token.isdigit() and right_token.isdigit():
                    if int(new_token) - int(right_token) < 10 and int(right_token) - int(new_token) > -10:
                        similar_right = 1
                break
            if probs.topk(1)[1] == wordmask[0, masked_index + 1]:
                done = 1
                break

            wordmask = self.tok([input], return_tensors="pt")["input_ids"]
        self.word_mask_right_count += right
        self.word_mask_similar_right_count += similar_right

    def indexmask(self, labels, indexmask):
        done = 0
        right = 1
        first = True
        input = self.tok.decode(indexmask[0])
        label = self.tok.decode(labels[0])
        label_sequence = []
        generate_sequence = []
        while done != 1:
            output, done = self.BartSqlGen(input_ids=indexmask, done=done)
            logits = output.logits
            if first == True:
                masked_index = (indexmask[0] == self.tok.mask_token_id)
                masked_index = torch.nonzero(masked_index).item()
                first = False
            else:
                masked_index += 1
            probs = logits[0, masked_index].softmax(dim=0)
            new_token = self.tok.decode(probs.topk(1)[1])

            input = input.split("<mask>")[0] + new_token + "<mask>" + input.split("<mask>")[1]  # 不是autoencoder形式
            if "<pad>" in input:
                input = input.split("<pad>")[0]
            if "<s>" in input and "</s>" in input:
                input = input.rsplit("<s>", 1)[1]
                input = input.rsplit("</s>", 1)[0]

            # 判出条件
            if indexmask.shape[1] > labels.shape[1] + 1:
                right = 0
            if masked_index >= labels.shape[1]:
                right = 0
                break
            elif probs.topk(1)[1] != labels[0, masked_index]:
                right = 0
            if masked_index + 1 >= indexmask.shape[1]:
                done = 1
                break
            if probs.topk(1)[1] == indexmask[0, masked_index + 1]:
                done = 1
                break

            indexmask = self.tok([input], return_tensors="pt")["input_ids"]
            generate_sequence.append(probs.topk(1)[1].item())
            label_sequence.append(labels[0, masked_index])
        generate_sentence = self.tok.decode(generate_sequence, skip_special_tokens=True)
        self.index_generate_list.append(generate_sentence)

        label_sentence = self.tok.decode(label_sequence, skip_special_tokens=True)
        self.index_label_list.append(label_sentence)

        self.index_mask_right_count += right

    def sqlmask(self, labels, sqlmask):
        try:
            done = 0
            done = torch.tensor(done).to(device)
            first = True
            input = self.tok.decode(sqlmask[0])
            if labels:
                label = self.tok.decode(labels[0])
                sql_lable = label.split("</s>")[0].split("<s>")[1].strip()
            label_sequence = []
            generate_sequence = []
            if labels:
                label_length = (labels[0] == 2)
                label_length = torch.nonzero(label_length)[0].item()
            current_state = self.env.reset()
            candidate_action = self.env.observe(current_state)
            candidate_action = action = [[x, "from"] for x in range(len(candidate_action)) if candidate_action[x] == 1]
            self.env.step(action[0][0])
            current_state = action[0][0]
            flag_done = False
            flag_query = True
            flag_from = False
            flag_from_join = False
            flag_from_join_start = False
            flag_from_join_end = False
            flag_select = False
            flag_select_attr = False
            flag_select_attr_start = False
            flag_select_attr_end = False
            flag_order_by = False
            flag_order_by_attr = False
            flag_dot = True
            flag_order_by_attr_start = True
            flag_order_by_attr_end = False
            order_by_attr = 0
            flag_terminal = False
            flag_end = False
            run_times = 0
            while done == 0:
                run_times += 1
                try:
                    output, done = self.BartSqlGen(input_ids=sqlmask.to(device), done=done)
                except Exception as e:
                    return None
                logits = output.logits
                if first:
                    masked_index = (sqlmask[0] == self.tok.mask_token_id)
                    masked_index = torch.nonzero(masked_index).item()
                    first = False
                else:
                    masked_index += 1
                probs = logits[0, masked_index].softmax(dim=0)
                new_token = self.tok.decode(probs.topk(1)[1])

                if flag_done:
                    candidate_action = self.env.observe(current_state)
                    if flag_from_join:
                        candidate_action[self.number_end + 21] = 0
                    action = [x for x in range(len(candidate_action)) if
                              candidate_action[x] == 1]
                    if (len(action) == 1 and action[0] == 1) or (flag_select and new_token == " improvement"):
                        break
                    action = [
                        [x, self.env.c_obj.num_word_map_seperate[x][0:]]
                        for x in range(len(candidate_action)) if
                        candidate_action[x] == 1]
                    flag_number = True
                    for ix in range(len(action)):
                        if not (self.number_start <= action[ix][0] <= self.number_end):
                            flag_number = False
                    if flag_number:
                        rd_num = random.randint(0, len(action) - 1)
                        new_tokens = []
                        for ix in range(len(action[rd_num][1])):
                            new_token = self.tok([action[rd_num][1][ix]])["input_ids"][0][1]
                            new_tokens.append(new_token)
                            generate_sequence.append(new_token)
                        new_tokens = self.tok.decode(new_tokens)
                        input = input.split("<mask>")[0] + new_tokens + "<mask>" + input.split("<mask>")[
                            1]  # 不是autoencoder形式
                        masked_index += (len(action[rd_num][1]) - 1)
                        done = self.env.step(action[rd_num][0])
                        current_state = action[rd_num][0]
                        continue
                    if candidate_action[1] == 1:
                        flag_terminal = True
                    else:
                        flag_terminal = False
                    flag_attr = False
                    for x in range(len(candidate_action)):
                        if flag_select or flag_order_by:
                            if self.attr_start <= x <= self.attr_end and candidate_action[x] == 1:
                                flag_attr = True
                    if flag_select:
                        flag_select_attr = False
                    elif flag_order_by:
                        flag_order_by_attr = False
                    k = 0
                    if flag_from and not flag_from_join and not flag_from_join_end and new_token == " join":
                        flag_from_join = True
                    elif flag_select and not flag_select_attr_end and not flag_dot and flag_attr and new_token == ",":
                        flag_select_attr = True
                        flag_dot = True
                    elif flag_order_by and not flag_order_by_attr_end and not flag_dot and flag_attr and new_token == ",":
                        flag_order_by_attr = True
                        flag_dot = True
                    elif not flag_dot and flag_terminal and ((flag_select and not flag_select_attr_end) or (
                            flag_order_by and not flag_order_by_attr_end and order_by_attr > 0)) and new_token == " ":
                        flag_end = True
                    action = [
                        [x, self.env.c_obj.num_word_map_seperate[x][1:]]
                        for x in range(len(candidate_action)) if
                        candidate_action[x] == 1 and str(new_token) == str(self.env.c_obj.num_word_map_seperate[x][0])]
                    if flag_end:
                        generate_sequence.append(probs.topk(1)[1].item())
                        break
                    if (not flag_from_join or flag_from_join_start) and not flag_select_attr and not flag_order_by_attr:
                        if (flag_select or flag_order_by) and not flag_dot:
                            candidate_action = []
                        k = 1
                        while len(action) == 0 and k <= 1000:
                            k += 1
                            new_token = self.tok.decode(probs.topk(k)[1][k - 1])
                            if flag_from and not flag_from_join and not flag_from_join_end and new_token == " join":
                                flag_from_join = True
                                break
                            elif flag_select and not flag_select_attr_end and not flag_dot and flag_attr and new_token == ",":
                                flag_select_attr = True
                                flag_dot = True
                                break
                            elif flag_order_by and not flag_order_by_attr_end and not flag_dot and flag_attr and new_token == ",":
                                flag_order_by_attr = True
                                flag_dot = True
                                break
                            elif not flag_dot and flag_terminal and ((flag_select and not flag_select_attr_end) or (
                                    flag_order_by and not flag_order_by_attr_end and order_by_attr > 0)) and new_token == " ":
                                flag_end = True
                                break
                            action = [
                                [x, self.env.c_obj.num_word_map_seperate[x][1:]]
                                for x in range(len(candidate_action)) if
                                candidate_action[x] == 1 and str(new_token) == str(
                                    self.env.c_obj.num_word_map_seperate[x][0])]
                        if k >= 1000:
                            generate_sequence = None
                            break
                    if k == 50265 or ((flag_select and not flag_select_attr_end and not flag_order_by) or (
                            flag_order_by and not flag_order_by_attr_end and order_by_attr > 0)) and action and \
                            action[0][
                                0] == 1:
                        generate_sequence.append(self.tok.encode(" ")[1])
                        break
                    elif flag_end:
                        generate_sequence.append(probs.topk(k)[1][k - 1].item())
                        break
                    if new_token == " where":
                        flag_from = flag_select = flag_order_by = False
                    if new_token == " select":
                        flag_select = True
                        flag_from = flag_order_by = False
                    flag_done = False
                    if flag_from and flag_from_join_start and action and self.table_begin <= action[0][
                        0] <= self.table_end:
                        done = self.env.step(action[0][0])
                        current_state = action[0][0]
                        if len(action[0][1]) == 0:
                            sql = self.env.get_sql().split(new_token + " ")[1]
                            sql = (new_token + " " + sql)
                        else:
                            str_remain = ""
                            for i in range(len(action[0][1])):
                                str_remain += action[0][1][i]
                            sql = self.env.get_sql().split(new_token + str_remain + " ")[1]
                            sql = (new_token + str_remain + " " + sql)
                        value = sql[0:len(sql) - 1]
                        value_new = []
                        start = 0
                        end = len(str(value))
                        while start < len(value):
                            if value[start:end] in self.table:
                                value_new.append(value[start:end])
                                start = end
                                end = len(value)
                            else:
                                end -= 1
                        candidate_action = action = [[current_state, value_new]]
                        new_token = self.tok.decode(probs.topk(1)[1])
                        action = [[x[0], x[1][1:]]
                                  for x in candidate_action if
                                  str(new_token) == str(x[1][0])]
                        k = 1
                        while len(action) == 0 and k < 1000:
                            k += 1
                            new_token = self.tok.decode(probs.topk(k)[1][k - 1])
                            action = [[x[0], x[1][1:]]
                                      for x in candidate_action if
                                      str(new_token) == str(x[1][0])]
                        if k >= 1000:
                            generate_sequence = None
                            break
                        flag_done = False
                        flag_from_join = False
                        flag_from_join_start = False
                        flag_from_join_end = True
                    if flag_select and flag_dot and flag_select_attr_start and action and self.attr_start <= action[0][
                        0] <= self.attr_end:
                        action_old = action
                        sql_old = self.env.get_sql().split(" from")[0]
                        done = self.env.step(action[0][0])
                        current_state = action[0][0]
                        sql_new = self.env.get_sql().split(" from")[0]
                        sql = sql_new.split(sql_old)[1]
                        value = sql[1:len(sql)]
                        value_new = []
                        start = 0
                        end = len(str(value))
                        while start < len(value):
                            if value[start:end] in self.table:
                                value_new.append(value[start:end])
                                start = end
                                end = len(value)
                            else:
                                end -= 1
                        candidate_action = action = [[current_state, value_new]]
                        new_token = self.tok.decode(probs.topk(1)[1])
                        try:
                            action = [[x[0], x[1][1:]]
                                      for x in candidate_action if
                                      str(new_token) == str(x[1][0])]
                        except Exception as e:
                            continue
                        k = 1
                        while len(action) == 0 and k < 1000:
                            k += 1
                            new_token = self.tok.decode(probs.topk(k)[1][k - 1])
                            try:
                                action = [[x[0], x[1][1:]]
                                          for x in candidate_action if
                                          str(new_token) == str(x[1][0])]
                            except Exception as e:
                                print("error candidate_action!")
                                print(candidate_action)
                                print(action_old)
                        if k >= 1000:
                            generate_sequence = None
                            break
                        flag_done = False
                        flag_from_join_start = False
                        flag_select_attr_end = True
                    if flag_order_by and flag_dot and flag_order_by_attr_start and action and self.attr_start <= \
                            action[0][
                                0] <= self.attr_end:
                        order_by_attr += 1
                        sql_old = self.env.get_sql()
                        done = self.env.step(action[0][0])
                        current_state = action[0][0]
                        sql_new = self.env.get_sql()
                        sql = sql_new.split(sql_old[0:len(sql_old) - 1])[1]
                        if sql[0] == ",":
                            value = sql[1:len(sql) - 1]
                        else:
                            value = sql[0:len(sql) - 1]
                        value_new = []
                        start = 0
                        end = len(str(value))
                        while start < len(value):
                            if value[start:end] in self.table:
                                value_new.append(value[start:end])
                                start = end
                                end = len(value)
                            else:
                                end -= 1
                        candidate_action = action = [[current_state, value_new]]
                        new_token = self.tok.decode(probs.topk(1)[1])
                        action = [[x[0], x[1][1:]]
                                  for x in candidate_action if
                                  str(new_token) == str(x[1][0])]
                        k = 1
                        while len(action) == 0 and k < 1000:
                            k += 1
                            new_token = self.tok.decode(probs.topk(k)[1][k - 1])
                            action = [[x[0], x[1][1:]]
                                      for x in candidate_action if
                                      str(new_token) == str(x[1][0])]
                        if k >= 1000:
                            generate_sequence = None
                            break
                        flag_done = False
                        flag_order_by_attr_start = False
                        flag_order_by_attr_end = True
                    if flag_from and flag_from_join and new_token == " join" and not flag_from_join_start:
                        flag_from_join_start = True
                    if flag_select and action and self.attr_start <= action[0][0] <= self.attr_end:
                        flag_select_attr_start = True
                    if flag_order_by and action and self.attr_start <= action[0][0] <= self.attr_end:
                        flag_order_by_attr_start = True
                    if k == 0:
                        generate_sequence.append(probs.topk(1)[1].item())
                    else:
                        generate_sequence.append(probs.topk(k)[1][k - 1].item())
                    database_name = json.load(open(sys.argv[1]))["dataset"][0:4]
                    input = input.split("<mask>")[0] + new_token + "<mask>" + input.split("<mask>")[
                        1]  # 不是autoencoder形式
                    if (flag_from and flag_from_join) or (flag_select and flag_select_attr) or (
                            flag_order_by and flag_order_by_attr):
                        flag_done = True
                    elif flag_from and ((database_name == "tpch" and new_token == ' part') or (
                            database_name == "tpcd" and new_token == ' customer' or new_token == ' store')):
                        input2 = input.split("<mask>")[0] + new_token + "<mask>" + input.split("<mask>")[
                            1]  # 不是autoencoder形式
                        if "<pad>" in input2:
                            input2 = input2.split("<pad>")[0]
                        if "<s>" in input and "</s>" in input:
                            input2 = input2.rsplit("<s>", 1)[1]
                            input2 = input2.rsplit("</s>", 1)[0]
                        sqlmask2 = self.tok([input2], return_tensors="pt")["input_ids"].to(device)
                        output2, done2 = self.BartSqlGen(input_ids=sqlmask2, done=done)
                        logits2 = output2.logits
                        probs2 = logits2[0, masked_index + 1].softmax(dim=0)
                        new_token2 = self.tok.decode(probs2.topk(1)[1])
                        if new_token == " part":
                            if new_token2 != "supp" and len(action) > 1:
                                for ix in range(len(action)):
                                    if len(action[ix][1]) == 0:
                                        done = self.env.step(action[ix][0])
                                        current_state = action[ix][0]
                                        break
                                flag_done = True
                        elif new_token == " customer":
                            if new_token2 != "_" and len(action) > 1:
                                for ix in range(len(action)):
                                    if len(action[ix][1]) == 0:
                                        done = self.env.step(action[ix][0])
                                        current_state = action[ix][0]
                                        break
                                flag_done = True
                        elif new_token == " store":
                            if new_token2 != "_" and len(action) > 1:
                                for ix in range(len(action)):
                                    if len(action[ix][1]) == 0:
                                        done = self.env.step(action[ix][0])
                                        current_state = action[ix][0]
                                        break
                                flag_done = True
                    else:
                        for i in range(len(action)):
                            if len(action[i][1]) == 0 and np.random.rand() < 1 / len(action):
                                try:
                                    done = self.env.step(action[i][0])
                                except Exception as e:
                                    print(self.env.get_sql())
                                    print(action)
                                    time.sleep(10000)
                                current_state = action[i][0]
                                flag_done = True
                else:
                    if flag_query:
                        action = [[x[0], x[1][len(new_token):len(x[1])]]
                                  for x in candidate_action if
                                  len(new_token) <= len(x[1]) and new_token == x[1][0:len(new_token)]]
                        k = 1
                        while len(action) == 0 and k < 1000:
                            k += 1
                            new_token = self.tok.decode(probs.topk(k)[1][k - 1])
                            action = [[x[0], x[1][len(new_token):len(x[1])]]
                                      for x in candidate_action if
                                      new_token == x[1][0:len(new_token)]]
                        if k >= 1000:
                            generate_sequence = None
                            break
                        done = self.env.step(action[0][0])
                        current_state = action[0][0]
                        generate_sequence.append(probs.topk(k)[1][k - 1].item())
                        flag_done = True
                        flag_query = False
                        flag_from = True
                        input = input.split("<mask>")[0] + new_token + "<mask>" + input.split("<mask>")[
                            1]  # 不是autoencoder形式
                    else:
                        if new_token == " by":
                            flag_order_by = True
                            flag_from = flag_select = False
                            flag_dot = True
                        candidate_action = action
                        if len(candidate_action) == 1 and len(candidate_action[0][1]) > 1:
                            if not (flag_from and flag_from_join_end) and not (
                                    flag_select and flag_select_attr_end) and not (
                                    flag_order_by and flag_order_by_attr_end):
                                done = self.env.step(action[0][0])
                                current_state = action[0][0]
                            new_tokens = []
                            for ix in range(len(candidate_action[0][1])):
                                new_token = self.tok([candidate_action[0][1][ix]])["input_ids"][0][1]
                                new_tokens.append(new_token)
                                generate_sequence.append(new_token)
                            new_tokens = self.tok.decode(new_tokens)
                            input = input.split("<mask>")[0] + new_tokens + "<mask>" + input.split("<mask>")[
                                1]  # 不是autoencoder形式
                            flag_done = True
                            masked_index += (len(candidate_action[0][1]) - 1)
                            if flag_select or flag_order_by:
                                flag_dot = False
                            action[0][1] = []
                        else:
                            action = [[x[0], x[1][1:]]
                                      for x in candidate_action if len(x[1]) != 0 and new_token == x[1][0]]
                            k = 1
                            k = 1
                            while len(action) == 0 and k <= 1000:
                                k += 1
                                new_token = self.tok.decode(probs.topk(k)[1][k - 1])
                                action = [[x[0], x[1][1:]]
                                          for x in candidate_action if len(x[1]) != 0 and new_token == x[1][0]]
                            if k >= 1000:
                                generate_sequence = None
                                break
                            generate_sequence.append(probs.topk(k)[1][k - 1].item())
                            input = input.split("<mask>")[0] + new_token + "<mask>" + input.split("<mask>")[
                                1]  # 不是autoencoder形式
                        if flag_from and flag_from_join_end and len(action[0][1]) == 0:
                            flag_from_join_end = False
                            flag_from_join = False
                            flag_done = True
                        elif flag_select and flag_select_attr_end and len(action[0][1]) == 0:
                            flag_select_attr_end = False
                            flag_dot = False
                            flag_done = True
                        elif flag_order_by and flag_order_by_attr_end and len(action[0][1]) == 0:
                            flag_order_by_attr_end = False
                            flag_dot = False
                            flag_done = True
                        elif not flag_done:
                            for i in range(len(action)):
                                if len(action[i][1]) == 0 and np.random.rand() < 1 / len(action):
                                    if flag_select or flag_order_by:
                                        flag_dot = False
                                    done = self.env.step(action[i][0])
                                    current_state = action[i][0]
                                    flag_done = True
                if "<pad>" in input:
                    input = input.split("<pad>")[0]
                if "<s>" in input and "</s>" in input:
                    input = input.rsplit("<s>", 1)[1]
                    input = input.rsplit("</s>", 1)[0]

                sqlmask = self.tok([input], return_tensors="pt")["input_ids"].to(device)
            if not generate_sequence:
                return None
            generate_sentence = self.tok.decode(generate_sequence, skip_special_tokens=True)
            print("gener_sentence = " + generate_sentence)
            if not labels:
                return generate_sentence
            self.sql_generate_list.append(generate_sentence)

            if labels:
                label_sentence = self.tok.decode(label_sequence, skip_special_tokens=True)
                print("label_sentence = " + sql_lable)
                self.sql_label_list.append(sql_lable)
        except:
            return None

    def draw_pictures(self, batch_loss, number):
        loss = []
        for i in batch_loss:
            loss.append(i)
        plt.figure(figsize=(30, 10), dpi=300)
        plt.plot(loss)
        f = plt.gcf()
        f.savefig(r"./pictures" + str(number) + ".png")
        f.clear()

    def mask_data(self, batch):
        batch2 = torch.clone(batch)
        for i in batch2:
            choose_mask_method = np.random.rand()
            if choose_mask_method < 1:
                while 1:
                    a = np.random.randint(0, len(i) - 1)
                    if i[a] != 1:
                        i[a] = self.tok.mask_token_id
                        break
                    else:
                        continue

        return batch2

    def dataLoader(self, data):
        count = 0
        train_data = []
        test_data = []
        train_batch = []
        test_batch = []

        for i in data:
            if count % 10 != 9 and count % 10 != 8:
                if len(train_batch) < self.batchsize:
                    train_batch.append(i)
                else:
                    train_data.append(train_batch)
                    train_batch = []
            else:
                if len(test_batch) < self.batchsize:
                    test_batch.append(i)
                else:
                    test_data.append(test_batch)
                    test_batch = []
            count += 1

        train_data_tok = self.tok(train_data, return_tensors="pt")["input_ids"]

        return train_data, test_data


class GenerationTask():
    def __init__(self):
        data = DataProcessing()
        """train_data, test_data = data()
        dataset = [train_data, test_data]"""

        PretrainModel = LoadPretrainedBartModel()
        config = PretrainModel.get_config()
        model = PretrainModel.get_model()
        task = PretrainModel.get_task()
        tok = PretrainModel.get_tok()

        GenModel = BartSqlGenModel(config, tok, model, task, None).to(device)
        print(torch.cuda.device_count())
        dict = torch.load("./workload_generation/BartSqlGen/net_full_final_TPCH.pth", map_location="cuda:3")
        GenModel.BartSqlGen.load_state_dict(dict)

        self.config = config
        self.model = model
        self.task = task
        self.tok = tok

        self.GenModel = GenModel

    def generate_sql_bad(self, index):
        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        time.sleep(5)
        first = True
        for i in index:
            i = i[1]
            if "#" in i:
                i = i.replace("#", ".")
            if first == True:
                input = "<mask> </s> " + "The improvement of " + i
                first = False
            else:
                input = input + ';' + i
        input = input + " is 10.0"
        input_ids = self.tok([input], return_tensors="pt")["input_ids"]
        generate = self.GenModel.sqlmask(None, input_ids)
        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        time.sleep(5)
        return generate

    def generate_sql_suboptimal(self, index):
        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        time.sleep(5)
        first = True
        for i in index:
            i = i[1]
            if "#" in i:
                i = i.replace("#", ".")
            if first == True:
                input = "<mask> </s> " + "The improvement of " + i
                first = False
            else:
                input = input + ';' + i
        input = input + " is 18.0"
        input_ids = self.tok([input], return_tensors="pt")["input_ids"]
        generate = self.GenModel.sqlmask(None, input_ids)
        gc.collect()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        torch.cuda.empty_cache()
        time.sleep(5)
        return generate

    def generate_sql(self, index):
        first = True
        for i in index:
            i = i[1]
            if "#" in i:
                i = i.replace("#", ".")
            if first == True:
                input = "<mask> </s> " + "The improvement of " + i
                first = False
            else:
                input = input + ';' + i
        input = input + " is 20.0"
        input_ids = self.tok([input], return_tensors="pt")["input_ids"]
        generate = self.GenModel.sqlmask(None, input_ids)
        return generate


if __name__ == '__main__':
    data = DataProcessing()
    train_data, test_data = data()
    dataset = [train_data, test_data]

    PretrainModel = LoadPretrainedBartModel()
    config = PretrainModel.get_config()
    model = PretrainModel.get_model()
    task = PretrainModel.get_task()
    tok = PretrainModel.get_tok()

    GenModel = BartSqlGenModel(config, tok, model, task, dataset)
    # GenModel.train()
    print(torch.cuda.device_count())
    dict = torch.load("./net_full_final_TPCH.pth", map_location="cuda:3")
    GenModel.BartSqlGen.load_state_dict(dict)
    GenModel.evaluate()
