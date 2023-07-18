# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import os
import json
import logging

import random
import numpy as np

import torch
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter

from .workload_generation.agent import WorkloadGeneration
from .workload_generation.dataset import SQLDataset, collate_fn4sql
from .workload_generation.generation_utils import gen_com


def main(argv):
    # 1. get the params.
    parser = gen_com.get_parser()
    args = parser.parse_args(argv)

    # 2. create the directory to store the `exp_res`.
    if not os.path.exists(os.path.dirname(args.logdir.format(args.exp_id))):
        os.makedirs(os.path.dirname(args.logdir.format(args.exp_id)))
    if not os.path.exists(os.path.dirname(args.model_save.format(args.exp_id, 0))):
        os.makedirs(os.path.dirname(args.model_save.format(args.exp_id, 0)))
    if not os.path.exists(os.path.dirname(args.data_save.format(args.exp_id, 0))):
        os.makedirs(os.path.dirname(args.data_save.format(args.exp_id, 0)))

    gen_com.set_logger(args.runlog.format(args.exp_id))

    logging.disable(logging.DEBUG)
    logging.info("Start Diagnosis Workload Generation.")
    logging.info(
        f"Create the directory `{os.path.dirname(args.logdir.format(args.exp_id))}` to save experiment result.")

    # specify the path to store the exp_res of `logdir` of the tensorboard.
    gen_com.summary_writer = SummaryWriter(args.logdir.format(args.exp_id))
    gen_com.summary_writer.add_text(
        "parameters",
        "|param|value|\n|-|-|\n%s" % ("\n".join([f"|{key}|{value}|" for key, value in vars(args).items()])),
        0
    )
    logging.info(f"Set the tensorboard logdir = `{args.logdir.format(args.exp_id)}`.")

    # 3. set the torch random_seed.
    # Sets the seed for generating random numbers. Returns a `torch.Generator` object.
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    logging.info(f"Set the random seed = `{args.seed}`.")

    # 4. load the training data.
    data = torch.load(args.data_load)
    data["src_vectors"] = [item["pno_tokens"] for item in data["src_tokens"]]
    data["tgt_vectors"] = [item["pno_tokens"] for item in data["tgt_tokens"]]
    logging.info(f"Load the data from `{args.data_load}({len(data['src_vectors'])})`.")

    # 5. split the data and create the train/valid data loader.
    if args.pre_mode == "ae":
        dataset = SQLDataset(data["tgt_vectors"], data["tgt_vectors"], data["src_tokens"])
        logging.info(f"All the training data is in the form of `(src, src)`.")
    else:
        dataset = SQLDataset(data["src_vectors"], data["tgt_vectors"], data["src_tokens"])
        logging.info(f"All the training data is in the form of `(src, tgt)`.")

    train_loader = DataLoader(dataset=dataset, batch_size=args.batch_size, shuffle=True,
                              collate_fn=collate_fn4sql, drop_last=True)
    valid_loader = DataLoader(dataset=dataset, batch_size=args.batch_size, shuffle=True,
                              collate_fn=collate_fn4sql, drop_last=True)

    torch.save(dataset, args.data_save.format(args.exp_id, "all"))
    logging.info(f"Save the dataset into `{os.path.dirname(args.data_save.format(args.exp_id, 0))}`.")

    # 6. start the training.
    agent = WorkloadGeneration(args)
    logging.info(f"Load the value of `is_bid`({args.is_bid}), `is_attn`({args.is_attn}), "
                 f"`is_ptr`({args.is_ptr}), `rnn_type`({args.rnn_type}).")

    with open(args.colinfo_file, "r") as rf:
        col_info = json.load(rf)
    with open(args.wordinfo_file, "r") as rf:
        word_info = json.load(rf)

    if args.train_mode == "rl_pg":
        logging.info("Start the `rl_pg` mode training.")
        logging.disable(logging.INFO)
        agent.env.setup()

        agent.pg_train(train_loader, valid_loader,
                       data["word2idx"], data["idx2word"], col_info, word_info)
        agent.env.connector.close()
        logging.disable(logging.DEBUG)
        logging.info("End the `rl_pg` mode training.")

    gen_com.summary_writer.close()
    logging.info("Close the tensorboard summary writer.")
