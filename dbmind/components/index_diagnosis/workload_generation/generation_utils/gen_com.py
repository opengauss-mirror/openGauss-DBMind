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

import json
import logging
import argparse
import configparser

from .workload import Table, Column

tf_step = 0
summary_writer = None


def get_parser():
    parser = argparse.ArgumentParser(
        description="The Framework of Index Diagnosis Workload Generation.")

    parser.add_argument("--gpu_no", type=str, default="-1")
    parser.add_argument("--exp_id", type=str, default="diag_exp_id")

    parser.add_argument("--database", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)

    parser.add_argument("--train_mode", type=str, default="rl_pg",
                        choices=["rl_pg"])
    parser.add_argument("--model_struct", type=str, default="Seq2Seq",
                        choices=["Seq2Seq"])
    parser.add_argument("--is_bid", action="store_true")
    parser.add_argument("--is_attn", action="store_true")
    parser.add_argument("--is_ptr", action="store_true")

    parser.add_argument("--rnn_type", type=str, default="GRU")

    parser.add_argument("--pre_epoch", type=int, default=1)
    parser.add_argument("--pre_lr", type=float, default=0.001)
    parser.add_argument("--pre_mode", type=str, default="not_ae")
    parser.add_argument("--force_ratio", type=float, default=0.7)

    parser.add_argument("--rein_epoch", type=int, default=1)
    parser.add_argument("--rein_lr", type=float, default=0.001)

    parser.add_argument("--max_diff", type=int, default=5)
    parser.add_argument("--pert_mode", type=str, default="column",
                        choices=["all", "value", "column"])
    parser.add_argument("--reward", type=str, default="all_dynamic",
                        choices=["static", "dynamic", "all_dynamic"])
    parser.add_argument("--reward_form", type=str, default="cost_red_ratio",
                        choices=["cost_red_ratio", "cost_ratio", "cost_ratio_norm", "inv_cost_ratio_norm"])

    parser.add_argument("--work_level", type=str, default="query")
    parser.add_argument("--work_type", type=str, default="not_template")
    parser.add_argument("--inf", type=int, default=1e6)
    parser.add_argument("--eps", type=int, default=1e-36)

    parser.add_argument("--data_load", type=str,
                        default="./data_resource/sample_data/sample_data.pt")
    parser.add_argument("--model_load", type=str, default="empty")

    parser.add_argument("--cost_estimator", type=str, default="optimizer")
    parser.add_argument("--reward_base", action="store_true")

    # params for tested index advisor.
    parser.add_argument("--victim", type=str, default="advisor")
    parser.add_argument("--exp_file", type=str,
                        default="The path to the configuration file.")

    parser.add_argument("--colinfo_file", type=str,
                        default="./data_resource/database_conf/colinfo.json")
    parser.add_argument("--wordinfo_file", type=str,
                        default="./data_resource/vocab/wordinfo.json")
    parser.add_argument("--schema_file", type=str,
                        default="./data_resource/database_conf/schema.json")
    parser.add_argument("--data_save", type=str,
                        default="./workload_generation/exp_res/{}/data/{}_data.pt")

    parser.add_argument("--seed", type=int, default=666)
    parser.add_argument("--runlog", type=str,
                        default="./workload_generation/exp_res/{}/exp_runtime.log")
    parser.add_argument("--logdir", type=str,
                        default="./workload_generation/exp_res/{}/logdir/")
    parser.add_argument("--model_save_gap", type=int, default=1)
    parser.add_argument("--model_save", type=str,
                        default="./workload_generation/exp_res/{}/model/rewrite_{}.pt")

    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--dropout", type=float, default=0.5)

    parser.add_argument("--max_len", type=int, default=55)
    parser.add_argument("--src_vbs", type=int, default=3040)
    parser.add_argument("--tgt_vbs", type=int, default=3040)
    parser.add_argument("--emb_size", type=int, default=128)

    parser.add_argument("--enc_hidden_size", type=int, default=128)
    parser.add_argument("--dec_hidden_size", type=int, default=128)

    return parser


def set_logger(log_file):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s: - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)


def add_summary_value(key, value, step=None):
    if step is None:
        summary_writer.add_scalar(key, value, tf_step)
    else:
        summary_writer.add_scalar(key, value, step)


def get_columns_from_db(db_connector):
    tables, columns = list(), list()
    for table in db_connector.get_tables():
        table_object = Table(table)
        tables.append(table_object)
        for col in db_connector.get_cols(table):
            column_object = Column(col)
            table_object.add_column(column_object)
            columns.append(column_object)

    return tables, columns


def get_columns_from_schema(schema_file):
    tables, columns = list(), list()
    with open(schema_file, "r") as rf:
        db_schema = json.load(rf)

    for item in db_schema:
        table_object = Table(item["table"])
        tables.append(table_object)
        for col_info in item["columns"]:
            column_object = Column(col_info["name"])
            table_object.add_column(column_object)
            columns.append(column_object)

    return tables, columns


def dbmind_assert(condition, comment=None):
    if not condition:
        if comment is None:
            raise AssertionError("Please check the value of this variable. "
                                 "The value of condition is %s." % condition)
        else:
            raise ValueError(comment)
