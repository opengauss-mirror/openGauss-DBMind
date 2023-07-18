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

import torch
from torch.utils.data import Dataset

import numpy as np

from .generation_utils import constants


def padd_to_longest(raw_data, max_len=55, tokenizer=None, is_src=False):
    if tokenizer is None or not is_src:
        padd_array = np.array([dat + [constants.EOS] +
                               [constants.PAD] * (max_len - len(dat) - 1)
                               for dat in raw_data])
    else:
        padd_array = np.array([[tokenizer.cls_token_id if tokenizer.cls_token_id else constants.BOS] +
                               dat + [tokenizer.eos_token_id if tokenizer.eos_token_id else constants.EOS] +
                               [tokenizer.pad_token_id if tokenizer.pad_token_id else constants.PAD] * (
                                       max_len - len(dat) - 2)
                               for dat in raw_data])

    # numpy array -> tensor
    padd_data_tensor = torch.from_numpy(padd_array)
    return padd_data_tensor


def collate_fn4sql(samples, tokenizer=None):
    src_sql, tgt_sql, sql_tokens = map(list, zip(*samples))

    max_len = np.max([len(sql) for sql in src_sql]) + 5
    padd_tensor_src = padd_to_longest(src_sql, max_len=max_len,
                                      tokenizer=tokenizer, is_src=True)

    max_len = np.max([len(sql) for sql in tgt_sql]) + 5
    padd_tensor_tgt = padd_to_longest(tgt_sql, max_len=max_len,
                                      tokenizer=tokenizer, is_src=False)

    return padd_tensor_src, padd_tensor_tgt, sql_tokens


class SQLDataset(Dataset):
    def __init__(self, src_sql, tgt_sql, sql_tokens):
        self.src_sql = src_sql
        self.tgt_sql = tgt_sql
        self.sql_tokens = sql_tokens

    def __getitem__(self, index):
        return self.src_sql[index], self.tgt_sql[index], self.sql_tokens[index]

    def __len__(self):
        return len(self.src_sql)
