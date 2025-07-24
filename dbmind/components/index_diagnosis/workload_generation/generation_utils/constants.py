# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
PAD = 0
UNK = 1
EOS = 2
BOS = 3
SEP = 4

WORD = {
    PAD: "<pad>",
    UNK: "<unk>",
    BOS: "<s>",
    EOS: "</s>",
    SEP: "<sep>"
}

operator = ["=", "!=", ">", "<", "<=", ">=", "<>"]
operator_vocab = ["=", "!=", ">", "<", "<=", ">="]
aggregator = ["max", "min", "count", "avg", "sum"]
order_by_key = ["DESC", "ASC"]
conjunction = ["AND", "OR"]

null_predicate = ["is null", "is not null"]
in_predicate = ["in", "not in"]
exists_predicate = ["exists", "not exists"]
like_predicate = ["like", "not like"]

join = ["JOIN", "ON"]
punctuation = ["(", ")", ",", " ", ";"]
keyword = ["select", "from", "aggregate", "where", "group by", "having", "order by"]
