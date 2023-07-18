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

import random
import numpy as np

import copy
import logging
from tqdm import tqdm

from workload_generation.generation_utils import constants

random.seed(666)
perturb_prop = 0.4


def valid_cand(token, table, step, ptok_nos, word2idx,
               idx2word, word_info, col_info, max_diff=5):
    if step >= len(token["pre_types"]):
        return [constants.PAD]

    cand = list()

    # 1) reserved: grammar keyword
    if token["pre_types"][step] in constants.keyword:
        cand = [word2idx[token["pre_tokens"][step].lower()]]
    elif token["pre_types"][step].upper() in constants.join:
        cand = [word2idx[token["pre_tokens"][step]]]

    # 2) tables
    elif "#join_table" in token["pre_types"][step]:
        cand = [word2idx[token["pre_tokens"][step]]]
    elif "#table" in token["pre_types"][step]:
        cand = [word2idx[tbl] for tbl in table if word2idx[tbl] not in ptok_nos]

    # 3) columns
    elif "#join_column" in token["pre_types"][step]:  # from/where
        cand = [word2idx[token["pre_tokens"][step].split(".")[-1]]]
    elif token["pre_types"][step] == "select#aggregate_column":
        for tbl in table:
            tbl_col = list(range(word_info[f"{tbl}#column name"]["start_id"],
                                 word_info[f"{tbl}#column name"]["end_id"] + 1))
            # 3.1) max()/min(): column of all types.
            if idx2word[str(ptok_nos[-1])] in constants.aggregator[:2]:
                cand.extend(tbl_col)
            # 3.2) count()/avg()/sum(): column of numeric types.
            elif idx2word[str(ptok_nos[-1])] in constants.aggregator[-3:]:
                cand.extend([col for col in tbl_col
                             if col_info[idx2word[str(col)]]["type"]
                             in ["integer", "numeric"]])
        # numeric aggregate column can be the same, filter columns
        # applied under the same aggregator selected already.
        cand_bak = copy.deepcopy(cand)
        cand = list(set(cand) - set([no for i, no in enumerate(ptok_nos)
                                     if token["pre_types"][i] == token["pre_types"][step]]))
        if len(cand) == 0:
            cand = list(set(cand_bak) - set([no for i, no in enumerate(ptok_nos)
                                             if token["pno_tokens"][i - 1] == token["pno_tokens"][step - 1]]))
    # special column (group by)
    elif token["pre_types"][step] == "group by#column":
        cand = list(set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "select#column"])
                    - set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "group by#column"]))
    # special column (having)
    elif token["pre_types"][step] == "having#aggregate_column":
        cand = list(set([tok_no for i, tok_no in enumerate(ptok_nos) if
                         i >= 1 and token["pre_types"][i - 1] == "select#aggregator" and
                         ptok_nos[i - 1] == ptok_nos[step - 1]])
                    - set([no for i, no in enumerate(ptok_nos) if
                           token["pre_types"][i] == "having#aggregate_column" and
                           ptok_nos[i - 1] == ptok_nos[step - 1]]))
    # special column (order by)
    elif token["pre_types"][step] == "order by#column":
        cand = list(set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "select#column"])
                    - set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "order by#column"]))
    elif token["pre_types"][step] == "order by#aggregate_column":
        cand = list(set([no for i, no in enumerate(ptok_nos) if
                         i >= 1 and token["pre_types"][i - 1] == "select#aggregator" and
                         ptok_nos[i - 1] == ptok_nos[step - 1]])
                    - set([no for i, no in enumerate(ptok_nos) if
                           token["pre_types"][i] == "order by#aggregate_column" and
                           ptok_nos[i - 1] == ptok_nos[step - 1]]))
    elif "#column" in token["pre_types"][step]:
        for tbl in table:
            cand.extend(list(range(word_info[f"{tbl}#column name"]["start_id"],
                                   word_info[f"{tbl}#column name"]["end_id"] + 1)))
        cand = list(set(cand) - set([no for i, no in enumerate(ptok_nos)
                                     if token["pre_types"][i] == token["pre_types"][step]]))

    # 4) values
    # 4.1) common values: column values and min()/max() aggregate values.
    elif "#value" in token["pre_types"][step] or \
            "#aggregate_value" in token["pre_types"][step]:
        cand = list(range(word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["start_id"],
                          word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["end_id"] + 1))

    # 4.2) special values: count()/avg()/sum() numeric aggregate values.
    elif len(ptok_nos) >= 3 and idx2word[str(ptok_nos[-3])] in constants.aggregator[2:] and \
            "#numeric_aggregate_value" in token["pre_types"][step]:
        cand = [constants.UNK]
    elif "#numeric_aggregate_value" in token["pre_types"][step]:
        cand = list(range(word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["start_id"],
                          word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["end_id"] + 1))

    # 5) operations
    elif "#join_operator" in token["pre_types"][step]:
        cand = [word2idx[token["pre_tokens"][step]]]
    elif "operator" in token["pre_types"][step]:
        cand = list(range(word_info["operator"]["start_id"],
                          word_info["operator"]["end_id"] + 1))

    # min/max
    elif token["pre_types"][step] == "select#aggregator" and \
            token["pre_tokens"][step].lower() in constants.aggregator[:2]:
        cand = list(range(word_info["aggregator"]["start_id"],
                          word_info["aggregator"]["start_id"] + 2))
    # count/avg/sum
    elif token["pre_types"][step] == "select#aggregator" and \
            token["pre_tokens"][step].lower() in constants.aggregator[2:]:
        cand = [word2idx[token["pre_tokens"][step].lower()]]

    elif token["pre_types"][step] == "having#aggregator":
        cand = [tok_no for i, tok_no in enumerate(ptok_nos)
                if token["pre_types"][i] == "select#aggregator"]
        existed = [tok_no for i, tok_no in enumerate(ptok_nos)
                   if token["pre_types"][i] == "having#aggregator"]
        for agg in existed:
            cand.remove(agg)
    elif "order by#aggregator" in token["pre_types"][step]:
        cand = [tok_no for i, tok_no in enumerate(ptok_nos)
                if token["pre_types"][i] == "select#aggregator"]
        existed = [tok_no for i, tok_no in enumerate(ptok_nos)
                   if token["pre_types"][i] == "order by#aggregator"]
        for agg in existed:
            cand.remove(agg)
    elif "conjunction" in token["pre_types"][step]:
        cand = list(range(word_info["conjunction"]["start_id"],
                          word_info["conjunction"]["end_id"] + 1))
    elif "order_by_key" in token["pre_types"][step]:
        cand = list(range(word_info["order by key"]["start_id"],
                          word_info["order by key"]["end_id"] + 1))

    # 6) predicate
    elif "null_predicate" in token["pre_types"][step]:
        cand = list(range(word_info["null"]["start_id"],
                          word_info["null"]["end_id"] + 1))
    elif "in_predicate" in token["pre_types"][step]:
        cand = list(range(word_info["in"]["start_id"],
                          word_info["in"]["end_id"] + 1))
    elif "exists_predicate" in token["pre_types"][step]:
        cand = list(range(word_info["exists"]["start_id"],
                          word_info["exists"]["end_id"] + 1))
    elif "like_predicate" in token["pre_types"][step]:
        cand = list(range(word_info["like"]["start_id"],
                          word_info["like"]["end_id"] + 1))

    # perturbation step constraint, already decoded words difference (not forcibly).
    #  group by / order by / having clause.
    if np.sum(np.array(token["pno_tokens"][:step]) != np.array(ptok_nos)) >= max_diff \
            and token["pno_tokens"][step] in cand:
        return [token["pno_tokens"][step]]
    else:
        return cand


def valid_cand_col(token, table, step, ptok_nos, column_left, word2idx,
                   idx2word, word_info, col_info, max_diff=5):
    # time-step exceed the max_len.
    if step >= len(token["pre_types"]):
        return [constants.PAD]

    # only the value is associated with the column selected.
    if "#column" not in token["pre_types"][step] and \
            "#aggregate_column" not in token["pre_types"][step] and \
            "#value" not in token["pre_types"][step] and \
            "#aggregate_value" not in token["pre_types"][step] and \
            "#numeric_aggregate_value" not in token["pre_types"][step]:
        return [token["pno_tokens"][step]]

    cand = list()

    # 3) columns
    # special column (aggregate)
    #  type for min()/avg()/count()
    if token["pre_types"][step] == "select#aggregate_column":
        # 3.1) max()/min(): column of all types.
        if idx2word[str(ptok_nos[-1])] in constants.aggregator[:2]:
            cand.extend(column_left)

        # filter columns selected in the same clause already.
        cand = list(set(cand) - set([no for i, no in enumerate(ptok_nos)
                                     if token["pre_types"][i] == token["pre_types"][step]]))
        if len(cand) == 0:
            cand = list(set([no for i, no in enumerate(token["pno_tokens"])
                             if token["pno_tokens"][i - 1] == token["pno_tokens"][step - 1]])
                        - set([no for i, no in enumerate(ptok_nos)
                               if ptok_nos[i - 1] == ptok_nos[step - 1]]))
    # special column (group by)
    elif token["pre_types"][step] == "group by#column":
        cand = list(set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "select#column"])
                    - set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "group by#column"]))
    # special column (having)
    elif token["pre_types"][step] == "having#aggregate_column":
        cand = list(set([tok_no for i, tok_no in enumerate(ptok_nos) if
                         i >= 1 and token["pre_types"][i - 1] == "select#aggregator" and
                         ptok_nos[i - 1] == ptok_nos[step - 1]])
                    - set([no for i, no in enumerate(ptok_nos) if
                           token["pre_types"][i] == "having#aggregate_column" and
                           ptok_nos[i - 1] == ptok_nos[step - 1]]))
    # special column (order by)
    elif token["pre_types"][step] == "order by#column":
        cand = list(set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "select#column"])
                    - set([no for i, no in enumerate(ptok_nos) if token["pre_types"][i] == "order by#column"]))
    elif token["pre_types"][step] == "order by#aggregate_column":
        cand = list(set([no for i, no in enumerate(ptok_nos) if
                         i >= 1 and token["pre_types"][i - 1] == "select#aggregator" and
                         ptok_nos[i - 1] == ptok_nos[step - 1]])
                    - set([no for i, no in enumerate(ptok_nos) if
                           token["pre_types"][i] == "order by#aggregate_column" and
                           ptok_nos[i - 1] == ptok_nos[step - 1]]))
    # repeated column
    elif "#column" in token["pre_types"][step]:  # select/where
        cand = list(set(column_left) - set([no for i, no in enumerate(ptok_nos)
                                            if token["pre_types"][i] == token["pre_types"][step]]))
        if len(cand) == 0 and token["pre_types"][step] == "where#column":
            cand = list(set([no for i, no in enumerate(ptok_nos)
                             if token["pre_types"][i] == "select#column"
                             or token["pre_types"][i] == "select#aggregate_column"])
                        - set([no for i, no in enumerate(ptok_nos)
                               if token["pre_types"][i] == "where#column"]))

    # 4) values
    # 4.1) common values: column values and min()/max() aggregate values.
    elif "#value" in token["pre_types"][step] or \
            "#aggregate_value" in token["pre_types"][step]:
        cand = list(range(word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["start_id"],
                          word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["end_id"] + 1))

    # 4.2) special values: count()/avg()/sum() numeric aggregate values.
    elif len(ptok_nos) >= 3 and idx2word[str(ptok_nos[-3])] in constants.aggregator[2:] and \
            "#numeric_aggregate_value" in token["pre_types"][step]:
        cand = [constants.UNK]
    elif "#numeric_aggregate_value" in token["pre_types"][step]:
        cand = list(range(word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["start_id"],
                          word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["end_id"] + 1))

    # perturbation step constraint, already decoded words difference (not forcibly).
    #  group by / order by / having clause.
    if np.sum(np.array(token["pno_tokens"][:step]) != np.array(ptok_nos)) >= max_diff \
            and token["pno_tokens"][step] in cand:
        return [token["pno_tokens"][step]]
    else:
        return cand


def valid_cand_val(token, table, step, ptok_nos, word2idx,
                   idx2word, word_info, col_info, max_diff=5):
    # time-step exceed the max_len.
    if step >= len(token["pre_types"]):
        return [constants.PAD]

    if "#value" not in token["pre_types"][step] and \
            "#aggregate_value" not in token["pre_types"][step] and \
            "#numeric_aggregate_value" not in token["pre_types"][step]:
        return [token["pno_tokens"][step]]

    cand = list()

    # 1) values
    # 1.1) common values: column values and min()/max() aggregate values.
    if "#value" in token["pre_types"][step] or \
            "#aggregate_value" in token["pre_types"][step]:
        try:
            cand = list(range(word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["start_id"],
                              word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["end_id"] + 1))
        except:
            logging.error("The current token type is `#value/#aggregate_value`.")
    # 1.2) special values: count()/avg()/sum() numeric aggregate values.
    elif idx2word[str(ptok_nos[-3])] in constants.aggregator[2:] and \
            "#numeric_aggregate_value" in token["pre_types"][step]:
        cand = [constants.UNK]
    elif "#numeric_aggregate_value" in token["pre_types"][step]:
        try:
            cand = list(range(word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["start_id"],
                              word_info[f"{idx2word[str(ptok_nos[-2])]}#column values"]["end_id"] + 1))
        except:
            logging.error("The current token type is `#numeric_aggregate_value`.")

    # perturbation step constraint, already decoded words difference (not forcibly).
    #  group by / order by / having clause.
    if np.sum(np.array(token["pno_tokens"][:step]) != np.array(ptok_nos)) >= max_diff \
            and token["pno_tokens"][step] in cand:
        return [token["pno_tokens"][step]]
    else:
        return cand


def sql2vec(sql_tokens, word2idx):
    vectors = list()
    for item in sql_tokens:
        vec = list()
        for i in range(len(item["pre_tokens"])):
            key = str(item['pre_tokens'][i]).strip("'").strip(" ")
            if "operator" in item["pre_types"][i] and \
                    item["pre_tokens"][i] == "<>":
                key = "!="
            elif "aggregator" in item["pre_types"][i]:
                key = key.lower()
            elif "column" in item["pre_types"][i]:
                # `table`.`column`
                key = key.split(".")[-1]
            # newly added for `<unk>`.
            elif "#numeric_aggregate_value" in item["pre_types"][i]:
                key = "<unk>"
            elif "value" in item["pre_types"][i]:
                key = f"{item['pre_types'][i].split('#')[0]}#_#{key}"

            vec.append(word2idx.get(key, 1))  # <unk>: 1

        vectors.append(vec)

    return vectors


def vec2sql(sql_tokens, sql_vectors, idx2word, col_info, mode="without_table"):
    columns = list(col_info.keys())
    tables = list(set([col_info[key]["table"] for key in col_info]))

    sql_res = list()
    for tok, vec in zip(sql_tokens, sql_vectors):
        res = {"sql_text": "", "sql_token": list(), "pno_tokens": list(map(int, vec))}
        for to, no in zip(tok["pre_tokens"], vec):
            # Filter the special token.
            if no in [constants.BOS, constants.EOS, constants.PAD]:
                continue

            pre1, pre2, pre3 = "", "", ""
            if len(res["sql_token"]) - 1 > 0:
                pre1 = res["sql_token"][len(res["sql_token"]) - 1]
            if len(res["sql_token"]) - 2 > 0:
                pre2 = res["sql_token"][len(res["sql_token"]) - 2]
            if len(res["sql_token"]) - 3 > 0:
                pre3 = res["sql_token"][len(res["sql_token"]) - 3]

            cur = idx2word[str(no)]
            # for numeric aggregate value
            if cur == "<unk>":
                cur = to
            res["sql_token"].append(cur)

            if pre1 in constants.aggregator and cur in columns:
                if mode == "with_table":
                    cur = f"({col_info[cur]['table']}.{cur})"
                else:
                    cur = f"({cur})"
            # constants.operator[:2] -> constants.aggregator[:2]
            elif pre3 in constants.aggregator[:2] and \
                    pre2 in columns and \
                    cur in col_info[pre2]["value"] and \
                    col_info[pre2]["type"] not in ["integer", "numeric"]:
                cur = f"'{cur}'"
            # constants.operator -> constants.aggregator
            elif pre3 not in constants.aggregator and \
                    pre2 in columns and \
                    cur in col_info[pre2]["value"] and \
                    col_info[pre2]["type"] not in ["integer", "numeric"]:
                cur = f"'{cur}'"

            if (pre1 in columns or pre1 in tables) and \
                    (cur in columns or cur in tables):
                if cur in columns and mode == "with_table":
                    res["sql_text"] += f", {col_info[cur]['table']}.{cur}"
                else:
                    res["sql_text"] += f", {cur}"
            elif pre1 in columns and cur in constants.aggregator:
                res["sql_text"] += f", {cur}"
            elif pre1 in constants.order_by_key and \
                    (cur in columns or cur in constants.aggregator):
                if cur in columns and mode == "with_table":
                    res["sql_text"] += f", {col_info[cur]['table']}.{cur}"
                else:
                    res["sql_text"] += f", {cur}"
            elif pre1 in constants.aggregator:
                res["sql_text"] += cur
            else:
                if cur in columns and mode == "with_table":
                    res["sql_text"] += f" {col_info[cur]['table']}.{cur}"
                else:
                    res["sql_text"] += f" {cur}"

        res["sql_text"] = res["sql_text"].strip(" ")
        sql_res.append(res)

    return sql_res


def random_gen(sql_token, word2idx, idx2word, word_info,
               col_info, mode="value", max_diff=5, perturb_prop=0.5, seed=666):
    random.seed(seed)
    valid_tokens, except_tokens, sql_vectors = list(), list(), list()
    for ino, token in tqdm(enumerate(sql_token)):
        vec = token["pno_tokens"]
        table = [token["pre_tokens"][i] for i, typ in
                 enumerate(token["pre_types"]) if "table" in typ]
        column_left = [token["pno_tokens"][i] for i, typ in
                       enumerate(token["pre_types"]) if
                       typ == "select#column" or
                       (typ == "select#aggregate_column" and
                        idx2word[str(token["pno_tokens"][i - 1])] in constants.aggregator[:2]) or
                       typ == "where#column"]
        sql_tok = list()
        for step in range(len(token["pre_types"])):
            if mode == "all":
                cand = valid_cand(token, table, step, sql_tok, word2idx,
                                  idx2word, word_info, col_info, max_diff=max_diff)
            elif mode == "column":
                cand = valid_cand_col(token, table, step, sql_tok, column_left, word2idx,
                                      idx2word, word_info, col_info, max_diff=max_diff)
            elif mode == "value":
                cand = valid_cand_val(token, table, step, sql_tok, word2idx,
                                      idx2word, word_info, col_info, max_diff=max_diff)

            if random.uniform(0, 1) > perturb_prop and vec[step] in cand:
                selected = vec[step]
                sql_tok.append(selected)
                if selected in column_left and \
                        (token["pre_types"][step] == "select#column" or
                         (token["pre_types"][step] == "select#aggregate_column" and
                          idx2word[str(token["pno_tokens"][step - 1])] in constants.aggregator[:2]) or
                         token["pre_types"][step] == "where#column"):
                    column_left.remove(selected)
            else:
                selected = random.choice(cand)
                sql_tok.append(selected)
                if selected in column_left and \
                        (token["pre_types"][step] == "select#column" or
                         (token["pre_types"][step] == "select#aggregate_column" and
                          idx2word[str(token["pno_tokens"][step - 1])] in constants.aggregator[:2]) or
                         token["pre_types"][step] == "where#column"):
                    column_left.remove(selected)
        valid_tokens.append(token)
        sql_vectors.append(sql_tok)

    return valid_tokens, except_tokens, sql_vectors
