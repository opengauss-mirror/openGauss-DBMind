# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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

import re

INDENT = "    "


def line_parser(line):
    tabs = re.findall(r"\t+", line)
    tab_space = ''.join([INDENT * tab.count("\t") for tab in tabs])
    if r":leaf=" in line:
        out = re.findall(r"[+-]?\d+[.]?\d*e?[+-]?\d*", line)
        if tabs:
            return (INDENT + tab_space + 'if state == ' + out[0] + ':\n' +
                    INDENT * 2 + tab_space + 'return ' + out[1] + '\n')
        else:
            return INDENT * 2 + 'return ' + out[1] + '\n'
    else:
        out = re.findall(r"[\w.-]+", line)
        idx = out[1][1:]
        state = out[0]
        if out[4] == out[8]:
            missing_value_handling = (" or np.isnan(row[" + idx + "])")
        else:
            missing_value_handling = ""

        if state == '0':
            return (INDENT * 2 + tab_space + 'state = (' + out[4] +
                    ' if ' + "row[" + idx + "] < " + out[2] + missing_value_handling +
                    ' else ' + out[6] + ')\n')
        else:
            return (INDENT + tab_space + 'if state == ' + out[0] + ':\n' +
                    INDENT * 2 + tab_space + 'state = (' + out[4] +
                    ' if ' + "row[" + idx + "] < " + out[2] + missing_value_handling +
                    ' else ' + out[6] + ')\n')


def tree_parser(trees, i):
    lines = trees.split('\n')
    n_lines = len(lines)
    if i == 0:
        return (INDENT + 'if num_booster == 0:\n'
                + "".join([line_parser(lines[i]) for i in range(n_lines - 1)])
                + "\n")
    else:
        return (INDENT + 'elif num_booster == ' + str(i) + ':\n'
                + "".join([line_parser(lines[i]) for i in range(n_lines - 1)])
                + "\n")


def model_to_py(n_estimators, model, out_file):
    trees = model.get_booster().get_dump()
    result = ["import numpy as np\n\n\n"
              + "def xgb_tree(row, num_booster):\n"]

    for i in range(len(trees)):
        result.append(tree_parser(trees[i], i))

    with open(out_file, 'w') as the_file:
        the_file.write(
            "".join(result) + "\n" +
            "def xgb_forward(row, base_score):\n" +
            INDENT + f"probs = list()\n" +
            INDENT + f"for i in range({len(trees) // n_estimators}):\n" +
            INDENT * 2 + f"predict = base_score\n" +
            INDENT * 2 + f"# initialize prediction with base score\n" +
            INDENT * 2 + f"for j in range(i, {len(trees)}, {len(trees) // n_estimators}):\n" +
            INDENT * 3 + "predict += xgb_tree(row, j)\n" +
            INDENT * 2 + "probs.append(predict)\n" +
            INDENT + "return probs\n\n\n" +
            "def xgb_predict(x, base_score):\n" +
            INDENT + "return [xgb_forward(row, base_score) for row in x]" + "\n"
        )
