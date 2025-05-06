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

# According to actual experience, the predicted result
# will be more perfect after the amount of data
# (length of the sequence) exceeds a certain level,
# so we set a threshold here based on experience
# to decide different detection behaviors.

THRESHOLD = {
    "positive": (0.0, -float("inf")),
    "negative": (float("inf"), 0.0),
    "both": (-float("inf"), float("inf")),
    "neither": (float("inf"), -float("inf"))
}
