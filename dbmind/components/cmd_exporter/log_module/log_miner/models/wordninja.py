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
import json
from math import log

from ..consts.constants import WORDNINJA_PATH


class WordNinja:
    def __init__(self):
        # Compound word segmentation dictionary
        try:
            with open(WORDNINJA_PATH, 'r') as file:
                words = json.loads(file.read())
        except FileNotFoundError as exception:
            raise exception
        except json.JSONDecodeError as exception:
            raise exception
        self._wordcost = dict((char, log((i + 1) * log(len(words)))) for i, char in enumerate(words))
        self._maxword = max(len(char) for char in words)

    def split(self, word):
        # Uses dynamic programming to infer the location of spaces in a string without spaces
        split_result = [self._split(split_re_word) for split_re_word in _SPLIT_RE.split(word)]
        return [item for sublist in split_result for item in sublist]

    def _split(self, word):
        # Find the best match for the index first characters
        def best_match(index):
            candidates = enumerate(reversed(cost[max(0, index - self._maxword):index]))
            return min(
                (candidate + self._wordcost.get(word[index - j - 1:index].lower(), 9e999), j + 1)
                for j, candidate in candidates
            )

        # Build the cost array
        cost = [0]
        for i in range(1, len(word) + 1):
            cost_i, _ = best_match(i)
            cost.append(cost_i)

        # Backtrack to recover the minimal-cost string
        out = []
        word_length = len(word)
        while word_length > 0:
            _, word_range = best_match(word_length)
            # Apostrophe and digit handling (added by Genesys)
            new_token = True
            # ignore a lone apostrophe
            if not word[word_length - word_range:word_length] == "'":
                if len(out) > 0:
                    # re-attach split 's and split digits
                    if out[-1] == "'s" or (word[word_length - 1].isdigit() and out[-1][0].isdigit()):
                        # combine current token with previous token
                        out[-1] = word[word_length - word_range:word_length] + out[-1]
                        new_token = False
            if new_token:
                out.append(word[word_length - word_range:word_length])
            word_length -= word_range
        return reversed(out)


DEFAULT_LANGUAGE_MODEL = WordNinja()
_SPLIT_RE = re.compile("[^a-zA-Z0-9']+")


def split(word):
    return DEFAULT_LANGUAGE_MODEL.split(word)
