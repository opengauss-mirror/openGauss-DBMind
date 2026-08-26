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

import numpy as np

from dbmind.components.cluster_diagnosis.model import (
    cn_xgboost_model,
    dn_xgboost_model,
    cn_xgboost_model_export,
    dn_xgboost_model_export
)
from dbmind.components.cluster_diagnosis.utils import (
    CN_INPUT_ORDER,
    DN_INPUT_ORDER,
)

BASE_SCORE = 0.5


def cn_xgb_diagnosis(features):
    model = cn_xgboost_model or cn_xgboost_model_export
    input_array = [[features[name] for name in CN_INPUT_ORDER]]
    res = np.array(model.xgb_predict(input_array, BASE_SCORE))
    exp = np.exp(res)
    probabilities = (exp.T / exp.sum(axis=1)).T
    answers = res.argmax(axis=1)
    return [answers[i] for i, probability in enumerate(probabilities)]


def dn_xgb_diagnosis(features):
    model = dn_xgboost_model or dn_xgboost_model_export
    input_array = [[features[name] for name in DN_INPUT_ORDER]]
    res = np.array(model.xgb_predict(input_array, BASE_SCORE))
    exp = np.exp(res)
    probabilities = (exp.T / exp.sum(axis=1)).T
    answers = res.argmax(axis=1)
    return [answers[i] for i, probability in enumerate(probabilities)]
