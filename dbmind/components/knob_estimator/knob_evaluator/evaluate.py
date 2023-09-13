import os
import pickle
import logging
import numpy as np

from .utils import load_data
from ..collect import collect
from ..knobtool import constants as my_constants
from ..knobtool.experience import ExpPool
from ..knobtool.knobs_manager import Knobs


def evaluate(
    new_conf,
    old_conf,
    exp_num=1,
    use_exp=False,
    wkld_feature=None,
    model_path=None,
    config=None,
):
    def _predict(model, conf: dict, knobs_info: Knobs):
        X = knobs_info.knob_align([conf], model.feature_names)
        X = knobs_info.knob_normalization(X, model.feature_names)
        return model.predict(X)[0]

    knobs_info = Knobs(my_constants.DB_KNOBS_INFO)

    if use_exp:
        exp_pool = ExpPool(my_constants.CM_EXP_POOL)
        exps = exp_pool.get_knobs_rank_similarity(wkld_feature, exp_num)
        weights = np.array([x[1] for x in exps])
        weights = ((weights - weights.min()) / weights.max) * 0.9 + 0.1

        knob_importance = {}
        for i, exp in enumerate(exps):
            for k, v in exp.importance:
                knob_importance[k] = knob_importance.get(k, 0) + v * weights[i]
        important_knobs = sorted(
            [x[0] for x in knob_importance.items()], key=lambda x: -x[1]
        )[:6]
        data_file, _ = collect(
            config,
            result_path=os.path.join(
                config["collect"]["file_dir"],
                "evaluate_tmp.csv",
            ),
            candidates=important_knobs,
            size=10,
        )
        X, y, knobs, _ = load_data(data_file)
        os.remove(data_file)

        distr_sim = exp_pool.get_data_distr_similarity(exps, (X, y, knobs))
        weights = np.array([x[1] for x in exps])
        weights = ((weights - weights.min()) / weights.max) * 0.9 + 0.1

        pred_res_new = 0
        pred_res_old = 0
        for i in range(len(exps)):
            pred_res_new = pred_res_new + weights[i] * _predict(
                distr_sim[i][0], new_conf, knobs_info=knobs_info
            )
            pred_res_old = pred_res_old + weights[i] * _predict(
                distr_sim[i][0], old_conf, knobs_info=knobs_info
            )
    else:
        assert model_path is not None
        model = None
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        pred_res_new = _predict(model, new_conf, knobs_info=knobs_info)
        pred_res_old = _predict(model, old_conf, knobs_info=knobs_info)

    percentage = abs((pred_res_new - pred_res_old) / pred_res_old)
    if percentage < 0.1:
        str1 = "unchanged"
    elif pred_res_old < pred_res_new:
        str1 = "improved"
    else:
        str1 = "decreased"
    logging.info(
        f"We consider this change to be {str1}, old:{pred_res_old}, new:{pred_res_new}"
    )
