import math
import xgboost as xg

from .knob_evaluator.utils import load_data
from sklearn.linear_model import Lasso
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.gaussian_process.kernels import WhiteKernel, RBF
from sklearn.gaussian_process import GaussianProcessRegressor


def scale(weights, control=1):
    weights = [w if w > 0 else 0.001 for w in weights]
    weights = [math.pow(w * (1 / min(weights)), control) for w in weights]

    total = sum(weights)
    return [w / total for w in weights]


class RankInfo:
    def __init__(self, size, models, models_performance, models_rank) -> None:
        self.size = size
        self.models = models
        self.models_performance = models_performance
        self.models_rank = models_rank

    @property
    def models_list(self):
        return list(self.models.keys())

    @property
    def knobs_list(self):
        return [knob[0] for knob in list(self.models_rank.values())[0]]

    @property
    def rank(self):
        final_score_rank = {}
        model_coefficient = scale(
            list(
                map(lambda x: x if x > 0 else 0.001, self.models_performance.values())
            ),
            control=2,
        )
        for knob in self.knobs_list:
            tmp_score = 0
            for m_idx, rank in enumerate(self.models_rank.values()):
                rank = {name: value for name, value in rank}
                tmp_score += rank[knob] * model_coefficient[m_idx]
            final_score_rank[knob] = tmp_score

        sort_rank = sorted(final_score_rank.items(), key=lambda x: x[1], reverse=True)
        sort_rank = {name: value for name, value in sort_rank}
        return sort_rank


def create_rank_info(raw_data_path: str):
    models = {}
    models_performance = {}
    models_rank = {}
    X, y, knobs, _ = load_data(raw_data_path)
    y = y.reshape(-1, 1)
    data_size = len(y)

    watershed = int(0.7 * data_size)
    X, tX = X[:watershed], X[watershed:]
    y, ty = y[:watershed], y[watershed:]

    mms = MinMaxScaler()
    mms.fit(X)
    X, tX = mms.transform(X), mms.transform(tX)
    mms.fit(y)
    y, ty = mms.transform(y), mms.transform(ty)

    kernel = WhiteKernel() + RBF()
    gpr = GaussianProcessRegressor(kernel=kernel, random_state=0).fit(X, y)
    models["gpr"] = gpr
    models_performance["gpr"] = gpr.score(tX, ty)

    linear = Lasso(alpha=0.03).fit(X, y)
    models["lasso"] = linear
    models_performance["lasso"] = linear.score(tX, ty)

    xgb_r = xg.XGBRegressor(objective="reg:squarederror", n_estimators=10, seed=123)
    xgb_r.fit(X, y)
    models["xgb"] = xgb_r
    models_performance["xgb"] = xgb_r.score(tX, ty)

    ty, y = ty.reshape(
        -1,
    ), y.reshape(
        -1,
    )
    parameter_space = {
        "hidden_layer_sizes": [(100, 100, 100)],
        "activation": ["relu"],
        "solver": ["adam"],
        "alpha": [0.1, 0.2],
        "learning_rate": ["constant"],
    }

    mlp = MLPRegressor(hidden_layer_sizes=20, random_state=1, max_iter=5000)
    clf = GridSearchCV(mlp, parameter_space, n_jobs=4, cv=3)
    clf.fit(X, y)
    models["mlp"] = clf
    models_performance["mlp"] = clf.score(tX, ty)
    for name, model in models.items():
        from sklearn.inspection import permutation_importance

        r = permutation_importance(model, tX, ty, n_repeats=30, random_state=0)
        models_rank[name] = [
            (knobs[i], r.importances_mean[i]) for i in range(len(knobs))
        ]
    unit = RankInfo(
        size=data_size,
        models=models,
        models_performance=models_performance,
        models_rank=models_rank,
    )
    return unit


def rank(data_path):
    f = lambda x: [(name, round(value, 3)) for name, value in x.items()]
    rank_info = create_rank_info(data_path)
    return f(rank_info.rank)
