import os
import math
import pygam
import pickle
import numpy as np

from . import constants as my_constants
from .knobs_manager import Knobs


class Experience:
    def __init__(
        self,
        feature=None,
        importance=None,
        model=None,
        knobs=None,
        data_set=None,
        data_num=-1,
    ):
        self.feature = feature  # [select, update, insert, delete]
        self.importance = importance  # {knob: importance}
        self.model = model  # model object
        self.knobs = knobs
        self.data_set = data_set
        self.data_num = data_num  # number of data collected


class ExpPool:
    def __init__(self, path):
        self._experience_pool = []
        self._file_path = path
        self._knobs_info = Knobs(my_constants)
        if path is not None and os.path.exists(path):
            self._load()

    @property
    def experience_pool(self):
        return tuple(self._experience_pool)

    # matching knob importance according to feature
    def get_knobs_rank_similarity(self, feature, K):
        distance = []
        for exp in self.experience_pool:
            distance.append((exp, self._feature_dist(feature, exp.feature)))
        distance.sort(key=lambda x: -x[1])
        return distance[: max(K, len(distance))]

    def get_data_distr_similarity(self, experiences, sample_data):
        res = []
        for exp in experiences:
            res.append((exp, self._KP_dist(exp, sample_data)))
        return res

    # calculate the distance between feature1 and feature2
    def _feature_dist(self, feature1, feature2):
        assert len(feature1) == len(feature2)
        suid_keys = feature1[0].keys()
        opts_keys = feature1[1].keys()

        feature1_v = [feature1[0][x] for x in suid_keys] + [
            feature1[1][x] for x in opts_keys
        ]
        feature2_v = [feature2[0][x] for x in suid_keys] + [
            feature2[1][x] for x in opts_keys
        ]

        return 1 / math.sqrt(
            sum([(feature1_v[i] - feature2_v[i]) ** 2 for i in range(len(feature1_v))])
        )

    def _KP_dist(self, experience, samples):
        X, y, knobs = samples
        tmp = []

        for i in range(len(X)):
            tmp_dict = {}
            for j in range(X[i]):
                tmp_dict[knobs[j]] = X[i][j]
            tmp.append(tmp_dict)

        X = self._knobs_info.knob_align(tmp, experience.knobs)
        X = self._knobs_info.knob_normalization(X, experience.knobs)

        y_pred = experience.model.predict(X)
        model = pygam.LinearGAM(n_splines=10)
        model.fit(X, y)
        params1 = []
        for i, term in enumerate(model.terms):
            if term.isintercept:
                continue
            params1.extend(model.partial_dependence(term=i))

        model.fit(X, y_pred)
        params2 = []
        for i, term in enumerate(model.terms):
            if term.isintercept:
                continue
            params2.extend(model.partial_dependence(term=i))
        return np.dot(params1, params2) / (
            np.linalg.norm(params1) * np.linalg.norm(params2)
        )

    def _load(self):
        # load all experience
        with open(self._file_path, "rb") as f:
            self._experience_pool = pickle.load(f)

    def add(self, experience):
        self._experience_pool.append(experience)

    def save(self):
        with open(self._file_path, "wb+") as f:
            pickle.dump(self._experience_pool, f)
