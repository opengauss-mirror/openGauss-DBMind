import os
import csv
import pickle
import numpy as np

from ..knobtool import constants as my_constants
from ..knobtool.knobs_manager import Knobs
from .rule_fit_model import RuleFit
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor


def load_data(data_path, normalize=False, logarithmic=False):
    knobs = None
    X = []
    y = []
    with open(data_path, "r") as file:
        reader = csv.reader(file)
        cols = next(reader)
        knobs, metric = cols[1:-1], cols[-1]
        for row in reader:
            X.append(row[1:-1])
            y.append(row[-1])
    X, y = np.array(X), np.array(y)
    knob_info = Knobs(knobs_csv_path=my_constants.DB_KNOBS_INFO)
    X = knob_info.enum_turn(X)
    X, y = X.astype(np.float64), y.astype(np.float64)
    if normalize:
        X = knob_info.knob_normalization(X, knobs)
        if logarithmic:
            y[y < 1] = 1.0
            y = np.log(y)
        scaler = MinMaxScaler(feature_range=(0.1, 1))
        scaler.fit([[max(y)], [min(y)]])
        y_t = scaler.transform(y.reshape(-1, 1))
        return X, y_t.reshape(y.shape), knobs, metric
    return X, y, knobs, metric


def load_model(restore=False, model_path=None):
    if not restore:
        return RuleFit(tree_generator=RandomForestRegressor(max_depth=2))
    if os.path.exists(str(model_path)):
        with open(model_path, "rb") as file:
            rf = pickle.load(file)
        return rf


def save_model(model_path, model):
    pickle.dump(model, open(model_path, "wb"))


def train_model(x_train, y_train, model, knobs):
    model.fit(x_train, y_train, feature_names=knobs)
    return model
