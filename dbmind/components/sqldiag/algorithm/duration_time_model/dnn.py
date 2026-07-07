import json
import logging
import os
import stat
import sys
from abc import ABC

import numpy as np

from . import AbstractModel
from ..word2vec import Word2Vector
from ...preprocessing import templatize_sql
from ...utils import check_illegal_sql


class KerasRegression:
    def __init__(self, encoding_dim=1):
        self.model = None
        self.encoding_dim = encoding_dim

    @staticmethod
    def build_model(shape, encoding_dim):
        from tensorflow.keras import Input, Model
        from tensorflow.keras.layers import Dense
        inputs = Input(shape=(shape,))
        layer_dense1 = Dense(128, activation='relu', kernel_initializer='he_normal')(inputs)
        layer_dense2 = Dense(256, activation='relu', kernel_initializer='he_normal')(layer_dense1)
        layer_dense3 = Dense(256, activation='relu', kernel_initializer='he_normal')(layer_dense2)
        layer_dense4 = Dense(256, activation='relu', kernel_initializer='he_normal', name='vectors')(layer_dense3)
        y_pred = Dense(encoding_dim)(layer_dense4)
        model = Model(inputs=inputs, outputs=y_pred)
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        return model

    def fit(self, features, labels, batch_size=128, epochs=300):
        shape = features.shape[1]
        if self.model is None:
            self.model = self.build_model(shape=shape, encoding_dim=self.encoding_dim)
        self.model.fit(features, labels, epochs=epochs, batch_size=batch_size, shuffle=True, verbose=0)

    def predict(self, features):
        predict_result = self.model.predict(features)
        return predict_result

    def save(self, filepath):
        self.model.save(filepath)

    def load(self, filepath):
        from tensorflow.keras.models import load_model
        self.model = load_model(filepath)


class DnnModel(AbstractModel, ABC):
    def __init__(self, params):
        super().__init__(params)
        self.w2v_model_parameter = {'max_len': 150,
                                    'sg': 1,
                                    'hs': 1,
                                    'min_count': 0,
                                    'window': 1,
                                    'size': 5,
                                    'iter': 30,
                                    'workers': 8
                                    }
        self.w2v = Word2Vector(**self.w2v_model_parameter)
        self.epoch = params.epoch
        self.scaler = None
        self.regression = KerasRegression(encoding_dim=1)
        self.data = None

    def build_word2vector(self, data):
        self.data = list(data)
        if self.w2v.model:
            self.w2v.update(self.data)
        else:
            self.w2v.fit(self.data)

    def fit(self, data):
        from sklearn.preprocessing import MinMaxScaler
        self.build_word2vector(data)
        list_vec = []
        list_cost = []
        for sql, duration_time in self.data:
            if check_illegal_sql(sql):
                continue
            filter_template = templatize_sql(sql)
            vector = self.w2v.str2vec(filter_template)
            list_vec.append(vector)
            list_cost.append(duration_time)

        features = np.array(list_vec)
        labels = np.array(list_cost)

        labels = labels.reshape(-1, 1)
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.scaler.fit(labels)
        labels = self.scaler.transform(labels)
        self.regression.fit(features, labels, epochs=self.epoch)

    def transform(self, data):

        feature_list = []
        data_backup = list(data)
        error_list = []
        for idx_error, sql in enumerate(data_backup):
            if check_illegal_sql(sql):
                error_list.append(idx_error)
                continue
            filter_template = templatize_sql(sql)
            vector = self.w2v.str2vec(filter_template)
            feature_list.append(vector)

        features = np.array(feature_list)
        predictions = self.regression.predict(features)
        predictions = np.abs(predictions)
        score = self.scaler.inverse_transform(predictions)
        if error_list:
            for item in error_list:
                score = np.insert(score, item, -1)
        score = np.hstack((np.array(data_backup).reshape(-1, 1), score.reshape(-1, 1))).tolist()
        return score

    @staticmethod
    def _serialize_scaler(scaler):
        if scaler is None:
            return None
        return {
            'version': 1,
            'feature_range': list(scaler.feature_range),
            'data_min_': scaler.data_min_.tolist(),
            'data_max_': scaler.data_max_.tolist(),
            'data_range_': scaler.data_range_.tolist(),
            'min_': scaler.min_.tolist(),
            'scale_': scaler.scale_.tolist(),
            'n_samples_seen_': int(scaler.n_samples_seen_),
            'n_features_in_': int(getattr(scaler, 'n_features_in_', len(scaler.scale_))),
            'clip': bool(getattr(scaler, 'clip', False)),
        }

    @staticmethod
    def _deserialize_scaler(payload):
        if payload is None:
            return None
        from sklearn.preprocessing import MinMaxScaler

        scaler = MinMaxScaler(feature_range=tuple(payload['feature_range']))
        scaler.data_min_ = np.asarray(payload['data_min_'])
        scaler.data_max_ = np.asarray(payload['data_max_'])
        scaler.data_range_ = np.asarray(payload['data_range_'])
        scaler.min_ = np.asarray(payload['min_'])
        scaler.scale_ = np.asarray(payload['scale_'])
        scaler.n_samples_seen_ = int(payload['n_samples_seen_'])
        scaler.n_features_in_ = int(payload['n_features_in_'])
        scaler.clip = bool(payload.get('clip', False))
        return scaler

    def load(self, filepath):
        realpath = os.path.realpath(filepath)
        if os.path.exists(realpath):
            dnn_path = os.path.join(realpath, 'dnn_model.h5')
            word2vector_path = os.path.join(realpath, 'w2v.model')
            scaler_path = os.path.join(realpath, 'scaler.json')
            legacy_scaler_path = os.path.join(realpath, 'scaler.pkl')
            self.regression.load(dnn_path)
            self.w2v.load(word2vector_path)
            if os.path.exists(scaler_path):
                with open(scaler_path, mode='r', encoding='utf-8') as f:
                    self.scaler = self._deserialize_scaler(json.load(f))
            elif os.path.exists(legacy_scaler_path):
                logging.error(
                    'Legacy pickle-based scaler artifact %s is no longer supported; '
                    'please resave the model.',
                    legacy_scaler_path,
                )
                sys.exit(1)
            else:
                logging.error("{} not exist.".format(realpath))
                sys.exit(1)
        else:
            logging.error("{} not exist.".format(realpath))
            sys.exit(1)

    def save(self, filepath):
        realpath = os.path.realpath(filepath)
        if not os.path.exists(realpath):
            os.makedirs(realpath, mode=0o700)
        if oct(os.stat(realpath).st_mode)[-3:] != '700':
            os.chmod(realpath, stat.S_IRWXU)
        dnn_path = os.path.join(realpath, 'dnn_model.h5')
        word2vector_path = os.path.join(realpath, 'w2v.model')
        scaler_path = os.path.join(realpath, 'scaler.json')
        self.regression.save(dnn_path)
        self.w2v.save(word2vector_path)
        with open(scaler_path, mode='w', encoding='utf-8') as f:
            json.dump(self._serialize_scaler(self.scaler), f)
        if os.path.exists(dnn_path):
            os.chmod(dnn_path, 0o600)
        if os.path.exists(word2vector_path):
            os.chmod(word2vector_path, 0o600)
        if os.path.exists(scaler_path):
            os.chmod(scaler_path, 0o600)
        print("DNN model is stored in '{}'".format(realpath))
