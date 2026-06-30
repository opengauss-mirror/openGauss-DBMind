# Copyright (c) 2026 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
# See the Mulan PSL v2 for more details.

import os
import pickle
import shlex
import sys
import types

import numpy as np
import pytest


def _install_optional_dependency_stubs(monkeypatch):
    word2vec_module = types.ModuleType('gensim.models.word2vec')

    class DummyWord2Vec:
        @classmethod
        def load(cls, filepath):
            return cls()

    word2vec_module.Word2Vec = DummyWord2Vec

    gensim_module = types.ModuleType('gensim')
    gensim_models_module = types.ModuleType('gensim.models')
    gensim_models_module.word2vec = word2vec_module
    gensim_module.models = gensim_models_module

    sklearn_module = types.ModuleType('sklearn')
    sklearn_preprocessing_module = types.ModuleType('sklearn.preprocessing')

    class DummyMinMaxScaler:
        def __init__(self, feature_range=(0, 1)):
            self.feature_range = feature_range

    sklearn_preprocessing_module.MinMaxScaler = DummyMinMaxScaler
    sklearn_module.preprocessing = sklearn_preprocessing_module

    sqlparse_module = types.ModuleType('sqlparse')
    sqlparse_sql_module = types.ModuleType('sqlparse.sql')
    sqlparse_tokens_module = types.ModuleType('sqlparse.tokens')

    class Identifier:
        pass

    class IdentifierList:
        pass

    sqlparse_sql_module.Identifier = Identifier
    sqlparse_sql_module.IdentifierList = IdentifierList
    sqlparse_tokens_module.Keyword = object()
    sqlparse_tokens_module.DML = object()
    sqlparse_module.sql = sqlparse_sql_module
    sqlparse_module.tokens = sqlparse_tokens_module
    psycopg2_module = types.ModuleType('psycopg2')

    monkeypatch.setitem(sys.modules, 'gensim', gensim_module)
    monkeypatch.setitem(sys.modules, 'gensim.models', gensim_models_module)
    monkeypatch.setitem(sys.modules, 'gensim.models.word2vec', word2vec_module)
    monkeypatch.setitem(sys.modules, 'sklearn', sklearn_module)
    monkeypatch.setitem(sys.modules, 'sklearn.preprocessing', sklearn_preprocessing_module)
    monkeypatch.setitem(sys.modules, 'sqlparse', sqlparse_module)
    monkeypatch.setitem(sys.modules, 'sqlparse.sql', sqlparse_sql_module)
    monkeypatch.setitem(sys.modules, 'sqlparse.tokens', sqlparse_tokens_module)
    monkeypatch.setitem(sys.modules, 'psycopg2', psycopg2_module)


def _dnn_model_class(monkeypatch):
    _install_optional_dependency_stubs(monkeypatch)
    from dbmind.components.sqldiag.algorithm.duration_time_model.dnn import DnnModel

    return DnnModel


class DummyScaler:
    feature_range = (0, 1)
    data_min_ = np.asarray([1.0, 2.0])
    data_max_ = np.asarray([5.0, 8.0])
    data_range_ = np.asarray([4.0, 6.0])
    min_ = np.asarray([-0.25, -0.33333333])
    scale_ = np.asarray([0.25, 0.16666667])
    n_samples_seen_ = 3
    n_features_in_ = 2
    clip = False


def test_dnn_scaler_uses_safe_json_state(monkeypatch):
    DnnModel = _dnn_model_class(monkeypatch)

    payload = DnnModel._serialize_scaler(DummyScaler)
    restored = DnnModel._deserialize_scaler(payload)

    assert payload['version'] == 1
    assert restored.feature_range == (0, 1)
    np.testing.assert_allclose(restored.data_min_, DummyScaler.data_min_)
    np.testing.assert_allclose(restored.data_max_, DummyScaler.data_max_)
    np.testing.assert_allclose(restored.data_range_, DummyScaler.data_range_)
    np.testing.assert_allclose(restored.min_, DummyScaler.min_)
    np.testing.assert_allclose(restored.scale_, DummyScaler.scale_)
    assert restored.n_samples_seen_ == DummyScaler.n_samples_seen_
    assert restored.n_features_in_ == DummyScaler.n_features_in_
    assert restored.clip is False


def test_dnn_load_rejects_legacy_pickle_scaler(monkeypatch, tmp_path, caplog):
    DnnModel = _dnn_model_class(monkeypatch)
    marker = tmp_path / 'pickle_marker'

    class Payload:
        def __reduce__(self):
            command = "printf 'payload executed' > {}".format(shlex.quote(str(marker)))
            return (os.system, (command,))

    (tmp_path / 'dnn_model.h5').write_text('placeholder', encoding='utf-8')
    (tmp_path / 'w2v.model').write_text('placeholder', encoding='utf-8')
    with (tmp_path / 'scaler.pkl').open('wb') as fp:
        pickle.dump(Payload(), fp)

    params = types.SimpleNamespace(epoch=1)
    model = DnnModel(params)
    model.regression.load = lambda filepath: None
    model.w2v.load = lambda filepath: None

    with pytest.raises(SystemExit):
        model.load(str(tmp_path))

    assert not os.path.exists(str(marker))
    caplog.clear()
