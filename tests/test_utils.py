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
import os
import threading
import time
import logging
from unittest import mock

import pytest

import dbmind.common.utils.cli
from dbmind.common import utils
from dbmind.common.utils import cast_to_int_or_float, dbmind_assert, split, string_to_dict, try_to_get_an_element, \
    adjust_timezone
from dbmind.constants import METRIC_MAP_CONFIG, MISC_PATH

from .conftest import assert_raise

CURR_DIR = os.path.realpath(os.path.dirname(__file__))

FAKE_LOG = {"path": ""}


@pytest.fixture(scope='function', autouse=True)
def initialize_fake_log():
    """ Create a empty file stream as /dev/null. Recover it after this test."""
    path = os.path.abspath(os.path.dirname(__file__))
    fake_log = os.path.join(path, "fake_log")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        with os.fdopen(os.open(fake_log, flags, 0o600), 'w'):
            pass

        os.chmod(fake_log, 0o600)
    except FileExistsError:
        pass

    FAKE_LOG["path"] = fake_log

    yield

    # Recovery
    if os.path.exists(fake_log):
        os.remove(fake_log)


def test_read_simple_conf_file():
    conf = utils.read_simple_config_file(
        os.path.join(MISC_PATH, METRIC_MAP_CONFIG)
    )

    assert len(conf) > 0
    for name, value in conf.items():
        assert not name.startswith('#')


def test_write_to_terminal():
    dbmind.common.utils.cli.write_to_terminal(1111)
    dbmind.common.utils.cli.write_to_terminal(111, level='error', color='red')
    dbmind.common.utils.cli.write_to_terminal('hello world', color='yellow')


__results = []


@utils.ttl_cache(0.1, 4)
def need_cache_func(a, b):
    __results.append((a, b))
    return a * b


def test_ttl_cache():
    __inner_results = []
    for i in range(4):
        __inner_results.append(need_cache_func(1, i))
        __inner_results.append(need_cache_func(1, i))
    for i in range(4):
        __inner_results.append(need_cache_func(1, i))
        __inner_results.append(need_cache_func(1, i))
    time.sleep(0.1)
    for i in range(4):
        __inner_results.append(need_cache_func(1, i))
        __inner_results.append(need_cache_func(1, i))

    assert __inner_results == [0, 0, 1, 1, 2, 2, 3, 3, 0, 0, 1, 1, 2, 2, 3, 3, 0, 0, 1, 1, 2, 2, 3, 3]
    assert __results == [(1, 0), (1, 1), (1, 2), (1, 3), (1, 0), (1, 1), (1, 2), (1, 3)]


def test_ttl_ordered_dict():
    d = utils.TTLOrderedDict(0.1)
    d['a'] = 1
    d['b'] = 2
    time.sleep(0.2)
    assert 'a' not in d
    assert d.get('b') is None
    assert len(d) == 0
    d['a'] = 2
    assert d['a'] == 2

    def update(start):
        for i in range(10):
            d[start + i] = i

    t1 = threading.Thread(target=update, args=(0,))
    t2 = threading.Thread(target=update, args=(5,))
    t1.start()
    t2.start()
    assert len(d) == 16
    t1.join()
    t2.join()

    time.sleep(0.5)
    assert len(d) == 0


def test_naive_queue():
    q = utils.NaiveQueue(8)
    for i in range(10):
        q.put(i)

    for i in range(8):
        assert q.get() == 2 + i

    def insert1():
        for i_ in range(4):
            q.put(i_)
        for i_ in q:
            assert i_ >= 0

    def insert2():
        for i_ in range(4):
            q.put(i_)

    t1 = threading.Thread(target=insert1)
    t2 = threading.Thread(target=insert2)
    t1.start()
    t2.start()

    assert len(q) == 8

    assert list(q) == [0, 1, 2, 3, 0, 1, 2, 3]


def test_fixed_dict():
    d = utils.base.FixedDict(max_len=3)
    d['a'] = 1
    d['b'] = 2
    d['c'] = 3

    # Test case 1: Add elements up to maximum length
    assert len(d) == 3
    assert list(d.items()) == [('a', 1), ('b', 2), ('c', 3)]

    # Test case 2: Add elements beyond maximum length
    d['d'] = 4
    assert len(d) == 3
    assert list(d.items()) == [('b', 2), ('c', 3), ('d', 4)]

    # Test case 3: Add elements with different types of keys and values
    d['e'] = 'five'
    d[6] = 'six'
    assert len(d) == 3
    assert list(d.items()) == [('d', 4), ('e', 'five'), (6, 'six')]

    # Test case 4: Access elements by key
    assert d['d'] == 4
    assert d[6] == 'six'

    # Test case 5: Check maximum length attribute
    assert d.max_len == 3


def test_cast_to_int_or_float():
    assert cast_to_int_or_float(1) == 1
    assert cast_to_int_or_float(1.0) == 1.0
    assert cast_to_int_or_float('1') == 1
    assert cast_to_int_or_float('1.0') == 1.0


def test_dbmind_assert():
    with pytest.raises(AssertionError) as assert_err:
        dbmind_assert(condition=None, comment=None)
    assert assert_err.type == AssertionError

    with pytest.raises(ValueError) as value_err:
        dbmind_assert(condition=None, comment='msg')
    assert value_err.type == ValueError


def test_split():
    assert split('') == []
    assert split('a,b,c') == ['a', 'b', 'c']


def test_string_to_dict():
    assert string_to_dict('a=1,b=2,c=3') == {'a': '1', 'b': '2', 'c': '3'}


def test_try_to_get_an_element():
    assert try_to_get_an_element([], 0) is None
    assert try_to_get_an_element([1, 2, 3], 5) == 1
    assert try_to_get_an_element([1, 2, 3], 1) == 2


def test_adjust_timezone():
    assert str(adjust_timezone('UTC-8')) == 'UTC-08:00'
    assert str(adjust_timezone('UTC+8')) == 'UTC+08:00'
    assert str(adjust_timezone('UTC+8:00')) == 'UTC+08:00'
    assert str(adjust_timezone('UTC+8:30')) == 'UTC+08:30'
    assert adjust_timezone('UTC+8:300') is None
    assert adjust_timezone('UTC+8::30') is None
    assert adjust_timezone('ABC+8:30') is None


def test_where_am_i():
    assert utils.where_am_i(globals()) == 'tests.test_utils'


def test_mp_rf_handler():
    logger = logging.getLogger()
    logging_handler = utils.MultiProcessingRFHandler(filename=FAKE_LOG["path"], maxBytes=100, backupCount=1)
    logging_handler.add_sensitive_word('w1')
    logging_handler.add_sensitive_word(['w2', 'w3'])
    logger.addHandler(logging_handler)
    logger.setLevel('DEBUG')
    logging.info("logging content")


@utils.ExceptionCatcher(strategy='raise', name='f1')
def exception_func1():
    raise ValueError("Wrong value type in f1")


@utils.ExceptionCatcher(strategy='ignore', name='f2')
def exception_func2():
    raise ValueError("Wrong value type in f2")


@utils.ExceptionCatcher(strategy='sensitive', name='f3')
def exception_func3():
    raise ValueError("password_is tom")


@utils.ExceptionCatcher(strategy='sensitive', name='f3')
def exception_func4():
    raise ValueError("username is tom")


def test_exception_catcher():
    assert_raise(ValueError, exception_func1)
    assert exception_func2() is None
    assert_raise(AssertionError, exception_func3)
    assert_raise(ValueError, exception_func4)


def test_retry():
    count = 0

    @utils.base.retry(times_limit=2)
    def retry_func():
        nonlocal count
        count = count + 1
        raise ValueError('error')
    assert_raise(ValueError, retry_func)
    assert count == 2


@pytest.fixture(autouse=False, name="new_env")
def mock_os_get_env(monkeypatch):
    env_choices = {'env1': None, 'env2': '!@#${} ()'}
    monkeypatch.setattr(os, 'getenv', mock.Mock(side_effect=lambda x: env_choices[x]))


def test_get_env(new_env):
    assert utils.get_env('env1') is None
    assert utils.get_env('env1', 'val1') == 'val1'
    assert_raise(Exception, utils.get_env, os.getenv('env2'))
