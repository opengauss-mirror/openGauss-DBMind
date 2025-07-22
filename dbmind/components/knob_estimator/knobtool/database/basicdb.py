# Copyright 2001-2017 by Vinay Sajip. All Rights Reserved.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appear in all copies and that
# both that copyright notice and this permission notice appear in
# supporting documentation, and that the name of Vinay Sajip
# not be used in advertising or publicity pertaining to distribution
# of the software without specific, written prior permission.
# VINAY SAJIP DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING
# ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
# VINAY SAJIP BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR
# ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
# IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
import threading, abc

_lock = threading.RLock()


def _acquireLock():
    """
    Acquire the module-level lock for serializing access to shared data.

    This should be released with _releaseLock().
    """
    if _lock:
        _lock.acquire()


def _releaseLock():
    """
    Release the module-level lock acquired by calling _acquireLock().
    """
    if _lock:
        _lock.release()


class Manager(type, abc.ABC):
    def __init__(cls, *args):
        cls._instance_pool = dict()
        super().__init__(*args)


class BasicDB(metaclass=Manager):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def update(self, sample: dict, is_clear_cache=True, is_restart=True):
        raise NotImplementedError

    @abc.abstractclassmethod
    def _exec(self, sql):
        raise NotImplementedError

    @classmethod
    def get_db(cls: type, name: str, *args, **kargs):
        _acquireLock()
        try:
            if name in cls._instance_pool:
                instance = cls._instance_pool[name]
            else:
                instance = cls(name, *args, **kargs)
                cls._instance_pool[name] = instance
            pass
        except Exception as e:
            raise e

        finally:
            _releaseLock()
        return instance
