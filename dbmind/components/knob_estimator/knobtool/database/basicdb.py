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
