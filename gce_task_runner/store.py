import threading
import time
from functools import wraps

_INSTANCES = {}
_INSTANCE_SIZE = None

_LOCK = threading.Lock()


def initialize(total_instance_size):
    global _INSTANCE_SIZE
    _INSTANCE_SIZE = int(total_instance_size)


def _check_initialized(f):
    global _INSTANCE_SIZE

    @wraps(f)
    def _inner(*args, **kwargs):
        if _INSTANCE_SIZE is None:
            raise RuntimeError('You mast call init() before call register()!!')
        return f(*args, **kwargs)

    return _inner


@_check_initialized
def register(instance_id, instance, timeout=0):
    """_INSTANCESにGCEインスタンスを格納する"""
    with _LOCK:
        limit = timeout + time.time() if timeout else None
        _INSTANCES[instance_id] = (instance, limit)


@_check_initialized
def pop(instance_id):
    """_INSTANCESに格納されたGCEインスタンスを取り出す"""
    global _INSTANCE_SIZE
    with _LOCK:
        instance = _INSTANCES.pop(instance_id, (None, None))
        if instance[0]:
            _INSTANCE_SIZE -= 1
        return instance


@_check_initialized
def get_time_overs():
    """_INSTANCESに格納された期限切れGCEインスタンスを全て取り出す"""
    global _INSTANCE_SIZE
    with _LOCK:
        ids = [_id for _id, (_, limit) in _INSTANCES.items() if limit and limit < time.time()]
        _INSTANCE_SIZE -= len(ids)
        return [(_id, _INSTANCES.pop(_id)) for _id in ids]


@_check_initialized
def get_remains_count():
    return _INSTANCE_SIZE
