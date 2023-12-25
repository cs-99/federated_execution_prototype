from __future__ import annotations
import threading
import logging


class LoggingLock:
    def __init__(self, name : str, logger : logging.Logger):
        self._lock = threading.Lock()
        self._logger = logger
        self._name = name

    def acquire(self):
        self._logger.debug(f'Lock {self._name} acquiring...')
        self._lock.acquire()
        self._logger.debug(f'Lock {self._name} acquired.')

    def release(self):
        self._lock.release()
        self._logger.debug(f'Lock {self._name} released.')


    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def locked(self):
        return self._lock.locked()
    
class LoggingCondition:
    def __init__(self, name : str, logger : logging.Logger, lock : threading.Lock = threading.Lock()):
        self._cond = threading.Condition(lock)
        self._logger = logger
        self._name = name

    def acquire(self):
        self._logger.debug(f'Condition {self._name} acquiring...')
        self._cond.acquire()
        self._logger.debug(f'Condition {self._name} acquired.')

    def release(self):
        self._cond.release()
        self._logger.debug(f'Condition {self._name} released.')

    def wait(self, timeout=None):
        self._logger.debug(f'Condition {self._name} waiting...')
        self._cond.wait(timeout)
        self._logger.debug(f'Condition {self._name} waited.')

    def notify(self, n=1):
        self._logger.debug(f'Condition {self._name} notifying...')
        self._cond.notify(n)
        self._logger.debug(f'Condition {self._name} notified.')

    def notify_all(self):
        self._logger.debug(f'Condition {self._name} notifying all...')
        self._cond.notify_all()
        self._logger.debug(f'Condition {self._name} notified all.')

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def locked(self):
        return self._cond.locked()
    
    def wait_for(self, predicate, timeout=None):
        self._logger.debug(f'Condition {self._name} waiting for...')
        self._cond.wait_for(predicate, timeout)
        self._logger.debug(f'Condition {self._name} waited for.')
    
    def __str__(self):
        return f'LoggingCondition({self._name})'

    def __repr__(self):
        return f'LoggingCondition({self._name})'
    