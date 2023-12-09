from __future__ import annotations
from typing import List

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

def list_instance_check(list, type) -> None:
    assert isinstance(list, List)
    for elem in list:
        if not isinstance(elem, type):
            raise TypeError("List contains elements that are not of type " + type)

def secs_to_ns(secs: float) -> int:
    return int(secs * 1000000000)

def ns_to_secs(ns: int) -> float:
    return ns / 1000000000


