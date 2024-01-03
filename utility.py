from __future__ import annotations
from collections import deque
from typing import Any, List

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

# a function that compares just the order of the elements in the lists, not their actual indices
# for example, [1,2,3] == [2,3,1] == [3,1,2]
# but [1,2,3] != [3,2,1]
def are_lists_equal_with_rotation(list1 : List, list2 : List) -> bool:
    if len(list1) != len(list2):
        return False  
    list1_deque = deque(list1)
    for _ in range(len(list1)):
        if list(list1_deque) == list2:
            return True
        list1_deque.rotate(1)
    return False

def get_next_entry_with_rotation(list : List, entry : Any) -> Any:
    assert entry in list
    index = list.index(entry)
    return list[(index + 1) % len(list)]
    



