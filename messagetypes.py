from typing import List, Tuple, Optional, Any
from dataclasses import dataclass

from tag import Tag
import utility

@dataclass
class ReleaseMessage:
    tag : Tag

@dataclass
class RequestMessage:
    tag : Tag

@dataclass
class MessageToInput:
    receiving_input : str
    
@dataclass
class PortMessage(MessageToInput):
    tag : Tag
    message : Any
    
@dataclass
class LoopDiscovery(MessageToInput):
    origin : str
    origin_output : str
    # TODO: add delay to loop detection
    entries : List[Tuple[str, str, str]] # reactor, input, output

@dataclass
class LoopDetected(LoopDiscovery):
    origin_input : str
    current_entry : int # index in entries list (origin excluded)

class Loop:
    _loop_members : List[str]

    def __init__(self, loop_members : List[str]):
        self._loop_members = loop_members

    def __contains__(self, item):
        return item in self._loop_members
    
    def __iter__(self):
        return iter(self._loop_members)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Loop):
            return False
        return utility.are_lists_equal_with_rotation(self._loop_members, __value._loop_members)      

    def get_next_loop_member(self, current_loop_member : str) -> str:
        return utility.get_next_entry_with_rotation(self._loop_members, current_loop_member)

@dataclass
class LoopAcquireRequest:
    tag : Tag
    origin : str
    loop : Loop

