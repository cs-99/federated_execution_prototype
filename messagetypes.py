from tag import Tag
from typing import List, Tuple, Optional, Any
from dataclasses import dataclass


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

@dataclass 
class LoopMessage: 
    tag : Tag

@dataclass
class LoopAcquireRequest(LoopMessage):
    pass

@dataclass
class LoopAcquireResponse(LoopMessage):
    pass
