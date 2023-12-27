from tag import Tag
from typing import List, Tuple, Optional, Any
from dataclasses import dataclass

@dataclass
class PortMessage:
    tag : Tag
    message : Optional[Any]

@dataclass
class LoopDiscovery:
    origin : str
    origin_output : str
    # TODO: add delay to loop detection
    entries : List[Tuple[str, str, str]] # reactor, input, output

@dataclass
class LoopDetected(LoopDiscovery):
    origin_input : str


@dataclass 
class LoopMessage : 
    sender : str
    request_origin : str
    success : bool # if false, its a request
    tag : Tag