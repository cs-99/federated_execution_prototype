from dataclasses import dataclass
from typing import List

@dataclass
class ReactionDeclaration:
    name: str
    triggers: List[str]
    effects: List[str]

@dataclass
class TimerDeclaration:
    name : str
    offset : int
    interval : int