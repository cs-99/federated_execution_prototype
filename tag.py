from __future__ import annotations
from time import time_ns
from datetime import datetime

class Tag:
    _UINT64_MAX : int = 18446744073709551615
    _UINT64_MIN : int = 0
    def __init__(self, time=None, microstep=0):
        self._time : int = time if time is not None else time_ns()
        self._microstep : int = microstep

    @property
    def time(self) -> int:
        return self._time
    
    @property
    def microstep(self) -> int:
        return self._microstep

    def __lt__(self, other : Tag) -> bool:
        if self.time < other.time:
            return True
        if self.time == other.time:
            return self.microstep < other.microstep
        return False

    def __gt__(self, other : Tag) -> bool:
        return other < self

    def __eq__(self, other : Tag) -> bool:
        return self.time == other.time and self.microstep == other.microstep

    def __ne__(self, other : Tag) -> bool:
        return not self == other

    def __le__(self, other : Tag) -> bool:
        return self < other or self == other
    
    def __ge__(self, other : Tag) -> bool:
        return self > other or self == other

    def __repr__(self) -> str:
        return f"[{datetime.fromtimestamp(self.time/1e9)}, {self.microstep}]"

    def delay(self, time : int = 0) -> Tag:
        # this comparison is possible because pythons integers are actually unbounded
        if self._time + time > Tag._UINT64_MAX:
            raise ValueError("Time overflow.")
        if time == 0:
            if self._microstep == Tag._UINT64_MAX:
                raise ValueError("Microstep overflow.")
            return Tag(self._time + time, self._microstep + 1)
        return Tag(self._time + time, 0)

    def subtract(self, time : int = 0) -> Tag:
        # this comparison is possible because pythons integers are actually unbounded
        if self._time - time < 0:
            raise ValueError("Time underflow.")
        if time == 0:
            return self.decrement()
        return Tag(self._time - time, Tag._UINT64_MAX)

    def decrement(self):
        if self._microstep == 0:
            return Tag(self._time - 1, Tag._UINT64_MAX)
        return Tag(self._time, self._microstep - 1)
    
assert Tag(time=0, microstep=1) < Tag(time=1, microstep=0)
assert Tag(time=1, microstep=0) < Tag(time=1, microstep=1)
assert Tag(1,1) == Tag(1,1)