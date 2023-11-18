from time import time_ns

class Tag:
    _time : int
    _microstep : int

    def __init__(self, time=None, microstep=None):
        self._time = time if time is not None else time_ns()
        self._microstep = microstep if microstep is not None else 0

    @property
    def time(self) -> int:
        return self._time
    
    @property
    def microstep(self) -> int:
        return self._microstep

    def __lt__(self, other) -> bool:
        if self.time < other.time:
            return True
        if self.time == other.time:
            return self.microstep < other.microstep
        return False

    def __gt__(self, other) -> bool:
        return other < self

    def __eq__(self, other) -> bool:
        return self.time == other.time and self.microstep == other.microstep

    def __ne__(self, other) -> bool:
        return not self == other

    def __le__(self, other):
        return self < other or self == other
    
    def __ge__(self, other):
        return self > other or self == other

assert Tag(time=0, microstep=1) < Tag(time=1, microstep=0)
assert Tag(time=1, microstep=0) < Tag(time=1, microstep=1)
assert Tag(1,1) == Tag(1,1)