"""
This is a prototype for the decentralized coordination contract used for federated (and enclaves) execution of Lingua Franca
"""
from __future__ import annotations
from typing import Callable, List, Optional, Tuple
import threading
# custom files from here
from tag import Tag
import utility
    
class Named:
    def __init__(self, name) -> None:
        self._name : str = name

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} {self._name}"

class RegisteredReactor:
    def __init__(self, reactor) -> None:
        assert isinstance(reactor, Reactor)
        self._reactor : Reactor = reactor

    @property
    def reactor(self) -> Reactor:
        return self._reactor

class Action(Named, RegisteredReactor):
    def __init__(self, name, reactor):
        Named.__init__(self, name)
        RegisteredReactor.__init__(self, reactor)
        self._function : Callable[..., None] = lambda x: None

    def set_func(self, function):
        self._function = function

    def exec(self, *args):
        print("Action " + self._name + " executing.")
        self._function(args)

class Timer(Action):
    def __init__(self, name, reactor, start_time = 0, interval = 0):
        super().__init__(name, reactor)
        self._start_time : int = start_time
        self._interval : int = interval


class Port(Named, RegisteredReactor):
    def __init__(self, name, reactor):
        Named.__init__(self, name)
        RegisteredReactor.__init__(self, reactor)

"""
Input and Output are directly the endpoints for communication
"""
class Input(Port, Action):
    def __init__(self, name, reactor):
        Port.__init__(self, name, reactor)
        Action.__init__(self, name, reactor)

class Output(Port):
    def __init__(self, name, reactor):
        super().__init__(name, reactor)
        reactor.register_release_tag_callback(lambda tag: CommunicationBus().set_tag_only(self, tag))

class Connection:
        @property 
        def input(self) -> Input:
            return self._input
        
        @property
        def output(self) -> Output:
            return self._output

        @property
        def is_set(self) -> bool: 
            return self._is_set

        @property
        def tag(self) -> Tag:
            return self._tag

        def __init__(self, output, input) -> None:
            if not isinstance(output, Output) or not isinstance(input, Input):
                raise ValueError("Connections must consist of output and input.")
            self._input : Input = input
            self._output : Output = output
            self._is_set : bool = False
            self._tag : Optional[Tag] = None

        def set_message(self, tag) -> None:
            self._is_set = True
            self._tag = tag
            # todo schedule on input

        def set_tag_only(self, tag) -> None:
            self._tag = tag
            # todo schedule on input

        def unset_message(self) -> None:
            self._is_set = False

class CommunicationBus(metaclass=utility.Singleton):
    def __init__(self):
        self._connections : List[Connection] = []
        self._connection_lock : threading.Lock = threading.Lock()

    def add_connection(self, output, input) -> None:
        self._connection_lock.acquire()
        self._connections.append(Connection(output, input)) 
        self._connection_lock.release()

    def set(self, output: Output, tag : Tag) -> None:
        self._connection_lock.acquire()
        for connection in self._connections:
            if connection.output == output:
                connection.set_message(tag)
        self._connection_lock.release()

    def set_tag_only(self, output: Output, tag : Tag) -> None:
        self._connection_lock.acquire()
        for connection in self._connections:
            if connection.output == output:
                connection.set_tag_only(tag)
        self._connection_lock.release()

        
class Reaction(Action):
    def __init__(self, name, reactor, triggers=None, effects=None):
        super().__init__(name, reactor)
        self._triggers : List[Input] = triggers if triggers is not None else []
        self._effects : List[Output] = effects if effects is not None else []
        utility.list_instance_check(self._triggers, Input)
        utility.list_instance_check(self._effects, Output)

    def __repr__(self) -> str:
        r = self.name
        r += " " + ', '.join(trigger.name for trigger in self._triggers)
        r += " -> " + ', '.join(effect.name for effect in self._effects)
        return r

    @property
    def triggers(self) -> List[Input]:
        return self._triggers

    @property
    def effects(self) -> List[Output]:
        return self._effects

"""
Every Reactor is considered federate, therefore has their own scheduling (see run())
"""
class Reactor(Named):
    def __init__(self, name, start_tag, stop_tag, inputs: List[str]=None, outputs: List[str]=None, reactions:List[Tuple[List[str], List[str]]]=None):
        assert start_tag < stop_tag
        super().__init__(name)
        self._release_tag_callbacks : List[Callable[[Tag], None]] = []
        self._inputs = [Input(input, self) for input in inputs] if inputs is not None else []
        self._outputs : List[Output]= [Output(output, self) for output in outputs] if outputs is not None else []
        self._reactions : List[Reaction] = [self._init_reaction(reaction) for reaction in reactions] if reactions is not None else []
        self._start_tag : Tag = start_tag
        self._stop_tag : Tag = stop_tag
        self._current_tag : Tag = self._start_tag.decrement()
        self._action_q : List[Action] = []
        

    def _init_reaction(self, reaction : Tuple[str, List[str], List[str]]) -> Reaction:
        triggers = []
        for input_name in reaction[1]:
            input = self.find_input_by_name(input_name)
            if input is None:
                raise ValueError(f"Input {input_name} not in {self.name}.")
            triggers.append(input)

        effects = []
        for output_name in reaction[2]:
            output = self.find_output_by_name(output_name)
            if output is None:
                raise ValueError(f"Output {output_name} not in {self.name}.")
            effects.append(output)

        return Reaction(reaction[0], self, triggers, effects)
        

    def __repr__(self) -> str:
        r = super().__repr__()
        r += f"\n\t Inputs: {', '.join(input.name for input in self._inputs)}"
        r += f"\n\t Outputs: {', '.join(output.name for output in self._outputs)}"
        r += "\n\t Reactions: \n\t\t"
        r += '\n\t\t'.join(repr(reaction) for reaction in self._reactions)
        return r

    def register_release_tag_callback(self, callback):
        self._release_tag_callbacks.append(callback)

    @property
    def inputs(self) -> List[Input]:
        return self._inputs

    @property
    def outputs(self) -> List[Output]:
        return self._outputs

    @property
    def ports(self) -> List[Port]:
        return self._inputs + self._outputs

    def find_input_by_name(self, name) -> Optional[ports.Input]:
        return self.find_port_by_name(self._inputs, name)

    def find_output_by_name(self, name) -> Optional[ports.Output]:
        return self.find_port_by_name(self._outputs, name)

    @staticmethod
    def find_port_by_name(ports, name) -> Optional[ports.Port]:
        for port in ports:
            if port.name == name:
                return port
        return None

    def _release_tag(self, tag):
        for callback in self._release_tag_callbacks:
            callback(tag)

    def run(self):
        pass


    

