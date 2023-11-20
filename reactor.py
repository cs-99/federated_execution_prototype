"""
This is a prototype for the decentralized coordination contract used for federated (and enclaves) execution of Lingua Franca
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, List, Optional, Dict, Tuple
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
        return f"{self._name}"

class RegisteredReactor:
    def __init__(self, reactor : Reactor) -> None:
        assert isinstance(reactor, Reactor)
        self._reactor : Reactor = reactor

    @property
    def reactor(self) -> Reactor:
        return self._reactor

class Action(Named, RegisteredReactor):
    def __init__(self, name, reactor : Reactor):
        Named.__init__(self, name)
        RegisteredReactor.__init__(self, reactor)
        self._function : Callable[[Tag], None] = lambda _: None

    def _set_func(self, function):
        self._function = function

    def exec(self, tag : Tag):
        print(f"Action {self._name} executing at {repr(tag)}.")
        self._function(tag)

@dataclass
class TimerDeclaration:
    name : str
    offset : int
    interval : int

# this class is just to distinguish triggers (timer, inputs) from reactions for now
class TriggerAction(Action):
    def __init__(self, name, reactor : Reactor):
        super().__init__(name, reactor)

class Timer(TriggerAction):
    def __init__(self, name, reactor : Reactor, offset = 0, interval = 0):
        super().__init__(name, reactor)
        assert isinstance(offset, int)
        assert isinstance(interval, int)
        self._offset : int = offset
        self._interval : int = interval
        #self._set_func(lambda tag: self._reactor.schedule_action_async(self, Tag(tag.time + self._interval)))

    def __repr__(self) -> str:
        return f"{super().__repr__()}({utility.ns_to_secs(self._offset)}, {utility.ns_to_secs(self._interval)})"

    def schedule_start(self):
        self._reactor.schedule_action_sync(self, Tag(self._reactor.start_tag.time + self._offset))
        
        

class Port(Named, RegisteredReactor):
    def __init__(self, name, reactor : Reactor):
        Named.__init__(self, name)
        RegisteredReactor.__init__(self, reactor)

"""
Input and Output are directly the endpoints for communication
"""
class Input(Port, TriggerAction):
    def __init__(self, name, reactor  : Reactor):
        Port.__init__(self, name, reactor)
        TriggerAction.__init__(self, name, reactor)

class Output(Port):
    def __init__(self, name, reactor : Reactor):
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

@dataclass
class ReactionDeclaration:
    name: str
    triggers: List[str]
    effects: List[str]

        
class Reaction(Action):
    def __init__(self, name, reactor, triggers=None, effects=None):
        super().__init__(name, reactor)
        self._triggers : List[Input] = triggers if triggers is not None else []
        self._effects : List[Output] = effects if effects is not None else []
        utility.list_instance_check(self._triggers, TriggerAction)
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

@dataclass
class ActionListEntry:
    tag : Tag
    actions : List[Action]

class ActionList:
    def __init__(self) -> None:
        self._list : List[ActionListEntry] = []

    def add(self, tag : Tag, action : Action) -> None:
        for entry in self._list:
            if entry.tag == tag:
                entry.actions.append(action)
                return
        self._list.append(ActionListEntry(tag, [action]))

    def get_next(self) -> Tuple[Tag, List[Action]]:
        assert len(self._list) > 0
        smallest_tag_index : int = 0
        smallest_tag : Tag = self._list[smallest_tag_index].tag
        for index, entry in enumerate(self._list):
            if entry.tag < smallest_tag:
                smallest_tag = entry.tag 
                smallest_tag_index = index
        return self._list.pop(smallest_tag_index)
        
    def __bool__(self) -> bool:
        return bool(self._list)

    def __repr__(self) -> str:
        r = ""
        for entry in self._list:
            r += repr(entry)
        return r

"""
Every Reactor is considered federate, therefore has their own scheduling (see run())
"""
class Reactor(Named):
    def __init__(self, name, start_tag, stop_tag, inputs: List[str]=None, outputs: List[str]=None, 
                timers: List[TimerDeclaration] = None, reactions:List[ReactionDeclaration]=None):
        assert start_tag < stop_tag
        super().__init__(name)
        self._release_tag_callbacks : List[Callable[[Tag], None]] = []
        self._inputs : List[Input]= [Input(input, self) for input in inputs] if inputs is not None else []
        self._outputs : List[Output]= [Output(output, self) for output in outputs] if outputs is not None else []
        self._timers : List[Timer] = [self._init_timer(timer) for timer in timers] if timers is not None else []
        # assigning empty list first to make sure _find_member_by_name works (it searches that list too)
        self._reactions : List[Reaction] = []
        self._reactions = [self._init_reaction(reaction) for reaction in reactions] if reactions is not None else []
        
        self._start_tag : Tag = start_tag
        self._stop_tag : Tag = stop_tag

        get_name_from_named : Callable[[Named], str] = lambda m : m.name
        all_names : List[str] = list(map(get_name_from_named, self._inputs + self._outputs + self._timers + self._reactions))
        for name in all_names:
            if all_names.count(name) > 1:
                raise ValueError(f"Reactor {self._name} contains duplicate member name '{name}'.")

        # scheduling 
        self._current_tag : Tag = self._start_tag.decrement()
        self._reaction_q : ActionList = ActionList()
        self._reaction_q_lock : threading.Lock = threading.Lock()
        self._reaction_q_cv : threading.Condition = threading.Condition(self._reaction_q_lock)
        
    @property
    def start_tag(self) -> Tag:
        return self._start_tag


    def _init_reaction(self, reaction : ReactionDeclaration) -> Reaction:
        triggers = []
        for trigger_name in reaction.triggers:
            trigger = self._find_member_by_name(trigger_name)
            if trigger is None or not isinstance(trigger, TriggerAction):
                raise ValueError(f"Trigger {trigger_name} not in {self.name}.")
            triggers.append(trigger)

        effects = []
        for output_name in reaction.effects:
            output = self._find_member_by_name(output_name)
            if output is None or not isinstance(output, Output):
                raise ValueError(f"Output {output_name} not in {self.name}.")
            effects.append(output)

        return Reaction(reaction.name, self, triggers, effects)

    def _init_timer(self, timer : TimerDeclaration) -> Timer:
        return Timer(timer.name, self, timer.offset, timer.interval)

    def __repr__(self) -> str:
        r = super().__repr__()
        r += f"\n\t Inputs: {', '.join(repr(input) for input in self._inputs)}"
        r += f"\n\t Outputs: {', '.join(repr(output) for output in self._outputs)}"
        r += f"\n\t Timers: {', '.join(repr(timer) for timer in self._timers)}"
        r += "\n\t Reactions: \n\t\t"
        r += '\n\t\t'.join(repr(reaction) for reaction in self._reactions)
        
        return r

    def register_release_tag_callback(self, callback):
        assert isinstance(callback, Callable)
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

    def _find_member_by_name(self, name) -> Optional[Named]:
        for member in self._inputs + self._outputs + self._reactions + self._timers:
            if member.name == name:
                return member
        return None

    @staticmethod
    def find_port_by_name(ports : List[Port], name : str) -> Optional[Port]:
        for port in ports:
            if port.name == name:
                return port
        return None

    def _release_current_tag(self):
        for callback in self._release_tag_callbacks:
            callback(self._current_tag)

    def schedule_action_async(self, action : Action, tag : Optional[Tag]=None) -> bool:
        with self._reaction_q_cv:
            if tag <= self._current_tag:
                return False
            self.schedule_action_sync(action, tag)
            self._reaction_q_cv.notify()
        return True
    
    # this method assumes that self._reaction_q_cv is held
    def schedule_action_sync(self, action : Action, tag : Optional[Tag] = None) -> bool:
        assert tag > self._current_tag
        assert self._reaction_q_lock.locked()
        if tag is None:
            tag = self._current_tag.delay()
        self._reaction_q.add(tag, action)

    # remove tags up until the current tag
    # unused, as the current tag gets initialized to the start_tag.decrement at construction time already
    # and therefore requests from other federates to schedule empty events will fail anyway when starting threads with .run()
    def _remove_tags_lte_current(self):
        with self._reaction_q_cv:
            tags_to_remove = []
            for tag in self._reaction_q.keys():
                if tag <= self._current_tag:
                    tags_to_remove.append(tag)
            for tag in tags_to_remove:
                del self._reaction_q[tag]

    def _schedule_timers_once(self) -> None:
        with self._reaction_q_cv:
            for timer in self._timers:
                timer.schedule_start()

    def _get_triggered_reactions(self, action : TriggerAction):
        triggered_reactions : List[Reaction] = []
        for reaction in self._reactions:
            if action in reaction.triggers:
                triggered_reactions.append(reaction)
        return triggered_reactions


    def run(self):
        self._schedule_timers_once()
        while True:
            actions = []
            with self._reaction_q_cv:
                self._release_current_tag()
                if not self._reaction_q:
                    return
                next : ActionListEntry = self._reaction_q.get_next()
                self._current_tag = next.tag
                actions = next.actions
                triggered_actions = []
                for action in actions:
                    if isinstance(action, TriggerAction):
                        triggered_actions += self._get_triggered_reactions(action)
                actions += triggered_actions
            for action in actions:
                action.exec(self._current_tag)



    

