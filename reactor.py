"""
This is a prototype for the decentralized coordination contract used for federated (and enclaves) execution of Lingua Franca
"""
from __future__ import annotations
from dataclasses import dataclass
from functools import total_ordering
from typing import Callable, Dict, List, Optional, Tuple, Any
import threading
from logging_primitives import LoggingCondition, LoggingLock
from pubsub import Subscriber, Publisher
import logging # thread safe by default

# custom files from here
from tag import Tag
import utility

# TODO: check lock acquiring order such that it is totally ordered and no deadlocks can occur
# loop lock and reaction_q_lock are acquired in different orders, therefore they can deadlock
    
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

class Action(Named, RegisteredReactor):
    def __init__(self, name, reactor : Reactor):
        Named.__init__(self, name)
        RegisteredReactor.__init__(self, reactor)
        self._function : Callable[[Tag], None] = lambda _: None

    def _set_func(self, function):
        self._function = function

    def exec(self, tag : Tag):
        self._reactor.logger.info(f"{self._name} executing at {tag}.")
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
        self._set_func(self._on_exec)

    def __repr__(self) -> str:
        return f"{super().__repr__()}({utility.ns_to_secs(self._offset)}, {utility.ns_to_secs(self._interval)})"

    def schedule_start(self):
        first_tag = Tag(self._reactor.start_tag.time + self._offset)
        if first_tag < self._reactor.stop_tag:
            self._reactor.schedule_action_sync(self, first_tag)

    def _on_exec(self, tag: Tag):
        next_tag = Tag(tag.time + self._interval)
        if next_tag < self._reactor.stop_tag:
            self._reactor.schedule_action_async(self, next_tag)
        

class Port(Named, RegisteredReactor):
    def __init__(self, name, reactor : Reactor):
        Named.__init__(self, name)
        RegisteredReactor.__init__(self, reactor)

@dataclass
class PortMessage:
    tag : Tag
    message : Optional[Any]

"""
Input and Output are also the endpoints for communication
"""
TAG_RELEASE_TOPIC_SUFFIX = "/__tag_release__"
TAG_REQUEST_TOPIC_SUFFIX = "/__tag_request__"
LOOP_DISCOVERY_TOPIC_SUFFIX = "/__loop_discovery__"
LOOP_DETECTED_TOPIC_SUFFIX = "/__loop_detected__" # after a loop is detected, this topic is used to notify the other federates
class Input(Port, TriggerAction):
    def __init__(self, name, reactor  : Reactor):
        Port.__init__(self, name, reactor)
        TriggerAction.__init__(self, name, reactor)
        self._is_connected : bool = False
        self._message_sub : Optional[Subscriber] = None
        self._loop_discovery_sub: Optional[Subscriber] = None
        self._loop_detected_sub : Optional[Subscriber] = None
        self._delay : Optional[int] = None
        self._connected_reactor_name : Optional[str] = None
        self._empty_request_pub : Optional[Publisher] = None
        self._released : Tag = Tag(0)

    def connect(self, reactor_name : str, output_name : str,  delay : Optional[int] = None):
        if self._is_connected:
            raise ValueError(f"{self._reactor}/{self.name} is already connected.")
        self._connected_reactor_name = reactor_name
        self._reactor.logger.debug(f"{self._reactor.name}/{self.name} connecting to {reactor_name}/{output_name}.")
        self._message_sub = Subscriber(f"{reactor_name}/{output_name}", self._receive_message)
        self._loop_discovery_sub = Subscriber(f"{reactor_name}/{output_name}{LOOP_DISCOVERY_TOPIC_SUFFIX}", self._on_loop_discovery)
        self._loop_detected_sub = Subscriber(f"{reactor_name}/{output_name}{LOOP_DETECTED_TOPIC_SUFFIX}", self._on_loop_detected)
        self._delay = delay
        self._empty_request_pub = Publisher(f"{reactor_name}{TAG_REQUEST_TOPIC_SUFFIX}")
        self._is_connected = True

    
    def _on_loop_discovery(self, loop_discovery : LoopDiscovery):
        if loop_discovery.origin == self._reactor.name:
            self._reactor.logger.debug(f"Loop discovered at {self._reactor.name}.")
            affected_outputs = self._reactor.get_affected_outputs(self)
            affected_output_names = [output.name for output in affected_outputs]
            if loop_discovery.origin_output in affected_output_names:
                # maybe TODO: make sure the loop has a delay or we are stuck at this tag forever
                # however, this could also be further down the loop if it is a nested loop
                # so either expand the discovery to track the entire loop or 
                # just dont mind at all whether the loop triggers itself without delay (as the user might actually want that)
                self._reactor.logger.debug(f"Loop triggers itself.")
            # TODO: if the loop has no delay, all of the loop participants have to "release together", as the loop could come back around with a new event but still being at the same tag (aka loop release)
            self._reactor.get_output(loop_discovery.origin_output).relay_loop_detected(LoopDetected(loop_discovery.origin, loop_discovery.origin_output, loop_discovery.entries, self.name))
        else:
            affected_outputs = self._reactor.get_affected_outputs(self)
            for output in affected_outputs:
                loop_discovery.entries.append((self._reactor.name, self.name, output.name))
                output.relay_loop_discovery(loop_discovery)

    def _on_loop_detected(self, loop_detected: LoopDetected):
        self._reactor.ledger.update_loop_members(loop_detected)
        if loop_detected.origin == self._reactor.name:
            return
        for output in self._reactor.get_affected_outputs(self):
            output.relay_loop_detected(loop_detected)
        
    def _receive_message(self, msg : PortMessage):
        assert msg.tag > self._released
        self._released = msg.tag
        if msg.message is None:
            self._reactor.notify()
            return
        if self._delay is not None:
            self._reactor.schedule_action_async(self, msg.tag.delay(self._delay))
        else:
            self._reactor.schedule_action_async(self, msg.tag)
        

    def _acquire_tag_predicate(self, tag_to_acquire : Tag, predicate : Callable[[None], bool]) -> bool:
        # connected reactor released the tag
        if tag_to_acquire <= self._released:
            self._reactor.logger.debug(f"{self.name} acquired {tag_to_acquire} because it was released by {self._connected_reactor_name}.")
            return True
        # or all loop members have acquired the tag (for every loop that the connected reactor is part of)
        # this is when this reactor started the loop acquire
        # however, it must be before the last acquired tag
        # this is the case for the reactor that has the first delay before the original event that triggered the loop acquire
        if self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag_to_acquire) \
             and not self._reactor.ledger.is_last_tag_fully_acquired_by_loops(self._connected_reactor_name, tag_to_acquire):
                self._reactor.logger.debug(f"{self.name} acquired {tag_to_acquire} because all loop members have acquired it.")
                return True
        # we have discovered that we are part of a loop (and have not requested the corresponding loop acquire yet)
        if self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
                and not self._reactor.ledger.requested_all_loop_acquires_already(self._connected_reactor_name, tag_to_acquire):
            self._reactor.logger.debug(f"{self.name} requested loop acquire for {tag_to_acquire}.")
            self._reactor.ledger.request_acquire_loops_where_is_member(self._connected_reactor_name, tag_to_acquire)
            
            return True
        # or the predicate is true (i.e. the actionlist in the scheduling has been modified)
        return predicate()
    
    def _loop_acquire_predicate(self, tag_to_acquire : Tag, loop_acquire_origins: set[str], predicate : Callable[[None], bool]) -> bool:
        # if we are part of one of the requested loops
        if self._reactor.ledger.is_part_of_same_loops(self._connected_reactor_name, loop_acquire_origins):
            self._reactor.logger.debug(f"{self.name} is part of the same loops as {self._connected_reactor_name}.")
            return True
        
        # or we have acquired the tag normally
        return self._acquire_tag_predicate(tag_to_acquire, predicate)
        
    
    def loop_acquire_tag(self, tag : Tag, loop_acquire_origins: set[str], predicate : Callable[[None], bool] = lambda: False) -> bool:
        tag_to_acquire = tag
        if self._delay is not None:
            tag_to_acquire = tag.subtract(self._delay)
        
        if not self._is_connected:
            return True
        
        res = self._reactor.wait_for(lambda: self._loop_acquire_predicate(tag_to_acquire, loop_acquire_origins, predicate))
        return res

    def acquire_tag(self, tag : Tag, predicate : Callable[[None], bool] = lambda: False) -> bool:
        tag_to_acquire = tag
        if self._delay is not None:
            tag_to_acquire = tag.subtract(self._delay)
        
        if self._released >= tag_to_acquire \
            or not self._is_connected:
            return True
        
        # see acquire_tag_predicate for explanation
        if self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag_to_acquire) \
            and not self._reactor.ledger.is_last_tag_fully_acquired_by_loops(self._connected_reactor_name, tag_to_acquire):
                return True
        
        if self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
            and not self._reactor.ledger.requested_all_loop_acquires_already(self._connected_reactor_name, tag_to_acquire):
                self._reactor.ledger.request_acquire_loops_where_is_member(self._connected_reactor_name, tag_to_acquire)
                return False
        else:
            self.request_empty_event_at( tag_to_acquire)
        self._reactor.logger.debug(f"{self.name} waiting for tag release {tag_to_acquire}.")
        return self._reactor.wait_for(lambda: self._acquire_tag_predicate(tag_to_acquire, predicate))

    def request_empty_event_at(self, tag : Tag):
        assert self._empty_request_pub is not None
        self._empty_request_pub.publish(tag)

class Output(Port):
    def __init__(self, name, reactor : Reactor):
        super().__init__(name, reactor)
        self._is_connected : bool = False
        self._message_pub : Optional[Publisher] = None
        self._loop_discovery_pub: Optional[Publisher] = None
        self._loop_detected_pub : Optional[Publisher] = None
        self._released : Tag = Tag(0)
        self._reactor.register_release_tag_callback(self._on_release)

    def set(self, tag : Tag):
        if self._is_connected:
            self._released = tag
            self._reactor.logger.debug(f"{self._reactor.name}/{self.name} setting {tag}.")
            self._message_pub.publish(PortMessage(tag, True))

    def _on_release(self, tag : Tag):
        if tag > self._released:
            self._released = tag
            self._message_pub.publish(PortMessage(tag, None))

    def connect(self, reactor_name : str):
        self._reactor.logger.debug(f"{self._reactor.name}/{self.name} connecting to {reactor_name}.")
        self._message_pub = Publisher(f"{self._reactor.name}/{self._name}")
        self._loop_discovery_pub = Publisher(f"{self._reactor.name}/{self._name}{LOOP_DISCOVERY_TOPIC_SUFFIX}")
        self._loop_discovery_pub.publish(LoopDiscovery(self._reactor.name, self._name, []))
        self._loop_detected_pub = Publisher(f"{self._reactor.name}/{self._name}{LOOP_DETECTED_TOPIC_SUFFIX}")
        self._is_connected = True

    def relay_loop_discovery(self, loop_discovery : LoopDiscovery):
        self._loop_discovery_pub.publish(loop_discovery)

    def relay_loop_detected(self, loop_detected : LoopDetected):
        self._loop_detected_pub.publish(loop_detected)

@dataclass
class ReactionDeclaration:
    name: str
    triggers: List[str]
    effects: List[str]
        
class Reaction(Action):
    def __init__(self, name, reactor, triggers=None, effects=None):
        super().__init__(name, reactor)
        self._triggers : List[TriggerAction] = triggers if triggers is not None else []
        self._effects : List[Output] = effects if effects is not None else []
        utility.list_instance_check(self._triggers, TriggerAction)
        utility.list_instance_check(self._effects, Output)
        self._set_func(self._set_outputs)

    def _set_outputs(self, tag: Tag):
        for effect in self._effects:
            effect.set(tag)

    def __repr__(self) -> str:
        r = self.name
        r += " " + ', '.join(trigger.name for trigger in self._triggers)
        r += " -> " + ', '.join(effect.name for effect in self._effects)
        return r

    @property
    def triggers(self) -> List[TriggerAction]:
        return self._triggers

    @property
    def effects(self) -> List[Output]:
        return self._effects

@total_ordering
@dataclass
class TaggedEntry:
    tag : Tag

    def __lt__(self, other: TaggedEntry) -> bool:
        return self.tag < other.tag

    def __eq__(self, other: TaggedEntry) -> bool:
        return self.tag == other.tag

@dataclass
class ActionListEntry(TaggedEntry):
    actions : List[Action]

# this is protected by reaction_q_cv
class ActionList:
    def __init__(self) -> None:
        self._list : List[ActionListEntry] = []

    def add(self, tag: Tag, action: Action) -> None:
        for entry in self._list:
            if entry.tag == tag:
                entry.actions.append(action)
                return
        self._list.append(ActionListEntry(tag, [action]))
        self._list.sort()

    def tag_exists(self, tag: Tag) -> bool:
        return any(entry.tag == tag for entry in self._list)

    def pop_tag(self, tag: Tag) -> List[Action]:
        assert any(entry.tag == tag for entry in self._list), f"No entry found for tag: {tag}"
        actions = [entry.actions for entry in self._list if entry.tag == tag][0]
        self._list = [entry for entry in self._list if entry.tag != tag]
        return actions

    def pop_next_tag(self) -> List[Action]:
        assert len(self._list) > 0
        return self.pop_tag(self._list[0].tag)
    
    def next_tag(self) -> Tag:
        if self._list:
            return self._list[0].tag
        return None

    def __repr__(self) -> str:
        r = ""
        for entry in self._list:
            r += repr(entry)
        return r

@dataclass 
class LoopAcquireEntry(TaggedEntry):
    origins : set[str]

class LoopAcquireRequests:
    def __init__(self) -> None:
        self._list: List[LoopAcquireEntry] = []

    def add(self, tag :Tag, origin: str) -> None:
        for entry in self._list:
            if entry.tag == tag:
                entry.origins.add(origin)
                return
        entry = LoopAcquireEntry(tag, set([origin]))
        self._list.append(entry)
        self._list.sort()

    def pop_tag(self, tag: Tag) -> set[str]:
        assert any(entry.tag == tag for entry in self._list), f"No entry found for tag: {tag}"
        matching_entry = [entry for entry in self._list if entry.tag == tag][0]
        self._list.remove(matching_entry)
        return matching_entry.origins
    
    def remove_tag(self, tag: Tag) -> None:
        self.pop_tag(tag)
    
    def get_origins(self, tag: Tag) -> set[str]:
        assert any(entry.tag == tag for entry in self._list), f"No entry found for tag: {tag}"
        return [entry.origins for entry in self._list if entry.tag == tag][0]

    def next_tag(self) -> Optional[Tag]:
        if self._list:
            return self._list[0].tag
        return None

    def __repr__(self) -> str:
        return ''.join(repr(entry) for entry in self._list)

    
@dataclass
class LoopDiscovery:
    origin : str
    origin_output : str
    # TODO: add delay to loop detection
    entries : List[tuple[str, str, str]] # reactor, input, output

@dataclass
class LoopDetected(LoopDiscovery):
    origin_input : str


@dataclass 
class LoopMessage : 
    sender : str
    request_origin : str
    success : bool # if false, its a request
    tag : Tag


LOOP_TOPIC_SUFFIX = "__loop__"
class FederateLoopLedger(RegisteredReactor):
    def __init__(self, reactor) -> None:
        super().__init__(reactor)
        self._loops_lock : threading.Lock = LoggingLock("loops_lock", self._reactor.logger)
        self._loops : Dict[frozenset, Tuple[Publisher, Subscriber, set[Tag]]] = {} # set of members -> (pub, sub, set of requested tags)
        self._loop_acquired : Dict[str, Tag] = {}

    def _on_loop_acquire_request(self, request: LoopMessage):
        if self._reactor.name == request.request_origin:
            return
        self._reactor.logger.debug(f"Loop acquire request from {request.request_origin} at {request.tag}.")

        with self._loops_lock:
            for loop in self._loops:
                if request.request_origin in loop:
                    if request.tag not in self._loops[loop][2]:
                        self._loops[loop][2].add(request.tag)
                        self._reactor.schedule_loop_acquire_async(request.tag, request.request_origin)
    
    def is_part_of_same_loops(self, reactor_name: str, loop_acquire_origins: set[str]) -> bool:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    for origin in loop_acquire_origins:
                        if origin in loop:
                            return True
        return False
    
    def create_loop_topic_name(self, members: set[str]) -> str:
        return '/'.join(sorted(members)) + LOOP_TOPIC_SUFFIX

    def loop_acquire_success(self, tag: Tag, loop_acquire_origins: set[str]) -> None:
        with self._loops_lock:
            for loop in self._loops:
                for origin in loop_acquire_origins:
                    if origin in loop:
                        self._loops[loop][0].publish(LoopMessage(self._reactor.name, origin, True, tag))
            self._loop_acquired[self._reactor.name] = tag
        self._remove_tag_if_done(self._reactor.name, tag)

    def _remove_tag_if_done(self, successful_member : str, tag: Tag) -> None:
        with self._loops_lock:
            for loop in self._loops:
                if successful_member in loop:
                    tag_done = True
                    for member in loop:
                        if member not in self._loop_acquired or self._loop_acquired[member] < tag:
                            tag_done = False
                            break
                    if tag_done:
                        self._loops[loop][2].remove(tag)


    def request_acquire_loops_where_is_member(self, reactor_name: str, tag: Tag) -> None:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    if tag not in self._loops[loop][2]:
                        self._loops[loop][0].publish(LoopMessage(self._reactor.name, self._reactor.name, False, tag))
                        self._loops[loop][2].add(tag)
                        self._reactor.schedule_loop_acquire_sync(tag, self._reactor.name)
    
    def requested_all_loop_acquires_already(self, reactor_name: str, tag: Tag) -> bool:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    if tag not in self._loops[loop][2]:
                        return False
        return True

    def _on_loop_acquire_success(self, msg: LoopMessage):
        with self._loops_lock:
            if self._loop_acquired.get(msg.sender) is None or msg.tag > self._loop_acquired[msg.sender]:
                self._loop_acquired[msg.sender] = msg.tag
            self._remove_tag_if_done(msg.sender, msg.tag)
        self._reactor.notify()

    def check_loops_acquired(self, loop_member, tag: Tag) -> bool:
        found = False # if member is not part of a known loop we return False
        with self._loops_lock:
            for loop in self._loops:
                if loop_member in loop:
                    found = True
                    for member in loop:
                        if member not in self._loop_acquired or self._loop_acquired[member] < tag:
                            return False
        return found
    
    # this method is used after check_loops_acquired to check whether the tag is the last one that was acquired
    def is_last_tag_fully_acquired_by_loops(self, loop_member, tag: Tag) -> bool:
        with self._loops_lock:
            lowest_tag_of_all_loops = Tag(Tag._UINT64_MAX, Tag._UINT64_MAX)
            for loop in self._loops:
                if loop_member in loop:
                    lowest_tag = Tag(Tag._UINT64_MAX, Tag._UINT64_MAX)
                    for member in loop:
                        assert member in self._loop_acquired
                        if self._loop_acquired[member] < lowest_tag:
                            lowest_tag = self._loop_acquired[member]
                    if lowest_tag < lowest_tag_of_all_loops:
                        lowest_tag_of_all_loops = lowest_tag
            return tag == lowest_tag_of_all_loops
    
    def is_loop_member(self, reactor_name: str) -> bool:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    return True
        return False

    def update_loop_members(self, loop_detected: LoopDetected) -> None:
        new_set = frozenset([entry[0] for entry in loop_detected.entries if entry[0]] + [loop_detected.origin])
        with self._loops_lock:
            if new_set in self._loops:
                return
            self._loops[new_set] = (Publisher(self.create_loop_topic_name(new_set)), 
                                    Subscriber(self.create_loop_topic_name(new_set), self._on_loop_message), 
                                    set())
            self._reactor.logger.debug(f"New loop detected: {new_set}.")
        self._reactor.notify()
        

    def _on_loop_message(self, msg : LoopMessage):
        if msg.sender == self._reactor.name:
            return
        if msg.success:
            self._on_loop_acquire_success(msg)
        else:
            self._on_loop_acquire_request(msg)


    
"""
Every Reactor is considered federate, therefore has their own scheduler (see run())
"""
class Reactor(Named):
    def __init__(self, name, start_tag, stop_tag, inputs: List[str]=None, outputs: List[str]=None, 
                timers: List[TimerDeclaration] = None, reactions:List[ReactionDeclaration]=None):
        assert start_tag < stop_tag
        super().__init__(name)
        self._logger : logging.Logger = logging.getLogger(self._name)
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
        self._reaction_q.add(self._start_tag, Action("__start__", self))
        self._reaction_q.add(self._stop_tag, Action("__stop__", self))
        self._reaction_q_lock : threading.Lock = threading.Lock()
        self._reaction_q_cv : threading.Condition = LoggingCondition("reaction_q_cv", self.logger, self._reaction_q_lock)
        self._loop_acquiries : LoopAcquireRequests = LoopAcquireRequests()
        self._ledger : FederateLoopLedger = FederateLoopLedger(self)

        self._own_request_sub : Subscriber = Subscriber(self.name + TAG_REQUEST_TOPIC_SUFFIX, self._on_tag_reqest) 
        
    @property
    def start_tag(self) -> Tag:
        return self._start_tag

    @property 
    def stop_tag(self) -> Tag:
        return self._stop_tag

    @property
    def current_tag(self) -> Tag:
        return self._current_tag

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def ledger(self) -> FederateLoopLedger:
        return self._ledger
    
    def _on_tag_reqest(self, tag : Tag):
        if self.schedule_empty_async_at(tag): 
            pass
        else: # scheduling failed -> we are at a later tag already (or at/after the stop tag)
            pass
            # one could resend the current tag here, but assuming that messages are delivered reliably it is not necessary
            # self._own_release_pub.publish(self._reactor.current_tag)

    def _init_reaction(self, reaction: ReactionDeclaration) -> Reaction:
        triggers = [self._find_member_by_name(trigger_name) for trigger_name in reaction.triggers]
        if any(trigger is None or not isinstance(trigger, TriggerAction) for trigger in triggers):
            raise ValueError(f"One or more triggers not found in {self.name}.")

        effects = [self._find_member_by_name(output_name) for output_name in reaction.effects]
        if any(effect is None or not isinstance(effect, Output) for effect in effects):
            raise ValueError(f"One or more effects not found in {self.name}.")

        return Reaction(reaction.name, self, triggers, effects)

    def _init_timer(self, timer : TimerDeclaration) -> Timer:
        return Timer(timer.name, self, timer.offset, timer.interval)

    def __repr__(self) -> str:
        r = super().__repr__()
        r += f"\n\t Start tag: {self.start_tag}"
        r += f"\n\t Stop tag: {self.stop_tag}"
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
        members : List[Named] = self._inputs + self._outputs + self._reactions + self._timers
        for member in members:
            if member.name == name:
                return member
        return None

    def _get_port(self, name : str) -> Port:
        port = self._find_member_by_name(name)
        assert port is not None
        assert isinstance(port, Port)
        return port

    def get_output(self, name : str) -> Output:
        output = self._get_port(name)
        assert isinstance(output, Output)
        return output 

    def get_input(self, name : str) -> Input:
        input = self._get_port(name)
        assert isinstance(input, Input)
        return input 

    def _release_tag(self, tag : Tag):
        self.logger.debug(f"{self._name} releasing {tag}.")
        for callback in self._release_tag_callbacks:
            callback(self._current_tag)

    def schedule_empty_async_at(self, tag : Tag) -> bool:
        with self._reaction_q_cv:
            if self._reaction_q.tag_exists(tag):
                return True
            return self.schedule_action_async(Action("__empty__", self), tag)

    def schedule_action_async(self, action : Action, tag : Optional[Tag]=None) -> bool:
        with self._reaction_q_cv:
            if tag <= self._current_tag or tag >= self._stop_tag:
                self._logger.debug(f"Failed to schedule {action} at {tag}.")
                return False
            self.schedule_action_sync(action, tag)
            self._reaction_q_cv.notify()
        return True
    
    def schedule_loop_acquire_async(self, requested_tag : Tag, request_origin : str) -> bool:
        with self._reaction_q_cv:
            if requested_tag <= self._current_tag or requested_tag >= self._stop_tag:
                return False
            self._loop_acquiries.add(requested_tag, request_origin)
            self._reaction_q_cv.notify()
        return True
    
    def schedule_loop_acquire_sync(self, requested_tag : Tag, request_origin : str) -> bool:
        assert self._reaction_q_lock.locked()
        if requested_tag <= self._current_tag or requested_tag >= self._stop_tag:
            return False
        self._loop_acquiries.add(requested_tag, request_origin)
        return True
    
    # this method assumes that self._reaction_q_cv is held
    def schedule_action_sync(self, action : Action, tag : Optional[Tag] = None) -> bool:
        assert tag > self._current_tag
        assert tag < self._stop_tag
        assert self._reaction_q_lock.locked()
        if tag is None:
            tag = self._current_tag.delay()
        self._reaction_q.add(tag, action)


    def _schedule_timers_once(self) -> None:
        for timer in self._timers:
            timer.schedule_start()

    def _get_triggered_reactions(self, action: TriggerAction) -> List[Reaction]:
        return [reaction for reaction in self._reactions if action in reaction.triggers]
    
    def get_affected_outputs(self, input: Input) -> List[Output]:
        return [output for reaction in self._reactions for output in reaction.effects if input in reaction.triggers and isinstance(output, Output)]

    def wait_for(self, predicate : Callable[[None], bool]) -> bool:
        assert self._reaction_q_lock.locked()
        return self._reaction_q_cv.wait_for(predicate)

    def notify(self) -> None:
        # in python notifying requires holding the lock, this is not the case in cpp
        # see https://stackoverflow.com/questions/46076186/why-does-python-threading-condition-notify-require-a-lock
        # TODO: check if it might be a good idea to acquire in the cpp impl too?
        self.logger.debug(f"{self._name} notifying.")
        with self._reaction_q_cv:
            self._reaction_q_cv.notify()

    def _get_next_tag(self) -> Tuple[Tag, set[str]]:
        assert self._reaction_q_lock.locked()
        loop_acquire_tag = self._loop_acquiries.next_tag()
        reaction_q_tag = self._reaction_q.next_tag()
        if loop_acquire_tag is None or loop_acquire_tag > reaction_q_tag:
            return reaction_q_tag, []
        return loop_acquire_tag, self._loop_acquiries.get_origins(loop_acquire_tag)

    def _next_tag_available(self) -> bool:
        return self._reaction_q.next_tag() is not None or self._loop_acquiries.next_tag() is not None

    def run(self):
        self._logger.debug(f"Thread id: {threading.current_thread().ident}.")
        with self._reaction_q_cv:
            self._schedule_timers_once()
        # releasing the tag directly before the start tag here
        self._release_tag(self._current_tag)
        stop_running : bool = False
        while True:
            actions = []
            with self._reaction_q_cv:
                if not self._next_tag_available():
                    self.wait_for(self._next_tag_available)

                next_tag : Tag
                loop_acquire_origins : set[str] 
                next_tag, loop_acquire_origins = self._get_next_tag()
                refetch_next_tag = False
                if loop_acquire_origins:
                    for input in self._inputs:
                        result : bool = input.loop_acquire_tag(next_tag, loop_acquire_origins, lambda: self._get_next_tag() != (next_tag, loop_acquire_origins))
                        if not result or self._get_next_tag() != (next_tag, loop_acquire_origins): # acquire_tag failed or new event
                            refetch_next_tag = True
                            self.logger.debug(f"refetching next tag.")
                            break
                    if not refetch_next_tag:
                        self._ledger.loop_acquire_success(next_tag, loop_acquire_origins)
                        self._loop_acquiries.remove_tag(next_tag)
                    continue

                self.logger.debug(f"next tag is {next_tag}.")
                for input in self._inputs:
                    result : bool = input.acquire_tag(next_tag, lambda: self._get_next_tag() != (next_tag, loop_acquire_origins))
                    if not result or self._get_next_tag() != (next_tag, loop_acquire_origins): # acquire_tag failed or new event
                        refetch_next_tag = True
                        self.logger.debug(f"refetching next tag.")
                        break
                if refetch_next_tag:
                    continue


                self._current_tag = next_tag
                actions = self._reaction_q.pop_next_tag()
                triggered_actions = [reaction for action in actions if isinstance(action, TriggerAction) for reaction in self._get_triggered_reactions(action)]
                actions += triggered_actions
                if self._current_tag == self._stop_tag:
                    stop_running = True
                
            for action in actions:
                action.exec(self._current_tag)
            
            self._release_tag(self._current_tag)

            if stop_running:
                return



    

