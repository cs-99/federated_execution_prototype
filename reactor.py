"""
This is a prototype for the decentralized coordination contract used for federated (and enclaves) execution of Lingua Franca
"""
from __future__ import annotations
from dataclasses import dataclass
from functools import total_ordering
from typing import Callable, Dict, List, Optional, Tuple
import threading
from pubsub import Subscriber, Publisher
import logging # thread safe by default

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

    def connect(self, reactor_name : str, output_name : str,  delay : Optional[int] = None):
        if self._is_connected:
            raise ValueError(f"{self._reactor}/{self.name} is already connected.")
        self._connected_reactor_name = reactor_name
        self._reactor.ledger.connect_tag_rel_req(reactor_name)
        self._message_sub = Subscriber(f"{reactor_name}/{output_name}", self._receive_message)
        self._loop_discovery_sub = Subscriber(f"{reactor_name}/{output_name}{LOOP_DISCOVERY_TOPIC_SUFFIX}", self._on_loop_discovery)
        self._loop_detected_sub = Subscriber(f"{reactor_name}/{output_name}{LOOP_DETECTED_TOPIC_SUFFIX}", self._on_loop_detected)
        self._is_connected = True
        self._delay = delay
    
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

    def _on_loop_detected(self, loop_discovery: LoopDiscovery):
        self._reactor.ledger.update_loop_members(loop_discovery)
        if loop_discovery.origin == self._reactor.name:
            return
        for output in self._reactor.get_affected_outputs(self):
            output.relay_loop_detected(loop_discovery)
        
    def _receive_message(self, tag : Tag):
        if self._delay is not None:
            self._reactor.schedule_action_async(self, tag.delay(self._delay))
        else:
            self._reactor.schedule_action_async(self, tag)
        # not necessary as a release tag message will be sent on release
        #self._reactor.ledger.update_release(self._connected_reactor_name, tag)

    def _acquire_tag_predicate(self, tag_to_acquire : Tag, loop_acquire_origins: set[str], predicate : Callable[[None], bool]):
        # connected reactor released the tag
        result = tag_to_acquire <= self._reactor.ledger.get_release(self._connected_reactor_name)
        # or all loop members have acquired the tag (for every loop that the connected reactor is part of)
        # this is when this reactor started the loop acquire
        result = result or self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag_to_acquire)
        # or the connected reactor is part of (one of the) loop(s) which started the loop acquire
        # this is when loops are discovered later than the acquire was started
        result = result or self._reactor.ledger.is_part_of_same_loops(self._connected_reactor_name, loop_acquire_origins)
        # or the predicate is true (i.e. the actionlist in the scheduling has been modified)
        result = result or predicate()
        return result

    def acquire_tag(self, tag : Tag, loop_acquire_origins: set[str], predicate : Callable[[None], bool] = lambda: False) -> bool:
        tag_to_acquire = tag
        if self._delay is not None:
            tag_to_acquire = tag.subtract(self._delay)
        self._reactor.logger.debug(f"{self.name} acquiring {tag_to_acquire}.")
        if tag_to_acquire <= self._reactor.ledger.get_release(self._connected_reactor_name) \
            or self._connected_reactor_name in loop_acquire_origins \
            or self._reactor.ledger.is_part_of_same_loops(self._connected_reactor_name, loop_acquire_origins) \
            or not self._is_connected:
            return True
        if self._reactor.ledger.is_loop_member(self._connected_reactor_name):
            self._reactor.ledger.loop_acquire_request(self._connected_reactor_name, tag_to_acquire)
        else:
            self._reactor.ledger.request_empty_event_at(self._connected_reactor_name, tag_to_acquire)
        self._reactor.logger.debug(f"{self.name} waiting for tag release {tag_to_acquire}.")
        return self._reactor.wait_for(lambda: self._acquire_tag_predicate(tag_to_acquire, loop_acquire_origins, predicate))

class Output(Port):
    def __init__(self, name, reactor : Reactor):
        super().__init__(name, reactor)
        self._is_connected : bool = False
        self._message_pub : Optional[Publisher] = None
        self._loop_discovery_pub: Optional[Publisher] = None
        self._loop_detected_pub : Optional[Publisher] = None

    def set(self, tag : Tag):
        if self._is_connected:
            self._message_pub.publish(tag)

    def connect(self, reactor_name : str):
        self._reactor.ledger.connect_tag_rel_req(reactor_name)
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
class ActionListEntry:
    tag: Tag
    actions: List[Action]
    loop_acquire_requests: set[str]

    def __lt__(self, other: ActionListEntry) -> bool:
        return self.tag < other.tag

    def __eq__(self, other: ActionListEntry) -> bool:
        return self.tag == other.tag

# this is protected by reaction_q_cv
class ActionList:
    def __init__(self) -> None:
        self._list : List[ActionListEntry] = []

    def add_action(self, tag: Tag, action: Action) -> None:
        for entry in self._list:
            if entry.tag == tag:
                entry.actions.append(action)
                return
        self._list.append(ActionListEntry(tag, [action], set()))
        self._list.sort()
    
    def add_request(self, tag: Tag, request_origin: str) -> None:
        for entry in self._list:
            if entry.tag == tag:
                entry.loop_acquire_requests.add(request_origin)
                return
        self._list.append(ActionListEntry(tag, [], {request_origin}))
        self._list.sort()

    def pop_tag(self, tag: Tag) -> List[Action]:
        assert len(self._list) > 0
        assert any(entry.tag == tag for entry in self._list), f"Tag {tag} not found in the list"
        actions = [entry.actions for entry in self._list if entry.tag == tag][0]
        self._list = [entry for entry in self._list if entry.tag != tag]
        return actions
    
    def remove_loop_acquire_requests(self):
        assert len(self._list) > 0
        self._list[0].loop_acquire_requests = set()
    
    def next_tag(self) -> Tuple[Tag, set[str]]:
        assert len(self._list) > 0
        return self._list[0].tag, self._list[0].loop_acquire_requests
        
    def __bool__(self) -> bool:
        return bool(self._list)

    def __repr__(self) -> str:
        r = ""
        for entry in self._list:
            r += repr(entry)
        return r
    
@dataclass
class LoopDiscovery:
    origin : str
    origin_output : str
    # TODO: add delay to loop detection
    entries : List[tuple[str, str, str]] # reactor, input, output

@dataclass
class LoopDetected(LoopDiscovery):
    origin_input : str

LOOP_ACQUIRE_REQUEST_TOPIC_SUFFIX = "/__loop_acquire_request__"
LOOP_ACQUIRE_SUCCESS_TOPIC_SUFFIX = "/__loop_acquire_success__"
class FederateLedger(RegisteredReactor):
    def __init__(self, reactor) -> None:
        super().__init__(reactor)
        self._released_lock : threading.Lock = threading.Lock()
        self._released : Dict[str, Tag] = {}
        self._loop_member_lock : threading.Lock = threading.Lock()
        self._loop_members : set[frozenset] = set()

        self._own_release_pub : Publisher = Publisher(self._reactor.name + TAG_RELEASE_TOPIC_SUFFIX)
        self._own_request_sub : Subscriber = Subscriber(self._reactor.name + TAG_REQUEST_TOPIC_SUFFIX, self._on_tag_reqest) 
        self._reactor.register_release_tag_callback(self._own_release_pub.publish)

        # these are not thread safe as connections are supposed to be drawn before running the reactors
        self._release_subs : Dict[str, Subscriber] = {}
        self._request_pubs : Dict[str, Publisher] = {}

        self._loop_acquire_request_sub : Subscriber = Subscriber(self._reactor.name + LOOP_ACQUIRE_REQUEST_TOPIC_SUFFIX, self._on_loop_acquire_request)
        self._loop_acquire_success_pub : Publisher = Publisher(self._reactor.name + LOOP_ACQUIRE_SUCCESS_TOPIC_SUFFIX)
        # these are protected by loop_member_lock too
        self._loop_acquire_request_pubs : Dict[str, Publisher] = {}
        self._loop_acquire_success_subs : Dict[str, Subscriber] = {}
        self._loop_acquired : Dict[str, Tag] = {}

    def update_release(self, reactor_name: str, tag: Tag) -> None:
        with self._released_lock:
            current = self._released.get(reactor_name)
            if current is None or tag > current:
                self._released[reactor_name] = tag
        self._reactor.notify()

    def _on_loop_acquire_request(self, request: Tuple[str, Tag]):
        reactor_name, tag = request
        self._reactor.logger.debug(f"Loop acquire request from {reactor_name} at {tag}.")
        self._reactor.schedule_loop_acquire(tag, reactor_name)
    
    def is_part_of_same_loops(self, reactor_name: str, loop_acquire_origins: set[str]) -> bool:
        with self._loop_member_lock:
            for loop in self._loop_members:
                if reactor_name in loop:
                    for origin in loop_acquire_origins:
                        if origin in loop:
                            return True
        return False

    def loop_acquire_success(self, tag: Tag, loop_acquire_origins: set[str]) -> None:
        for origin in loop_acquire_origins:
            self._loop_acquire_success_pub.publish((origin, tag))
    
    def loop_acquire_request(self, reactor_name: str, tag: Tag) -> None:
        with self._loop_member_lock:
            for loop in self._loop_members:
                if reactor_name in loop:
                    for member in loop:
                        if member != self._reactor.name:
                            self._loop_acquire_request_pubs[member].publish((self._reactor.name, tag))

    def _on_loop_acquire_success(self, member : str,  success: Tuple[str, Tag]):
        if success[0] != self._reactor.name:
            return
        with self._loop_member_lock:
            if self._loop_acquired.get(member) is None or success[1] > self._loop_acquired[member]:
                self._loop_acquired[member] = success[1]
        self._reactor.notify()

    def check_loops_acquired(self, loop_member, tag: Tag) -> bool:
        found = False # if member is not part of a known loop we return False
        with self._loop_member_lock:
            for loop in self._loop_members:
                if loop_member in loop:
                    found = True
                    for member in loop:
                        if member != self._reactor.name:
                            if member not in self._loop_acquired or self._loop_acquired[member] < tag:
                                return False
        return found
    
    def is_loop_member(self, reactor_name: str) -> bool:
        with self._loop_member_lock:
            for loop in self._loop_members:
                if reactor_name in loop:
                    return True
        return False

    def update_loop_members(self, loop_detected: LoopDetected) -> None:
        new_set = frozenset([entry[0] for entry in loop_detected.entries if entry[0]] + [loop_detected.origin])
        with self._loop_member_lock:
            if new_set in self._loop_members:
                return
            self._loop_members.add(new_set)
            for member in new_set:
                self._loop_acquire_request_pubs[member] = Publisher(member + LOOP_ACQUIRE_REQUEST_TOPIC_SUFFIX)
                self._loop_acquire_success_subs[member] = Subscriber(member + LOOP_ACQUIRE_SUCCESS_TOPIC_SUFFIX, lambda msg: self._on_loop_acquire_success(member, msg))
            self._reactor.logger.debug(f"Loop members: {self._loop_members}")
        self._reactor.notify()

    def get_release(self, reactor_name) -> Tag:
        with self._released_lock:
            return self._released.setdefault(reactor_name, Tag(0, 0))

    def request_empty_event_at(self, reactor_name : str, tag : Tag):
        assert reactor_name in self._request_pubs
        self._reactor.logger.debug(f"Requesting empty event at {reactor_name} {tag}")
        self._request_pubs.get(reactor_name).publish(tag)

    def _on_tag_reqest(self, tag : Tag):
        if self._reactor.schedule_empty_async_at(tag): 
            pass
        else: # scheduling failed -> we are at a later tag already (or at/after the stop tag)
            pass
            # one could resend the current tag here, but assuming that messages are delivered reliably it is not necessary
            # self._own_release_pub.publish(self._reactor.current_tag)

    def connect_tag_rel_req(self, reactor_name):
        if reactor_name in self._release_subs:
            return
        self._release_subs[reactor_name] = Subscriber(reactor_name + TAG_RELEASE_TOPIC_SUFFIX, (lambda n: lambda t: self.update_release(n, t))(reactor_name))
        self._request_pubs[reactor_name] = Publisher(reactor_name + TAG_REQUEST_TOPIC_SUFFIX)

    
"""
Every Reactor is considered federate, therefore has their own scheduler (see run())
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
        self._reaction_q.add_action(self._start_tag, Action("__start__", self))
        self._reaction_q.add_action(self._stop_tag, Action("__stop__", self))
        self._reaction_q_lock : threading.Lock = threading.Lock()
        self._reaction_q_cv : threading.Condition = threading.Condition(self._reaction_q_lock)
        self._ledger : FederateLedger = FederateLedger(self)

        self._logger : logging.Logger = logging.getLogger(self._name)
        
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
    def ledger(self) -> FederateLedger:
        return self._ledger

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
        self.logger.debug(f"releasing {tag}.")
        for callback in self._release_tag_callbacks:
            callback(self._current_tag)

    def schedule_empty_async_at(self, tag : Tag) -> bool:
        return self.schedule_action_async(Action("__empty__", self), tag)

    def schedule_action_async(self, action : Action, tag : Optional[Tag]=None) -> bool:
        with self._reaction_q_cv:
            if tag <= self._current_tag or tag >= self._stop_tag:
                return False
            self.schedule_action_sync(action, tag)
            self._reaction_q_cv.notify()
        return True
    
    def schedule_loop_acquire(self, requested_tag : Tag, request_origin : str) -> bool:
        with self._reaction_q_cv:
            if requested_tag <= self._current_tag or requested_tag >= self._stop_tag:
                return False
            self._reaction_q.add_request(requested_tag, request_origin)
            self._reaction_q_cv.notify()
        return True
    
    # this method assumes that self._reaction_q_cv is held
    def schedule_action_sync(self, action : Action, tag : Optional[Tag] = None) -> bool:
        assert tag > self._current_tag
        assert tag < self._stop_tag
        assert self._reaction_q_lock.locked()
        if tag is None:
            tag = self._current_tag.delay()
        self._reaction_q.add_action(tag, action)

    # remove tags up until the current tag
    # unused, as the current tag gets initialized to the start_tag.decrement at construction time already
    # and therefore requests from other federates to schedule empty events will fail anyway when starting threads with .run()
    def _remove_tags_lte_current(self):
        tags_to_remove = []
        for tag in self._reaction_q.keys():
            if tag <= self._current_tag:
                tags_to_remove.append(tag)
        for tag in tags_to_remove:
            del self._reaction_q[tag]

    def _schedule_timers_once(self) -> None:
        for timer in self._timers:
            timer.schedule_start()

    def _get_triggered_reactions(self, action: TriggerAction) -> List[Reaction]:
        return [reaction for reaction in self._reactions if action in reaction.triggers]
    
    def get_affected_outputs(self, input: Input) -> List[Output]:
        return [output for reaction in self._reactions for output in reaction.effects if input in reaction.triggers and isinstance(output, Output)]

    def wait_for(self, predicate) -> bool:
        return self._reaction_q_cv.wait_for(predicate)

    def notify(self) -> None:
        # in python notifying requires holding the lock, this is not the case in cpp
        # see https://stackoverflow.com/questions/46076186/why-does-python-threading-condition-notify-require-a-lock
        # TODO: check if it might be a good idea to acquire in the cpp impl too?
        with self._reaction_q_cv:
            self._reaction_q_cv.notify()

    def run(self):
        with self._reaction_q_cv:
            self._schedule_timers_once()
        # releasing the tag directly before the start tag here
        self._release_tag(self._current_tag)
        stop_running : bool = False
        while True:
            actions = []
            acquiring_for_loop : bool = False
            with self._reaction_q_cv:
                if not self._reaction_q:
                    self.wait_for(lambda: bool(self._reaction_q))

                next_tag : Tag
                loop_acquire_origins : set[str] 
                next_tag, loop_acquire_origins = self._reaction_q.next_tag()
                acquiring_for_loop = bool(loop_acquire_origins)
                self.logger.debug(f"next tag is {next_tag}.")
                refetch_next_tag : bool = False
                for input in self._inputs:
                    result : bool = input.acquire_tag(next_tag, loop_acquire_origins, lambda: self._reaction_q.next_tag() != (next_tag, loop_acquire_origins))
                    if not result or self._reaction_q.next_tag() != (next_tag, loop_acquire_origins): # acquire_tag failed or actionlist modified
                        refetch_next_tag = True
                        self.logger.debug(f"refetching next tag.")
                        break
                if refetch_next_tag:
                    continue

                if acquiring_for_loop:
                    self._ledger.loop_acquire_success(next_tag, loop_acquire_origins)
                    self._reaction_q.remove_loop_acquire_requests()
                    continue

                self._current_tag = next_tag
                actions = self._reaction_q.pop_tag(self._current_tag)
                triggered_actions = [reaction for action in actions if isinstance(action, TriggerAction) for reaction in self._get_triggered_reactions(action)]
                actions += triggered_actions
                if self._current_tag == self._stop_tag:
                    stop_running = True
                self._release_tag(self._current_tag)
            for action in actions:
                action.exec(self._current_tag)
            
            if stop_running:
                return



    

