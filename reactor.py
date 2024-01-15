"""
This is a prototype for the decentralized coordination contract used for federated (and enclaves) execution of Lingua Franca
"""
from __future__ import annotations
from dataclasses import dataclass
from functools import total_ordering
import time
from typing import Callable, Dict, List, Optional, Set, Tuple, Any
import threading
import logging # thread safe by default
import copy

# custom files from here
from tag import Tag
import utility
from logging_primitives import LoggingCondition, LoggingLock
from pubsub import Subscriber, Publisher
from declarations import TimerDeclaration, ReactionDeclaration
from messagetypes import Loop, LoopAcquireRequest, MessageToInput, PortMessage, LoopDiscovery, LoopDetected, ReleaseMessage, RequestMessage


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
        self._delay : Optional[int] = None
        self._connected_reactor_name : Optional[str] = None

    def connect(self, reactor_name : str,  delay : Optional[int] = None):
        if self._is_connected:
            raise ValueError(f"{self._reactor}/{self.name} is already connected.")
        self._connected_reactor_name = reactor_name
        self._reactor.logger.debug(f"{self._reactor.name}/{self.name} connecting to {reactor_name}.")
        self._reactor.connections.connect_backwards(reactor_name)
        self._delay = delay
        self._is_connected = True

        
    def on_message(self, msg : PortMessage):
        if self._delay is not None:
            self._reactor.schedule_action_async(self, msg.tag.delay(self._delay))
        else:
            self._reactor.schedule_action_async(self, msg.tag)
        self._reactor.notify()
        

    def _acquire_tag_predicate(self, tag: Tag, tag_before_delay : Tag, predicate : Callable[[None], bool]) -> bool:
        # connected reactor released the tag
        if self._reactor.ledger.has_released(self._connected_reactor_name, tag_before_delay):
            self._reactor.logger.debug(f"{self.name} acquired {tag_before_delay} because it was released by {self._connected_reactor_name}.")
            return True
        # or all loop members have acquired the tag (for every loop that the connected reactor is part of)
        # this is when this reactor started the loop acquire
        # however, it must be before the last acquired tag
        # this is the case for the reactor that has the first delay before the original event that triggered the loop acquire
        if self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag_before_delay) \
             and not self._reactor.ledger.is_last_tag_fully_acquired_by_loops(self._connected_reactor_name, tag_before_delay):
                self._reactor.logger.debug(f"{self.name} acquired {tag_before_delay} because all loop members have acquired it.")
                return True
        # we have discovered that we are part of a loop (and have not requested the corresponding loop acquire yet)
        if self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
                and not self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag) \
                and not self._reactor.ledger.tag_requests_lte_open_in_loops(self._connected_reactor_name, tag):
            self._reactor.logger.debug(f"{self.name} requested loop acquire for {tag_before_delay}.")
            self._reactor.ledger.request_acquire_loops_where_is_member(self._connected_reactor_name, tag)
            return True
        
        if self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
            and  self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag) \
            and tag_before_delay < tag:
                return True
        
        if not self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
            or self._reactor.ledger.is_last_tag_fully_acquired_by_loops(self._connected_reactor_name, tag_before_delay):
            self._reactor.connections.request_empty_event_at(self._connected_reactor_name, tag_before_delay)
        # or the predicate is true (i.e. the actionlist in the scheduling has been modified)
        return predicate()
    
    def _loop_acquire_predicate(self, tag : Tag, tag_before_delay : Tag, loop_acquire_origins: Set[str], predicate : Callable[[None], bool]) -> bool:
        # if we are part of one of the requested loops
        if self._reactor.ledger.is_part_of_same_loops(self._connected_reactor_name, loop_acquire_origins):
            return True
        
        # or we have acquired the tag normally
        return self._acquire_tag_predicate(tag, tag_before_delay, predicate)
    
    def _subtract_delay(self, tag : Tag) -> Tag:
        new_tag = tag
        if self._delay is not None:
            new_tag = tag.subtract(self._delay)
        return new_tag
        
    
    def loop_acquire_tag(self, tag : Tag, loop_acquire_origins: Set[str], predicate : Callable[[None], bool] = lambda: False) -> bool:
        tag_before_delay = self._subtract_delay(tag)
        
        if not self._is_connected:
            return True
        
        if self._reactor.ledger.is_part_of_same_loops(self._connected_reactor_name, loop_acquire_origins):
            return True
        
        return self._reactor.wait_for(lambda: self._loop_acquire_predicate(tag, tag_before_delay, loop_acquire_origins, predicate))


    def loop_dependencies_released(self, loop_acquire_origins: Set[str], tag: Tag) -> bool:
        if not self._is_connected:
            return True

        # those have been acquired already
        if not self._reactor.ledger.is_part_of_same_loops(self._connected_reactor_name, loop_acquire_origins):
            return True
        
        tag_before_delay = self._subtract_delay(tag)

        return self._reactor.ledger.has_released(self._connected_reactor_name, tag_before_delay)
        


    def acquire_tag(self, tag : Tag, predicate : Callable[[None], bool] = lambda: False) -> bool:
        tag_before_delay = self._subtract_delay(tag)
        
        if self._reactor.ledger.has_released(self._connected_reactor_name, tag_before_delay)\
            or not self._is_connected:
            return True
        
        # see acquire_tag_predicate for explanation
        if self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag_before_delay) \
            and not self._reactor.ledger.is_last_tag_fully_acquired_by_loops(self._connected_reactor_name, tag_before_delay):
                return True
        
        if self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
            and self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag) \
            and tag_before_delay < tag:
                return True
        
        if self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
            and not self._reactor.ledger.check_loops_acquired(self._connected_reactor_name, tag) \
            and not self._reactor.ledger.tag_requests_lte_open_in_loops(self._connected_reactor_name, tag):
                self._reactor.ledger.request_acquire_loops_where_is_member(self._connected_reactor_name, tag)
                return False
        
        if not self._reactor.ledger.is_loop_member(self._connected_reactor_name) \
            or self._reactor.ledger.is_last_tag_fully_acquired_by_loops(self._connected_reactor_name, tag_before_delay):
            self._reactor.connections.request_empty_event_at(self._connected_reactor_name, tag_before_delay)
        self._reactor.logger.debug(f"{self.name} waiting for tag release {tag_before_delay}.")
        return self._reactor.wait_for(lambda: self._acquire_tag_predicate(tag, tag_before_delay, predicate))


class Output(Port):
    def __init__(self, name, reactor : Reactor):
        super().__init__(name, reactor)
        self._connected_reactor_names : set[str] = set()
    
    def connect(self, reactor_name : str, input_name : str):
        self._reactor.connections.connect_forward(reactor_name)
        self._connected_reactor_names.add(reactor_name)
        self._reactor.connections.register_outgoing_connection(self.name, reactor_name, input_name)

    def start_loop_discovery(self):
        self._reactor.connections.start_loop_discovery(self.name)

    def set(self, tag : Tag):
        for reactor_name in self._connected_reactor_names:
            self._reactor.ledger.reset_loop_requests(reactor_name)
        self._reactor.connections.send_portmessage(self._name, tag, None)
        
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
        if not any(entry.tag == tag for entry in self._list):
            return []
        actions = [entry.actions for entry in self._list if entry.tag == tag][0]
        self._list = [entry for entry in self._list if entry.tag != tag]
        return actions
    
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
    requests: Dict[str, List[Tuple[Loop, int]]] # request origin, 
                                                #list of loops requested (each loop is list of reactor names)
                                                # and the id of the request

class LoopAcquireRequests:
    def __init__(self) -> None:
        self._list: List[LoopAcquireEntry] = []

    def add(self, tag :Tag, request_origin: str, loop_req: Tuple[Loop, int]) -> None:
        for entry in self._list:
            if entry.tag == tag:
                entry.requests.setdefault(request_origin, []).append(loop_req)
                return
        entry = LoopAcquireEntry(tag, {request_origin: [loop_req]})
        self._list.append(entry)
        self._list.sort()

    def remove_request(self, tag: Tag, request_origins: Set[str]) -> None:
        assert any(entry.tag == tag for entry in self._list), f"No entry found for tag: {tag}"
        matching_entry = [entry for entry in self._list if entry.tag == tag][0]
        requests_copy = matching_entry.requests.copy()  # Create a copy of the requests dictionary
        for member in request_origins:
            requests_copy.pop(member)  # Remove entries from the copy
        matching_entry.requests = requests_copy  # Assign the updated copy back to the original dictionary
        if not matching_entry.requests:
            self._list.remove(matching_entry)
    
    def get_request_origins(self, tag: Tag) -> Dict[str, List[Tuple[Loop, int]]]:
        assert any(entry.tag == tag for entry in self._list), f"No entry found for tag: {tag}"
        matching_entry = [entry for entry in self._list if entry.tag == tag][0]
        return matching_entry.requests 

    def next_tag(self) -> Optional[Tag]:
        if self._list:
            return self._list[0].tag
        return None

    def __repr__(self) -> str:
        return ''.join(repr(entry) for entry in self._list)

LOOP_PREFIX = "__loop__"
class LoopEntry:
    def __init__(self, loop : Loop) -> None:
        self._loop : Loop = loop
        self.self_requested : Optional[Tag] = None
        self.self_requested_id : int = 0
        self.last_tag_loop_acquired : Tag = Tag(0, 0)

    @property
    def loop(self) -> Loop:
        return self._loop

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, LoopEntry):
            return False
        return self._loop == __value._loop
    
    def __hash__(self) -> int:
        return hash(tuple(sorted(self._loop)))
    
    def __repr__(self) -> str:
        return f"LoopEntry({self._loop})"

    def __iter__(self):
        return iter(self._loop)
    
    def __contains__(self, item):
        return item in self._loop


class FederateLedger(RegisteredReactor):
    def __init__(self, reactor) -> None:
        super().__init__(reactor)
        self._loops_lock : threading.Lock = LoggingLock("loops_lock", self._reactor.logger)
        self._loops : Set[LoopEntry] = set()
        self._releases_lock : threading.Lock = threading.Lock()
        self._releases : Dict[str, Tag] = {}

    def reset_loop_requests(self, reactor_name: str) -> None:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    loop.self_requested = None
                    loop.last_tag_loop_acquired = Tag(0,0)

    def release_other(self, reactor_name: str, tag: Tag) -> None:
        self._reactor.logger.debug(f"Release from {reactor_name} at {tag}.")
        with self._releases_lock:
            assert self._releases.get(reactor_name) is None or self._releases[reactor_name] < tag
            self._releases[reactor_name] = tag
        self._reactor.notify()

    def has_released(self, reactor_name : str, tag : Tag) -> bool:
        with self._releases_lock:
            return reactor_name in self._releases and self._releases[reactor_name] >= tag

    def on_loop_acquire_request(self, sender_name : str, request: LoopAcquireRequest):
        self._reactor.logger.debug(f"Loop acquire request from {sender_name} at {request.tag}, originating at {request.origin} in loop {request.loop}.")
        if self._reactor.name != request.origin:
            if self._reactor.schedule_loop_acquire_async(request.tag, request.origin, (request.loop, request.request_id)):
                pass
            else:
                self._reactor.connections.send_loop_ac_req(request.loop.get_next_loop_member(self._reactor.name), request)
            return
            
        with self._loops_lock:
            for loop_entry in self._loops:
                if loop_entry.loop == request.loop:
                    if loop_entry.self_requested != request.tag or loop_entry.self_requested_id != request.request_id:
                        # this is not the last request we sent, 
                        # therefore another (earlier) event must have occured
                        # that event could have scheduled events in the loop which are behind the request received here
                        # that message is useless now
                        return
                    loop_entry.last_tag_loop_acquired = request.tag
                    loop_entry.self_requested = None
                    self._reactor.logger.debug(f"Loop {loop_entry} acquired {request.tag}.")
                    break
        self._reactor.notify()
        return
        

    def is_part_of_same_loops(self, reactor_name: str, request_origins: Set[str]) -> bool:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    for member in request_origins:
                        if member in loop:
                            return True
        return False
    
    def forward_loop_acquire_requests(self, tag: Tag, requests: Dict[str, List[Tuple[Loop, int]]]) -> None:
        for request_origin, loop_reqs in requests.items():
            for loop_req in loop_reqs:
                self._reactor.connections.send_loop_ac_req(
                        loop_req[0].get_next_loop_member(self._reactor.name),
                        LoopAcquireRequest(tag, request_origin, loop_req[0], loop_req[1])
                    ) 

    def request_acquire_loops_where_is_member(self, reactor_name: str, tag: Tag) -> None:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    loop.self_requested_id += 1
                    loop.self_requested = tag
                    self._reactor.schedule_loop_acquire_sync(tag, self._reactor.name, (loop.loop, loop.self_requested_id))
    
    def tag_requests_lte_open_in_loops(self, reactor_name: str, tag: Tag) -> bool:
        with self._loops_lock:
            for loop in self._loops:
                if reactor_name in loop:
                    if loop.self_requested is None:
                        return False
                    if not loop.self_requested <= tag:
                        return False
            return True

    def check_loops_acquired(self, loop_member, tag: Tag) -> bool:
        found = False # if member is not part of a known loop we return False
        with self._loops_lock:
            for loop in self._loops:
                if loop_member in loop:
                    found = True
                    if loop.last_tag_loop_acquired < tag:
                        return False
        return found
    
    # this method is used after check_loops_acquired to check whether the tag is the last one that was acquired
    def is_last_tag_fully_acquired_by_loops(self, loop_member, tag: Tag) -> bool:
        with self._loops_lock:
            lowest_tag_of_all_loops = Tag(Tag._UINT64_MAX, Tag._UINT64_MAX)
            for loop in self._loops:
                if loop_member in loop:
                    lowest_tag = loop.last_tag_loop_acquired
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
        self._reactor.logger.debug(f"Updating loop members for {loop_detected}.")
        new_entry = LoopEntry(Loop([loop_detected.origin] + [entry[0] for entry in loop_detected.entries]))
        with self._loops_lock:
            self._loops.add(new_entry)
        self._reactor.notify()
        

LF_CONNECTION_PREFIX = "__lf__"
LF_BACKWARDS_CONNECTION_PREFIX = "__lf_backwards__"
RECEIVING_INPUT_PLACEHOLDER = "__receiving_input__"
class ReactorConnections(RegisteredReactor):
    def __init__(self, reactor: Reactor) -> None:
        super().__init__(reactor)
        self._outgoing_connections : Dict[str, List[Tuple[str, str]]] = {}
        # these are for the regular connections (and discovery, release, loop acquire requests)
        self._publishers : Dict[str, Publisher] = {}
        self._subscribers : Dict[str, Subscriber] = {}
        # these are for empty event schedule requests, since they go the other direction
        # note: splitting the directions makes sure that messages are not handled by reactors that were not supposed to get the message
        # i.e. imagine a -> b -> c without splitting directions, 
        # would mean that b would receive all messages from a and c, as it has to subscribe to c for empty event requests
        # note: this does not affect the loop discovery/acquire, as those just require in-order-delivery in one direction
        self._backwards_publishers : Dict[str, Publisher] = {}
        self._forward_subscribers: Dict[str, Subscriber] = {}
        self._reactor.register_release_tag_callback(self._on_release)

    def start_loop_discovery(self, output_name : str):
        self._reactor.logger.debug(f"Starting loop discovery for {output_name}.")
        self._reactor.logger.debug(f"{self._outgoing_connections}")
        self._send_message_to_connected_inputs(output_name, LoopDiscovery(RECEIVING_INPUT_PLACEHOLDER, self._reactor.name, output_name, []))

    def _on_release(self, tag : Tag):
        for publisher in self._publishers.values():
            publisher.publish(ReleaseMessage(tag))

    def _send_message_to_connected_inputs(self, output_name : str, message : MessageToInput):
        if output_name not in self._outgoing_connections:
            return
        set_receiving_input = message.receiving_input == RECEIVING_INPUT_PLACEHOLDER
        for connection in self._outgoing_connections[output_name]:
            if set_receiving_input:
                message.receiving_input = connection[1]
            self._reactor.logger.debug(f"Sending {message} to {connection[0]}.")
            self._publishers[connection[0]].publish(message)

    def send_portmessage(self, output_name: str, tag: Tag, message: Any):
        self._send_message_to_connected_inputs(output_name, PortMessage(RECEIVING_INPUT_PLACEHOLDER, tag, message))

    def send_loop_ac_req(self, reactor_name: str, msg: LoopAcquireRequest):
        self._publishers[reactor_name].publish(msg)

    def request_empty_event_at(self, reactor_name: str, tag: Tag):
        self._backwards_publishers[reactor_name].publish(RequestMessage(tag))

    def connect_backwards(self, other_reactor_name: str):
        if other_reactor_name not in self._backwards_publishers:
            self._backwards_publishers[other_reactor_name] = Publisher(LF_BACKWARDS_CONNECTION_PREFIX + self._reactor.name + "/" + other_reactor_name)
            self._subscribers[other_reactor_name] = Subscriber(LF_CONNECTION_PREFIX + other_reactor_name + "/" + self._reactor.name, lambda msg: self._on_message(other_reactor_name, msg))

    def connect_forward(self, other_reactor_name: str):
        if other_reactor_name not in self._publishers:
            self._publishers[other_reactor_name] = Publisher(LF_CONNECTION_PREFIX + self._reactor.name + "/" + other_reactor_name)
            self._forward_subscribers[other_reactor_name] = Subscriber(LF_BACKWARDS_CONNECTION_PREFIX + other_reactor_name + "/" + self._reactor.name, lambda msg: self._on_empty_event_request(other_reactor_name, msg))

    def register_outgoing_connection(self, output_name, other_reactor_name: str, other_reactors_input):
        if output_name not in self._outgoing_connections:
            self._outgoing_connections[output_name] = [(other_reactor_name, other_reactors_input)]
        else:
            self._outgoing_connections[output_name].append((other_reactor_name, other_reactors_input))

    def _on_empty_event_request(self, sender_name: str, msg: RequestMessage):
        assert isinstance(msg, RequestMessage)
        self._reactor.schedule_empty_async_at(msg.tag)

    def _on_message(self, sender_name: str, msg : Any):
        self._reactor.logger.debug(f"Message from {sender_name}: {msg}")
        if type(msg) is LoopAcquireRequest:
            self._reactor.ledger.on_loop_acquire_request(sender_name, msg)
        elif type(msg) is LoopDiscovery:
            self._on_loop_discovery(msg)
        elif type(msg) is LoopDetected:
            self._on_loop_detected(msg)
        elif type(msg) is PortMessage:
            self._reactor.get_input(msg.receiving_input).on_message(msg)
            # self._reactor.ledger.release_other(sender_name, msg.tag) technically not necessary
        elif type(msg) is ReleaseMessage:
            self._reactor.ledger.release_other(sender_name, msg.tag)
        else:
            raise ValueError(f"Unknown message type: {msg}.")
        
    def _on_loop_discovery(self, loop_discovery : LoopDiscovery):
        if loop_discovery.origin == self._reactor.name:
            self._reactor.logger.debug(f"Loop discovered at {self._reactor.name}.")
            affected_outputs = self._reactor.get_affected_outputs(self._reactor.get_input(loop_discovery.receiving_input))
            affected_output_names = [output.name for output in affected_outputs]
            if loop_discovery.origin_output in affected_output_names:
                # maybe TODO: make sure the loop has a delay or we are stuck at this tag forever
                # however, this could also be further down the loop if it is a nested loop
                # so either expand the discovery to track the entire loop or 
                # just dont mind at all whether the loop triggers itself without delay (as the user might actually want that)
                self._reactor.logger.debug(f"Loop triggers itself.")
            # TODO: if the loop has no delay, all of the loop participants have to "release together", as the loop could come back around with a new event but still being at the same tag (aka loop release)
            loop_detected_msg = LoopDetected(loop_discovery.entries[0][1], loop_discovery.origin, loop_discovery.origin_output, loop_discovery.entries, loop_discovery.receiving_input, current_entry=0)
            self._reactor.ledger.update_loop_members(loop_detected_msg)
            self._publishers[loop_discovery.entries[0][0]].publish(loop_detected_msg)
        else:
            affected_outputs = self._reactor.get_affected_outputs(self._reactor.get_input(loop_discovery.receiving_input))
            for output in affected_outputs:
                loop_discovery.entries.append((self._reactor.name, loop_discovery.receiving_input, output.name))
                loop_discovery.receiving_input = RECEIVING_INPUT_PLACEHOLDER
                self._send_message_to_connected_inputs(output.name, loop_discovery)


    def _on_loop_detected(self, loop_detected: LoopDetected):
        self._reactor.ledger.update_loop_members(loop_detected)
        next_index = loop_detected.current_entry + 1
        if next_index < len(loop_detected.entries):
            loop_detected.current_entry = next_index
            self._publishers[loop_detected.entries[next_index][0]].publish(loop_detected)
    
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

        self._ledger : FederateLedger = FederateLedger(self)
        self._connections : ReactorConnections = ReactorConnections(self)

        self._own_request_sub : Subscriber = Subscriber(self.name + TAG_REQUEST_TOPIC_SUFFIX, self._on_tag_reqest) 
        
    @property
    def start_tag(self) -> Tag:
        return self._start_tag
    
    @property
    def connections(self) -> ReactorConnections:
        return self._connections

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
    
    @property
    def reaction_q_cv(self) -> threading.Condition:
        return self._reaction_q_cv
    
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
            if self._reaction_q.tag_exists(tag) or tag <= self._current_tag:
                return True
            result =  self.schedule_action_sync(Action("__empty__", self), tag)
            self._reaction_q_cv.notify()
            return result

    def schedule_action_async(self, action : Action, tag : Optional[Tag]=None) -> bool:
        with self._reaction_q_cv:
            if tag <= self._current_tag:
                self._logger.debug(f"Failed to schedule {action} at {tag}.")
                return False
            self.schedule_action_sync(action, tag)
            self._reaction_q_cv.notify()
        return True
    
    def schedule_loop_acquire_async(self, requested_tag : Tag, request_origin : str, loop_req: Tuple[Loop, int]) -> bool:
        with self._reaction_q_cv:
            if requested_tag <= self._current_tag:
                return False
            self._loop_acquiries.add(requested_tag, request_origin, loop_req)
            self._reaction_q_cv.notify()
        return True
    
    def schedule_loop_acquire_sync(self, requested_tag : Tag, request_origin : str, loop_req: Tuple[Loop, int]) -> bool:
        assert self._reaction_q_lock.locked()
        if requested_tag <= self._current_tag:
            return False
        self._loop_acquiries.add(requested_tag, request_origin, loop_req)
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

    def _get_next_tag(self) -> Tuple[Tag, Dict[str, List[Tuple[Loop, int]]]]:
        assert self._reaction_q_lock.locked()
        loop_acquire_tag = self._loop_acquiries.next_tag()
        reaction_q_tag = self._reaction_q.next_tag()
        if loop_acquire_tag is None or loop_acquire_tag > reaction_q_tag:
            return reaction_q_tag, []
        return loop_acquire_tag, self._loop_acquiries.get_request_origins(loop_acquire_tag)

    def _next_tag_available(self) -> bool:
        return self._reaction_q.next_tag() is not None or self._loop_acquiries.next_tag() is not None
    
    def _loop_dependencies_released(self, loop_acquire_origins : Set[str], tag : Tag) -> bool:
        for input in self._inputs:
            if not input.loop_dependencies_released(loop_acquire_origins, tag):
                return False
        return True

    def run(self):
        self._logger.debug(f"Thread id: {threading.current_thread().ident}.")
        for output in self._outputs:
            output.start_loop_discovery()
        # some ideas:
        # either: on loop_detected_message, send the current tag with it and stop the main loop until the message comes back with the same tag
                # if a federate received that message, it tries to stop at the same tag and relays the message with the tag it actually stopped at
                # if the tag was altered, repeat with higher tag
                # -> probably the better version for transient federates
        # or: acknowledge loop_discovery messages backwards on dead ends, such that the discovery phase can be finished completely before starting at all
        #time.sleep(2)
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
                loop_acquire_requests : Dict[str, List[Tuple[Loop, int]]]
                next_tag, loop_acquire_requests = self._get_next_tag()
                refetch_next_tag = False
                self.logger.debug(f"next tag is {next_tag}.")
                if loop_acquire_requests:
                    for input in self._inputs:
                        result : bool = input.loop_acquire_tag(next_tag, loop_acquire_requests.keys(), lambda: self._get_next_tag() != (next_tag, loop_acquire_requests))
                        if not result or self._get_next_tag() != (next_tag, loop_acquire_requests): # acquire_tag failed or new event
                            refetch_next_tag = True
                            self.logger.debug(f"refetching next tag.")
                            break
                    if refetch_next_tag:
                        continue

                    # we got a request from a loop, and need to acquire the other loops we are part of
                    if self.name in loop_acquire_requests:
                        self._ledger.forward_loop_acquire_requests(next_tag, {self.name: loop_acquire_requests[self.name]})
                        self._loop_acquiries.remove_request(next_tag, set([self.name]))
                        continue
                    self._ledger.forward_loop_acquire_requests(next_tag, loop_acquire_requests)
                    self._loop_acquiries.remove_request(next_tag, loop_acquire_requests.keys())
                    continue

                for input in self._inputs:
                    result : bool = input.acquire_tag(next_tag, lambda: self._get_next_tag() != (next_tag, loop_acquire_requests))
                    if not result or self._get_next_tag() != (next_tag, loop_acquire_requests): # acquire_tag failed or new event
                        refetch_next_tag = True
                        self.logger.debug(f"refetching next tag.")
                        break
                if refetch_next_tag:
                    continue 

                self._current_tag = next_tag
                actions = self._reaction_q.pop_tag(self._current_tag)
                triggered_actions = [reaction for action in actions if isinstance(action, TriggerAction) for reaction in self._get_triggered_reactions(action)]
                actions += triggered_actions
                if self._current_tag == self._stop_tag:
                    stop_running = True
                
            for action in actions:
                action.exec(self._current_tag)
            
            self._release_tag(self._current_tag)

            if stop_running:
                return

