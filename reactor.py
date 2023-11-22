"""
This is a prototype for the decentralized coordination contract used for federated (and enclaves) execution of Lingua Franca
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, List, Optional
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
class Input(Port, TriggerAction):
    def __init__(self, name, reactor  : Reactor):
        Port.__init__(self, name, reactor)
        TriggerAction.__init__(self, name, reactor)
        self._released_tag : Tag = Tag(0)
        self._is_connected : bool = False
        self._tag_req_pub : Optional[Publisher] = None
        self._tag_rel_sub : Optional[Subscriber] = None
        self._message_sub : Optional[Subscriber] = None
        self._delay : Optional[int] = None

    def connect(self, topic_name : str, delay : Optional[int] = None):
        if self._is_connected:
            raise ValueError(f"{self._reactor}/{self.name} is already connected.")
        self._tag_rel_sub = Subscriber(topic_name + TAG_RELEASE_TOPIC_SUFFIX, self._update_released_tag)
        self._message_sub = Subscriber(topic_name, self._receive_message)
        self._tag_req_pub = Publisher(topic_name + TAG_REQUEST_TOPIC_SUFFIX)
        self._is_connected = True
        self._delay = delay

    def _update_released_tag(self, tag : Tag):
        if tag > self._released_tag:
            self._released_tag = tag
            self._reactor.logger.debug(repr(self) + " released " + repr(tag))
            self._reactor.notify()

    def _receive_message(self, tag : Tag):
        if self._delay is not None:
            self._reactor.schedule_action_async(self, tag.delay(self._delay))
        else:
            self._reactor.schedule_action_async(self, tag)
        self._update_released_tag(tag)

    def acquire_tag(self, tag : Tag, predicate : Callable[[None], bool] = lambda: False) -> bool:
        tag_to_acquire = tag
        if self._delay is not None:
            tag_to_acquire = tag.subtract(self._delay)
        self._reactor.logger.debug(f"{self.name} acquiring {tag_to_acquire}.")
        if tag_to_acquire <= self._released_tag or not self._is_connected:
            return True
        self._tag_req_pub.publish(tag_to_acquire) # schedule empty event at sender
        self._reactor.logger.debug(f"{self.name} waiting for tag release {tag_to_acquire}.")
        return self._reactor.wait_for(lambda: tag_to_acquire <= self._released_tag or predicate())

class Output(Port):
    def __init__(self, name, reactor : Reactor):
        super().__init__(name, reactor)
        self._is_connected : bool = False
        self._tag_rel_sub : Optional[Subscriber] = None
        self._tag_only_pub : Optional[Publisher] = None
        self._message_pub : Optional[Publisher] = None
        reactor.register_release_tag_callback(self._send_tag_only)
    
    def _send_tag_only(self, tag):
        if self._is_connected:
            self._tag_rel_pub.publish(tag)

    def set(self, tag : Tag):
        if self._is_connected:
            self._message_pub.publish(tag)

    def connect(self, topic_name):
        self._tag_rel_pub = Publisher(topic_name + TAG_RELEASE_TOPIC_SUFFIX)
        self._tag_req_sub = Subscriber(topic_name + TAG_REQUEST_TOPIC_SUFFIX, self._process_tag_request)
        self._message_pub = Publisher(topic_name)
        self._is_connected = True
    
    def _process_tag_request(self, tag : Tag):
        # received a message, therefore must be connected at this point
        if not self._reactor.schedule_empty_async_at(tag): # scheduling failed -> we are at a later tag already
            self._tag_rel_pub.publish(self._reactor.current_tag)


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

    def pop_tag(self, tag: Tag) -> List[Action]:
        assert len(self._list) > 0
        actions = list(filter(lambda e: e.tag == self.next_tag(), self._list))[0].actions
        self._list = list(filter(lambda e: e.tag != self.next_tag(), self._list))
        return actions
    
    def next_tag(self) -> Tag:
        assert len(self._list) > 0
        smallest_tag : Tag = self._list[0].tag
        for entry in self._list:
            if entry.tag < smallest_tag:
                smallest_tag = entry.tag 
        return smallest_tag
        
    def __bool__(self) -> bool:
        return bool(self._list)

    def __repr__(self) -> str:
        r = ""
        for entry in self._list:
            r += repr(entry)
        return r

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
        self._reaction_q.add(self._start_tag, Action("__start__", self))
        self._reaction_q.add(self._stop_tag, Action("__stop__", self))
        self._reaction_q_lock : threading.Lock = threading.Lock()
        self._reaction_q_cv : threading.Condition = threading.Condition(self._reaction_q_lock)

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

    def _release_current_tag(self):
        self.logger.debug(f"releasing {self._current_tag}.")
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
    
    # this method assumes that self._reaction_q_cv is held
    def schedule_action_sync(self, action : Action, tag : Optional[Tag] = None) -> bool:
        assert tag > self._current_tag
        assert tag < self._stop_tag
        assert self._reaction_q_lock.locked()
        if tag is None:
            tag = self._current_tag.delay()
        self._reaction_q.add(tag, action)

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

    def _get_triggered_reactions(self, action : TriggerAction):
        triggered_reactions : List[Reaction] = []
        for reaction in self._reactions:
            if action in reaction.triggers:
                triggered_reactions.append(reaction)
        return triggered_reactions  

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
        stop_running : bool = False
        while True:
            actions = []
            with self._reaction_q_cv:
                if not self._reaction_q:
                    self.wait_for(lambda: bool(self._reaction_q))

                next_tag : Tag = self._reaction_q.next_tag()
                self.logger.debug(f"next tag is {next_tag}.")
                refetch_next_tag : bool = False
                for input in self._inputs:
                    result : bool = input.acquire_tag(next_tag, lambda: self._reaction_q.next_tag() != next_tag)
                    if not result or self._reaction_q.next_tag() != next_tag: # acquire_tag failed or actionlist modified
                        refetch_next_tag = True
                        self.logger.debug(f"refetching next tag.")
                        break
                if refetch_next_tag:
                    continue

                self._current_tag = next_tag
                actions = self._reaction_q.pop_tag(self._current_tag)

                triggered_actions = []
                for action in actions:
                    if isinstance(action, TriggerAction):
                        triggered_actions += self._get_triggered_reactions(action)
                actions += triggered_actions
                if self._current_tag == self._stop_tag:
                    stop_running = True
                self._release_current_tag()
            for action in actions:
                action.exec(self._current_tag)
            
            if stop_running:
                return



    

