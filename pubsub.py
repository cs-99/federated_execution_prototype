from __future__ import annotations
from queue import Queue, Empty # queue is thread safe by default
from typing import Callable, List, Any, Optional, Union
import threading
import logging

import utility

# Note technically List.append is thread safe due to GIL already but who knows maybe some python impl does not have a GIL

# singleton that manages all topics
class CommunicationBus(metaclass=utility.Singleton):
    def __init__(self):
        self._topics_lock : threading.Lock = threading.Lock()
        self._topics : List[Topic] = []

        # stuff to signal stopping the subscriber callback thread
        self._run_subscriber_cbs_lock : threading.Lock = threading.Lock()
        self._stop_subscriber_cbs : bool = False

    def _find_topic(self, topic_name) -> Optional[Topic]:
        with self._topics_lock:
            for topic in self._topics:
                if topic.name == topic_name:
                    return topic
            return None

    def _get_topic(self, topic_name) -> Topic:
        topic = self._find_topic(topic_name)
        if topic is not None:
            return topic
        new_topic = Topic(topic_name)
        with self._topics_lock:
            self._topics.append(new_topic)
        return new_topic

    def register(self, topic_name : str, member : Union[Publisher, Subscriber]) -> Topic:
        topic : Topic = self._get_topic(topic_name)
        topic.register(member)
        return topic

    def publish(self, topic_name, message):
        topic = self._find_topic(topic_name)
        if topic is None:
            raise ValueError(f"Topic {topic_name} does not exist.")
        topic.publish(message)

    def run_all_subscriber_callbacks(self):
        with self._run_subscriber_cbs_lock:
            self._stop_subscriber_cbs = False
        while True:
            current_topics = []
            with self._topics_lock:
                current_topics = self._topics.copy()
            for topic in current_topics:
                topic.run_sub_callbacks()
            with self._run_subscriber_cbs_lock:
                if self._stop_subscriber_cbs:
                    return

    def stop_running_subscriber_callbacks(self):
        with self._run_subscriber_cbs_lock:
            self._stop_subscriber_cbs = True
        
comms = CommunicationBus()

class RegisteredOnTopic:
    def __init__(self, topic_name : str) -> None:
        logging.debug(topic_name)
        self._topic = comms.register(topic_name, self)
        self._is_registered = True

    def unregister(self):
        assert self._is_registered
        self._topic.unregister(self)
        self._is_registered = False
    

class Publisher(RegisteredOnTopic):
    def __init__(self, topic_name : str) -> None:
        super().__init__(topic_name)

    def publish(self, message : Any) -> None:
        assert self._is_registered
        self._topic.publish(message)

class Subscriber(RegisteredOnTopic):
    def __init__(self, topic_name : str, callback : Callable[[Any], None]) -> None:
        # Queue is thread safe by default
        self._queue : Queue = Queue()
        assert isinstance(callback, Callable)
        self._callback = callback
        super().__init__(topic_name)

    def insert_message(self, message : Any) -> None:
        assert self._is_registered
        self._queue.put(message)

    def process_next_message(self):
        assert self._is_registered
        try:
            self._callback(self._queue.get_nowait())
        except Empty:
            pass

class Topic:
    def __init__(self, name : str):
        self._name : str = name
        self._publishers : List[Publisher] = []
        self._subscribers : List[Subscriber] = []
        self._topic_lock : threading.Lock = threading.Lock()

    @property
    def name(self):
        return self._name

    def unregister(self, member : Union[Publisher, Subscriber]):
        with self._topic_lock:
            if isinstance(member, Publisher):
                self._publishers.remove(member) 
            elif isinstance(member, Subscriber):
                self._subscribers.remove(member) 
            else:
                raise ValueError("Topic member must be Publisher or Subscriber.")

    def register(self, member : Union[Publisher, Subscriber]):
        with self._topic_lock:
            if isinstance(member, Publisher):
                self._publishers.append(member) 
            elif isinstance(member, Subscriber):
                self._subscribers.append(member) 
            else:
                raise ValueError("Topic member must be Publisher or Subscriber.")
    
    def publish(self, message : Any):
        with self._topic_lock:
            for sub in self._subscribers:
                sub.insert_message(message)

    def run_sub_callbacks(self):
        subs = []
        with self._topic_lock:
            subs = self._subscribers.copy()
        for sub in subs:
            sub.process_next_message()
