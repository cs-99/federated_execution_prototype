from fileinput import filename
import random
from typing import List, Optional
from threading import Thread
import logging
import sys

from reactor import Reactor, ReactionDeclaration, TimerDeclaration
from pubsub import comms
from tag import Tag
from utility import secs_to_ns

def create_random_input_declaration(min = 0, max = 10) -> List[str]:
    return [f"input{i}" for i in range(random.randint(min, max))]

def create_random_output_declaration(min = 0, max = 10) -> List[str]:
    return [f"output{i}" for i in range(random.randint(min, max))]

def create_random_timer_declaration(min=0, max=10, min_offset=0, max_offset=secs_to_ns(100), min_interval=1, max_interval=secs_to_ns(100)) -> List[str]:
    assert min_interval >= 1
    return [TimerDeclaration( f"timer{i}", random.randint(min_offset, max_offset), random.randint(min_interval, max_interval))  for i in range(random.randint(min, max))]

def create_random_reaction_declaration(input_decl : List[str], output_decl : List[str], timer_decl : List[TimerDeclaration], min = 0, max = 10) -> List[ReactionDeclaration]:
    timer_names = list(map(lambda t: t.name, timer_decl))
    reactions : List[ReactionDeclaration] = []
    for i in range(random.randint(min, max)):
        # at least one trigger required
        triggers = [random.choice(input_decl + timer_names)] 
        # optionally more triggers
        for trigger in input_decl + timer_names:
            if trigger not in triggers and random.choice([True, False]):
                triggers.append(trigger)

        effects = [output for output in output_decl if random.choice([True, False])]
        reactions.append(ReactionDeclaration(f"reaction{i}",triggers, effects))
    return reactions

def create_random_reactor(name : str, start_tag : Tag, stop_tag : Tag) -> Reactor:
    # at least one input required to have a reaction
    inputs = create_random_input_declaration(1, 5)
    outputs = create_random_output_declaration(1, 5)
    timers = create_random_timer_declaration(min=2)
    reactions = create_random_reaction_declaration(inputs, outputs, timers, 1, 5)
    return Reactor(name, start_tag, stop_tag, inputs, outputs, timers, reactions)

def run_reactors(reactors : List[Reactor]):
    threads : List[Thread] = []
    # Note: this thread is handling the callbacks to subscribers of topics
    # this is basically what every node in ROS2 would do with rclcpp::spin(), but combined in one thread
    comms_thread = Thread(target=comms.run_all_subscriber_callbacks)
    comms_thread.start()
    for reactor in reactors:
        threads.append(Thread(target=reactor.run))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    comms.stop_running_subscriber_callbacks()

def connect(reactor_out : Reactor, output_name : str, reactor_in : Reactor, input_name : str, delay : Optional[int] = None) -> None:
    reactor_out.get_output(output_name).connect(reactor_in.name, input_name)
    reactor_in.get_input(input_name).connect(reactor_out.name, delay)

def create_pub_sub_reactors(start_tag : Tag, stop_tag : Tag, message_every_secs : float = 0.5) -> List[Reactor]:
    reactors = []
    reactors.append(Reactor("pub", start_tag, stop_tag, 
                        [], 
                        ["out"], 
                        [TimerDeclaration("timer", secs_to_ns(0), secs_to_ns(message_every_secs))],
                        [ReactionDeclaration("on_timer", ["timer"], ["out"])]
                        ))
    reactors.append(Reactor("sub", start_tag, stop_tag, 
                        ["in"], 
                        [], 
                        [],
                        [ReactionDeclaration("on_in", ["in"], [])]
                        ))
    connect(reactors[0], "out", reactors[1], "in")
    return reactors

def create_cycle_reactors(start_tag : Tag, stop_tag : Tag, message_every_secs : float = 0.5, delay : float = 0.01) -> List[Reactor]:
    reactors = []
    reactors.append(Reactor("pub", start_tag, stop_tag, 
                        ["in"], 
                        ["out"], 
                        [TimerDeclaration("timer", secs_to_ns(0), secs_to_ns(message_every_secs))],
                        [ReactionDeclaration("on_timer", ["timer"], ["out"]), ReactionDeclaration("on_in", ["in"], [])]
                        ))
    reactors.append(Reactor("sub", start_tag, stop_tag, 
                        ["in"], 
                        ["out"], 
                        [],
                        [ReactionDeclaration("on_in", ["in"], ["out"])]
                        ))
    connect(reactors[0], "out", reactors[1], "in")
    connect(reactors[1], "out", reactors[0], "in", secs_to_ns(delay))
    return reactors

def create_double_cycle_reactors(start_tag : Tag, stop_tag : Tag, 
        message_every_secs_1 : float = 0.5, delay_1 : float = 0.1,
        message_every_secs_2 : float = 0.5, delay_2 : float = 0.1) -> List[Reactor]:
    reactors = []
    reactors.append(Reactor("a", start_tag, stop_tag, 
                        ["in"], 
                        ["out"], 
                        [TimerDeclaration("timer", secs_to_ns(0), secs_to_ns(message_every_secs_1))],
                        [ReactionDeclaration("on_timer", ["timer"], ["out"]), ReactionDeclaration("on_in", ["in"], [])]
                        ))
    reactors.append(Reactor("b", start_tag, stop_tag, 
                        ["in_c", "in_a"], 
                        ["out_c", "out_a"], 
                        [],
                        [ReactionDeclaration("on_in_a", ["in_a"], ["out_a"]),
                        ReactionDeclaration("on_in_c", ["in_c"], ["out_c"])]
                        ))
    reactors.append(Reactor("c", start_tag, stop_tag, 
                        ["in"], 
                        ["out"], 
                        [TimerDeclaration("timer", secs_to_ns(0), secs_to_ns(message_every_secs_2))],
                        [ReactionDeclaration("on_timer", ["timer"], ["out"]), ReactionDeclaration("on_in", ["in"], [])]
                        ))
    connect(reactors[0], "out", reactors[1], "in_a")
    connect(reactors[1], "out_a", reactors[0], "in", secs_to_ns(delay_1))
    connect(reactors[2], "out", reactors[1], "in_c")
    connect(reactors[1], "out_c", reactors[2], "in", secs_to_ns(delay_2))
    return reactors


def create_deep_loop(start_tag : Tag, stop_tag : Tag, message_every_secs : float = 0.5):
    reactors = []
    reactors.append(Reactor("a", start_tag, stop_tag, ["in"], ["out"],
                    [TimerDeclaration("t", 0, secs_to_ns(message_every_secs))],
                    [ReactionDeclaration("r", ["t"], ["out"])],
                    ))
    reactors.append(Reactor("b", start_tag, stop_tag, ["in"], ["out"],
                    [],
                    [ReactionDeclaration("r", ["in"], ["out"])],
                    ))
    reactors.append(Reactor("c", start_tag, stop_tag, ["in"], ["out"],
                    [],
                    [ReactionDeclaration("r", ["in"], ["out"])],
                    ))
    reactors.append(Reactor("d", start_tag, stop_tag, ["in"], ["out"],
                    [],
                    [ReactionDeclaration("r", ["in"], ["out"])],
                    ))
    connect(reactors[0], "out", reactors[1], "in")
    connect(reactors[1], "out", reactors[2], "in")
    connect(reactors[2], "out", reactors[3], "in")
    connect(reactors[3], "out", reactors[0], "in", secs_to_ns(0.3))
    return reactors


if __name__ == "__main__":
    log_handlers : List[logging.Handler] = [
        logging.FileHandler("output.log", encoding="ascii", mode="w"),
        logging.StreamHandler(sys.stdout)
    ]
    # log level INFO logs only executed actions
    # log level DEBUG logs some coordination too
    logging.basicConfig(handlers=log_handlers, level=logging.INFO)

    # start at tag 1000 sec for better readability
    start_tag = Tag(secs_to_ns(1000)) 
    stop_tag = start_tag.delay(secs_to_ns(5))
    #reactors : List[Reactor] = [create_random_reactor(f"random_reactor{i}", start_tag, stop_tag) for i in range(1)]
    #reactors = create_pub_sub_reactors(start_tag, stop_tag)
    reactors = create_cycle_reactors(start_tag, stop_tag)
    #reactors = create_double_cycle_reactors(start_tag, stop_tag, delay_1=0.01)
    # reactors = create_deep_loop(start_tag, stop_tag)
    for reactor in reactors:
        logging.info(reactor)
    try:
        run_reactors(reactors)
    except Exception as e: # this ensures everything gets logged when interrupting the process
        logging.error(e)
        exit()