import random
from typing import List
from threading import Thread

from reactor import Reactor, ReactionDeclaration, TimerDeclaration, CommunicationBus
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
    for reactor in reactors:
        thread = Thread(target=reactor.run)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

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
    CommunicationBus().add_connection(reactors[0].get_output("out"), [reactors[1].get_input("in")])
    return reactors

def create_cycle_reactors(start_tag : Tag, stop_tag : Tag, message_every_secs : float = 0.5) -> List[Reactor]:
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
    CommunicationBus().add_connection(reactors[0].get_output("out"), [reactors[1].get_input("in")], delay=secs_to_ns(0.1))
    CommunicationBus().add_connection(reactors[1].get_output("out"), [reactors[0].get_input("in")])
    return reactors

if __name__ == "__main__":
    start_tag = Tag()
    stop_tag = start_tag.delay(secs_to_ns(10))
    #reactors : List[Reactor] = [create_random_reactor(f"random_reactor{i}", start_tag, stop_tag) for i in range(1)]
    #reactors = create_pub_sub_reactors(start_tag, stop_tag)
    reactors = create_cycle_reactors(start_tag, stop_tag)
    run_reactors(reactors)