import random
from typing import List

from reactor import Reactor, ReactionDeclaration, TimerDeclaration
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



if __name__ == "__main__":
    start_tag = Tag()
    stop_tag = start_tag.delay(secs_to_ns(10))
    reactors : List[Reactor] = [create_random_reactor(f"random_reactor{i}", start_tag, stop_tag) for i in range(1)]
    for reactor in reactors:
        print(reactor)
        reactor.run()