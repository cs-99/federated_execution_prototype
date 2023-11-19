import random
from typing import List

from reactor import Reactor
from tag import Tag
from utility import secs_to_ns

def create_random_reactor(name : str, start_tag : Tag, stop_tag : Tag) -> Reactor:
    # at least one input required to have a reaction
    inputs = [f"input{i}" for i in range(random.randint(1, 10))] 
    outputs = [f"output{i}" for i in range(random.randint(0, 10))]
    reactions = []
    for i in range(random.randint(0, 10)):
        # at least one trigger required
        triggers = [random.choice(inputs)] 
        # optionally more triggers
        for input in inputs:
            if input not in triggers and random.choice([True, False]):
                triggers.append(input)

        effects = [output for output in outputs if random.choice([True, False])]
        reactions.append((f"reaction{i}",triggers, effects))
    return Reactor(name, start_tag, stop_tag, inputs, outputs, reactions)

if __name__ == "__main__":
    start_tag = Tag()
    stop_tag = start_tag.delay(secs_to_ns(10))
    reactors : List[Reactor] = [create_random_reactor(f"random_reactor{i}", start_tag, stop_tag) for i in range(3)]
    for reactor in reactors:
        print(reactor)