import random
from typing import List

from reactor import Reactor

def create_random_reactor(name : str) -> Reactor:
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
    return Reactor(name, inputs, outputs, reactions)

if __name__ == "__main__":
    reactors : List[Reactor] = [create_random_reactor(f"random_reactor{i}") for i in range(3)]
    for reactor in reactors:
        print(reactor)