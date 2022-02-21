## Populating the Computation Graph

- n records
- start with an equidistant graph

1. know the type of machines, small algorithm to determine how many records are given to each machine
2. everything computed is written to a disk
3. decide how many reducers we want 
4. write final output to a machine

## Events

1. has an associated time
2. has an associated state
- completed execution
- failed execution
- straggling

## Task vs. Event Queue
- event queue specifies the tasks for a specific stage of computation, can use this as a global synchronizer
- task queue is directly correlated to the computation execution graph
- scheduler touches both between task queue and computation graph, as well as coordinating between event and task queue

## Time
- will jump and skip over steps where no computations/events are occuring
