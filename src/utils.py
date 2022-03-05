import networkx as nx
import matplotlib.pyplot as plt
import itertools
import numpy as np
from enum import Enum
from typing import List, Union, Tuple

from queue import Queue, PriorityQueue


class EventType(Enum):
    TERMINATE = 1
    FAIL = 2
    STRAGGLE = 3


class WorkerStatus(Enum):
    FREE = 1
    FAILED = 2
    BUSY = 3


class TaskStatus(Enum):
    COMPLETE = 1
    PENDING = 2
    UNASIGGNED = 3


class Task:
    def __init__(self, task_id: int = 0,
                 task_parameters={"task_type": "sort", "n_records": 1000},
                 task_status: TaskStatus = TaskStatus.PENDING,
                 task_dependencies=0) -> None:
        # This task_id becomes the node number when we construct the task graph
        self.task_id = task_id
        self.status = task_status
        self.task_dependencies = task_dependencies
        self.task_parameters = task_parameters

    def debug(self):
        print("task_id: ", self.task_id)
        print("status: ", self.status)
        print("dependencies: ", self.task_dependencies)
        print("parameters: ", self.task_parameters)


class Worker:
    def __init__(self, memory_size, network_bandwidth, disk_bandwidth, status: WorkerStatus, failure_rate, straggle_rate, task: Task = None) -> None:
        self.memory_size = memory_size
        self.network_bandwidth = network_bandwidth
        self.disk_bandwidth = disk_bandwidth
        self.status = status
        self.task = task
        self.failure_rate = failure_rate
        self.straggle_rate = straggle_rate

        self.bandwidth_status = {}  # {Worker: bandwidth_usage}
        self.current_bandwidth = 0

    def processing_time(self) -> List[Union[EventType, int]]:
        total_processing_time = 0

        total_processing_time += self.networking_time()
        total_processing_time += self.disk_time()

        task_parameters = self.task.task_parameters
        # print(task_parameters)
        # use task and worker parameters to figure out the procesing time for the worker
        if task_parameters["task_type"] == "sort":
            total_processing_time += task_parameters["n_records"] * np.log(
                task_parameters["n_records"])

        if task_parameters["task_type"] == "grep":
            total_processing_time += task_parameters["n_records"]

        return [EventType.TERMINATE, total_processing_time]

    def networking_time(self):
        # TODO: Have this function actually calculate networking time given its attributes
        return 5

    def disk_time(self):
        # TODO: Have this function actually calculate networking time given its attributes
        return 5


class Event:
    def __init__(self, time=0, event_type: EventType = None, worker: Worker = None) -> None:
        self.time = time
        self.event_type = event_type
        self.worker = worker

    def debug(self):
        print("Time: ", self.time)
        print("Event type: ", self.event_type)

    def __eq__(self, other):
        return self.time == other.time

    def __gt__(self, other):
        return self.time > other.time

    def __lt__(self, other):
        return self.time < other.time


class TaskGraph(nx.DiGraph):
    subset_color = [
        "gold",
        "violet",
        "limegreen",
        "darkorange",
    ]

    def __init__(self, *task_topology: Tuple[int]):
        super().__init__()
        extents = nx.utils.pairwise(itertools.accumulate((0,) + task_topology))
        print(extents)
        layers = [range(start, end) for start, end in extents]
        print(layers)
        print(self)
        for (i, layer) in enumerate(layers):
            # list of Task objects
            task_list = [dict(task=Task(task_id=t_id)) for t_id in layer]
            node_list = list(zip(list(layer), task_list))
            print(node_list)
            self.add_nodes_from(node_list, layer=i)
        for layer1, layer2 in nx.utils.pairwise(layers):
            self.add_edges_from(itertools.product(layer1, layer2))

    def debug(self):
        print("debugging graph\n")
        for node in self.nodes:
            self.nodes[node]["task"].debug()

    def print_graph(self):
        color = [self.subset_color[data["layer"]]
                 for v, data in self.nodes(data=True)]
        pos = nx.multipartite_layout(self, subset_key="layer")
        plt.figure(figsize=(8, 8))
        nx.draw(self, pos, node_color=color, with_labels=False)
        plt.axis("equal")
        plt.show()


class Scheduler:

    def __init__(self, task_graph: TaskGraph, Workers: List[Worker]) -> None:
        self.G = task_graph  # Graph of tasks
        self.Workers = Workers  # List of workers

    def simulate(self) -> None:
        # choose a first task from the head of the queue
        # task is ready
        # hashmap of workers which stores the busy/free workers
        # assign task to the worker
        # create an event, some time units for the task insert the event in priority queue

        current_time = 0
        task_queue = Queue()  # task queue as a topologically sorted task graph
        event_queue = PriorityQueue()  # priority queue for events
        free_worker_list = self.Workers  # list to store the free workers

        print(self.G.nodes)
        # initiliaze the task dependency numbers
        for node_number in self.G.nodes:
            # TODO: add task_dependencies to TaskGraph()
            node = self.G.nodes[node_number]
            node["task"].task_dependencies = self.G.in_degree(node_number)
            if (node["task"].task_dependencies == 0):
                # add the ready tasks to the queue
                task_queue.put(node["task"])

        self.G.debug()

        print("starting to work through task_queue.\n")
        while not task_queue.empty():  # iterate through the tasks that are ready to execute
            task = task_queue.get()  # get the first task from the queue
            if len(free_worker_list) == 0:
                break
            worker = free_worker_list.pop()  # remove a worker from the free_list
            worker.task = task  # assign the task to the worker
            worker.status = WorkerStatus.BUSY
            # get the approximate processing time and the probabilistic fate of the worker
            event_type, task_process_time = worker.processing_time()
            # create an event with the above parameters
            event = Event(current_time + task_process_time, event_type, worker)
            event_queue.put(event)  # add the event to the event queue

        last_event = None
        print("starting to work through event_queue.\n")
        while not event_queue.empty():
            # print("processing an event")
            event = event_queue.get()
            last_event = event
            # print(event)
            # print("processed an event")
            if (event.event_type == EventType.TERMINATE):
                #print("in here")
                worker = event.worker
                worker.task.status = TaskStatus.COMPLETE
                # get the out-edges of a task
                for k in (self.G[worker.task.task_id].keys()):
                    # decrement the dependecies of all out_going edges
                    self.G.nodes[k]["task"].task_dependencies -= 1
                    # if dependencies of a task are 0, it is ready to be added into the task queue
                    if (self.G.nodes[k]["task"].task_dependencies == 0):
                        task_queue.put(self.G.nodes[k]["task"])
                # print("got out of for loop")
                worker.task = None  # remove the task from the worker
                worker.status = WorkerStatus.FREE  # mark the status of the worker to free
                # add the worker to the free list
                free_worker_list.append(worker)

        print(f"Last event time: {last_event.time}")
        return None
