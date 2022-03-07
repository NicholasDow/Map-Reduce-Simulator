import networkx as nx
import matplotlib.pyplot as plt
import itertools
import numpy as np
from typing import List, Union, Tuple
from enum import Enum, auto
from queue import Queue, PriorityQueue

from .types import *


class Task:
    def __init__(self, parent_prog: Union[MReduceProg, DaskProg],
                 task_id: int = 0,
                 task_op: Union[CommonOp, MReduceOp, DaskOp] = MReduceOp.map,
                 n_records: int = 1000,
                 task_status: TaskStatus = TaskStatus.UNASSIGNED,
                 task_dependencies=0) -> None:
        # This task_id becomes the node number when we construct the task graph
        self.n_records = n_records
        self.task_id = task_id
        self.task_op = task_op
        self.status = task_status
        self.prog = parent_prog
        self.task_dependencies = task_dependencies
        self.task_parameters = {
            "task_type": self.task_op.name,
            "n_records": self.n_records,
        }

    def debug(self):
        print("task_id: ", self.task_id)
        print("status: ", self.status)
        print("dependencies: ", self.task_dependencies)
        print("parameters: ", self.task_parameters)


class Worker:
    def __init__(self,
                 network_bandwidth: int = 100,
                 disk_bandwidth: int = 50,
                 failure_rate: int = 0.1,
                 straggle_rate: int = 0.4,
                 task: Task = None,
                 status: WorkerStatus = WorkerStatus.FREE) -> None:
        # assume infinite memory size
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

        task_op = self.task.task_op
        n_rec = self.task.n_records
        # use task and worker parameters to figure out the procesing time for the worker
        if task_op == MReduceOp.sort:
            total_processing_time += n_rec * np.log(n_rec)
        if task_op == MReduceOp.reduce:
            total_processing_time += n_rec

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


class Program:
    task_topology = []
    # hashmap of tasks that stores (dependent task, dependent data amount)
    task_dependency_infos = {}
    program_type = None  # "sort", "grep", or etc.
    system_type = None  # "mapreduce", "dask", or etc.

    def __init__(self, program_type: Union[MReduceProg, DaskProg],
                 system_type: SystemOptions = SystemOptions.mapreduce,
                 task_topology=[], **kwargs):
        self.program_type = program_type
        self.system_type = system_type
        self.task_topology = task_topology


class TaskLayerChoices(Enum):
    first_layer = auto()
    one_to_one = auto()
    fully_connected = auto()


class TaskGraph(nx.DiGraph):
    subset_color = [
        "gold",
        "violet",
        "limegreen",
        "darkorange",
    ]

    def __init__(self):
        super().__init__()
        self.layer_count = 0
        self.prev_range = None
        self.range = None

    def add_layer(self,
                  task_list: List[Task],
                  option: TaskLayerChoices,
                  starting_idx: int):
        self.range = range(starting_idx, starting_idx + len(task_list))
        node_list = zip(list(self.range), task_list)
        self.add_nodes_from(node_list, layer=self.layer_count)
        self.layer_count += 1
        # need some way to specify which edges
        if option == TaskLayerChoices.one_to_one:
            self.add_edges_from(zip(list(self.prev_range), list(self.range)))
        elif option == TaskLayerChoices.fully_connected:
            self.add_edges_from(itertools.product(self.prev_range, self.range))
        self.prev_range = self.range
        self.range = None

    def __init__(self, program):
        super().__init__()

        if program.system_type == SystemOptions.mapreduce:
            # task_topology = (M, R)
            M, R = program.task_topology
            layers = [range(M), range(M, M+R)]

            for (i, layer) in enumerate(layers):
                # list of Task objects
                if i == 0:  # Map
                    task_list = [dict(task=Task(task_id=t_id))
                                 for t_id in layer]
                else:  # Reduce
                    task_list = [dict(task=Task(task_id=t_id))
                                 for t_id in layer]
                node_list = list(zip(list(layer), task_list))
                print(node_list)
                self.add_nodes_from(node_list, layer=i)
            for layer1, layer2 in nx.utils.pairwise(layers):
                self.add_edges_from(itertools.product(layer1, layer2))
        else:
            extents = nx.utils.pairwise(
                itertools.accumulate((0,) + program.task_topology))
            print(extents)
            layers = [range(start, end) for start, end in extents]
            print(layers)
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


class WorkerGraph(nx.DiGraph):
    workers = []

    def __init__(self, workers):
        self.workers = workers
        self.free_workers = workers
        self.seed_network_topology()

    def seed_network_topology(self):
        # add all combinational pairs
        idxs = range(len(self.workers))
        self.add_nodes_from(idxs)
        self.add_weighted_edges_from(
            [x + (0,) for x in itertools.combinations(idxs, 2)], 'bandwidth')

    def empty(self) -> bool:
        return len(self.free_workers) == 0

    def pop(self):
        return self.free_workers.pop()

    def append(self):
        return self.free_workers.append()

    def print_graph(self):
        labels = nx.get_edge_attributes(self, 'bandwidth')
        pos = nx.spring_layout(self)
        nx.draw(self, pos, with_labels=True)
        nx.draw_networkx_edge_labels(self, pos, edge_labels=labels)

    def get_in_bandwidth(self, worker):
        worker_idx = self.workers.index(worker)
        return self.in_degree(worker_idx, weight='bandwidth')

    def get_out_bandwidth(self, worker):
        worker_idx = self.workers.index(worker)
        return self.out_degree(worker_idx, weight='bandwidth')

    def get_current_bandwidth(self, worker):
        return self.get_in_bandwidth(worker) + self.get_out_bandwidth(worker)

    def request(self, src_worker, dest_worker):
        src_worker_idx, dest_worker_idx = self.workers.index(
            src_worker), self.workers.index(dest_worker)
        # start with the easiest one
        # request as much as both source and dest can afford at the same time
        bandwidth = min(src_worker.network_bandwidth - self.get_current_bandwidth(src_worker),
                        dest_worker.network_bandwidth - self.get_current_bandwidth(dest_worker))

        # update bandwidth-weighted edges
        self.add_edge(src_worker_idx, dest_worker_idx, bandwidth=bandwidth)
        print(self[src_worker_idx][dest_worker_idx]['bandwidth'])

        return bandwidth


class Scheduler:

    def __init__(self, task_graph: TaskGraph, workers: WorkerGraph) -> None:
        self.g = task_graph  # Graph of tasks
        self.workers = workers  # List of workers

    def simulate(self) -> None:
        # choose a first task from the head of the queue
        # task is ready
        # hashmap of workers which stores the busy/free workers
        # assign task to the worker
        # create an event, some time units for the task insert the event in priority queue

        current_time = 0
        task_queue = Queue()  # task queue as a topologically sorted task graph
        event_queue = PriorityQueue()  # priority queue for events
        free_worker_list = self.workers  # list to store the free workers

        print(self.g.nodes)
        # initiliaze the task dependency numbers
        for node_number in self.g.nodes:
            # TODO: add task_dependencies to TaskGraph()
            node = self.g.nodes[node_number]
            node["task"].task_dependencies = self.g.in_degree(node_number)
            if (node["task"].task_dependencies == 0):
                # add the ready tasks to the queue
                task_queue.put(node["task"])

        self.g.debug()

        print("starting to work through task_queue.\n")
        while not task_queue.empty():  # iterate through the tasks that are ready to execute
            task = task_queue.get()  # get the first task from the queue
            if free_worker_list.empty():
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
                # print("in here")
                worker = event.worker
                worker.task.status = TaskStatus.COMPLETE
                # get the out-edges of a task
                for k in (self.g[worker.task.task_id].keys()):
                    # decrement the dependecies of all out_going edges
                    self.g.nodes[k]["task"].task_dependencies -= 1
                    # if dependencies of a task are 0, it is ready to be added into the task queue
                    if (self.g.nodes[k]["task"].task_dependencies == 0):
                        task_queue.put(self.g.nodes[k]["task"])
                # print("got out of for loop")
                worker.task = None  # remove the task from the worker
                worker.status = WorkerStatus.FREE  # mark the status of the worker to free
                # add the worker to the free list
                free_worker_list.append(worker)

        print(f"Last event time: {last_event.time}")
        return None
