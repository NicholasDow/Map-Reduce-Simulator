"""Sets up the worker graph by setting parameters for each worker, such as failure_rate and straggle_rate. Workers are currently equistant from each other for the topology."""

from .architecture import *


def seed_worker_list(num_mach: int, max_port: int):
    init_workers = []
    for i in range(num_mach):
        worker = None
        if i < int(num_mach * 0.2):
            worker = Worker(is_switch=False, worker_id=i,
                            max_port_connections=max_port,
                            straggle_rate=0, failure_rate=0)
        elif i < int(num_mach * 0.4):
            worker = Worker(is_switch=False, worker_id=i,
                            max_port_connections=max_port,
                            straggle_rate=0.05, failure_rate=0)
        elif i < int(num_mach * 0.6):
            worker = Worker(is_switch=False, worker_id=i,
                            max_port_connections=max_port,
                            straggle_rate=0.1, failure_rate=0.05)
        else:
            worker = Worker(is_switch=False, worker_id=i,
                            max_port_connections=max_port)
        init_workers.append(worker)
    return init_workers


def seed_workers(num_mach: int, max_distance: int) -> WorkerGraph:
    init_workers = seed_worker_list(num_mach=num_mach, max_port=50)
    return WorkerGraph(init_workers, max_distance, seed_network=True)


def seed_worker_network(num_mach: int,
                        link_bandwidth: int,
                        machines_per_switch: int,
                        max_port_connections: int) -> WorkerNetwork:
    init_workers = seed_worker_list(num_mach=num_mach,
                                    max_port=max_port_connections)
    return WorkerNetwork(workers=init_workers,
                         workers_per_switch=machines_per_switch,
                         link_bandwidth=link_bandwidth,
                         max_port=max_port_connections)
