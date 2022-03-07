"""Sets up the worker graph by setting parameters for each worker, such as failure_rate and straggle_rate. Workers are currently equistant from each other for the topology."""

from .architecture import *


def seed_workers(num_mach: int, max_distance: int) -> WorkerGraph:
    init_workers = []
    for i in range(num_mach):
        worker = None
        if i < int(num_mach * 0.2):
            worker = Worker(worker_id=i, straggle_rate=0, failure_rate=0)
        elif i < int(num_mach * 0.4):
            worker = Worker(worker_id=i, straggle_rate=0.05, failure_rate=0)
        elif i < int(num_mach * 0.6):
            worker = Worker(worker_id=i, straggle_rate=0.1, failure_rate=0.05)
        else:
            worker = Worker(worker_id=i)
        init_workers.append(worker)
    return WorkerGraph(init_workers, max_distance)
