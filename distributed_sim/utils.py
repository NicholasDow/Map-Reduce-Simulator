from .architecture import *


def seed_workers(num_mach: int) -> WorkerGraph:
    init_workers = []
    for i in range(num_mach):
        worker = None
        if i < int(num_mach * 0.2):
            worker = Worker(straggle_rate=0, failure_rate=0)
        elif i < int(num_mach * 0.4):
            worker = Worker(straggle_rate=0.05, failure_rate=0)
        elif i < int(num_mach * 0.6):
            worker = Worker(straggle_rate=0.1, failure_rate=0.05)
        else:
            worker = Worker()
        init_workers.append(worker)
    return WorkerGraph(init_workers)
