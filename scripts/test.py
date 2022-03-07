
from distributed_sim.utils import *


def main():
    print("--------------Grep task--------------")
    M, R = 5, 1
    task_topology = [M, R]
    program = Program(program_type="grep", system_type="mapreduce",
                      task_topology=task_topology)
    taskG = TaskGraph(program)
    # taskG.print_graph()
    grep_task = Task(task_parameters={"task_type": "grep", "n_records": 10})
    worker = Worker(memory_size=100, network_bandwidth=10, disk_bandwidth=10,
                    status=WorkerStatus.FREE, failure_rate=0.01, straggle_rate=0.01, task=grep_task)
    print(worker.processing_time())
    # Processing time works correctly after changing task assigned to worker!

    # Sort task: (M=15000, R=4000 in the paper, section 5.3)
    print("--------------Sort task--------------")
    M, R = 5, 2
    task_topology = [M, R]
    program = Program(program_type="sort", system_type="mapreduce",
                      task_topology=task_topology)
    taskG = TaskGraph(program)

    sort_task = Task(task_parameters={"task_type": "sort", "n_records": 10})

    worker = Worker(memory_size=100, network_bandwidth=10, disk_bandwidth=10,
                    status=WorkerStatus.FREE, failure_rate=0.01, straggle_rate=0.01, task=sort_task)
    print(worker.processing_time())

    sample_workers = [
        Worker(memory_size=100, network_bandwidth=10, disk_bandwidth=10,
               status=WorkerStatus.FREE, failure_rate=0.01, straggle_rate=0.01, task=None),
        Worker(memory_size=100, network_bandwidth=10, disk_bandwidth=10,
               status=WorkerStatus.FREE, failure_rate=0.01, straggle_rate=0.01, task=None),
        Worker(memory_size=100, network_bandwidth=10, disk_bandwidth=10, status=WorkerStatus.FREE, failure_rate=0.01, straggle_rate=0.01, task=None)]

    scheduler = Scheduler(taskG, sample_workers)
    scheduler.simulate()


if __name__ == "__main__":
    main()
