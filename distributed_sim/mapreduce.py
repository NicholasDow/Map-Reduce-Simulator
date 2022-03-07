from .architecture import *
from .utils import *
from .types import *


class MRProcedure:

    def __init__(self,
                 num_machines: int,
                 max_distance: int,
                 n_records: int,
                 prog_type: MReduceProg):
        self.num_machines = num_machines
        self.max_distance = max_distance
        self.n_records = n_records
        self.split_size = 64
        self.grep_selectivity = 0.000001
        self.prog = prog_type
        self.workers = seed_workers(num_machines, max_distance)
        self.task_graph = None
        if self.prog == MReduceProg.distributedgrep:
            self.n_map_tasks = int(n_rec / self.split_size)
            self.n_shuffle_tasks = self.n_map_tasks
            self.n_shuffle_split = max(1,int((n_rec*self.grep_selectivity)/self.split_size))
            self.n_reduce_tasks = 1
            self.task_graph = self._build_grep(
                self.n_map_tasks, self.n_shuffle_tasks, self.n_reduce_tasks, self.split_size, self.shuffle_split)
        if self.prog == MReduceProg.distributedsort:
            self.n_map_tasks = n_rec / self.split_size
            self.n_shuffle_tasks = n_map_tasks
            self.n_reduce_tasks = min (n_map_tasks, num_mach * 3)
            self.task_graph = self._build_sort(
                self.n_map_tasks, self.n_shuffle_tasks, self.n_reduce_tasks, self.split_size)
        self.scheduler = Scheduler(
            self.task_graph, self.workers, failure_penalty=10)

    @staticmethod
    def translate_op(op: MReduceOp) -> List[CommonOp]:
        if op == MReduceOp.map:
            return [CommonOp.read_disk,
                    CommonOp.compute,
                    CommonOp.write_disk]
        elif op == MReduceOp.reduce:
            return [CommonOp.read_remote,
                    CommonOp.compute,
                    CommonOp.write_disk]

    @staticmethod
    def _build_grep(n_map_tasks:int, n_shuffle_tasks:int, n_reduce_tasks:int, split_size:int, shuffle_split:int) -> TaskGraph:
        t = TaskGraph()

        maptask_list = []
        for i in range(n_map_tasks):
            maptask_list.append(
                Task(parent_prog=MReduceProg.distributedgrep,
                     task_id=i, task_op=MReduceOp.map, n_records=split_size))
        t.add_layer(maptask_list, starting_idx=0,
                    option=TaskLayerChoices.first_layer)
        shuffletask_list = []
        for i in range(n_shuffle_tasks):
            shuffletask_list.append(
                Task(parent_prog=MReduceProg.distributedgrep,
                     task_id=i+n_map_tasks, task_op=MReduceOp.shuffle, n_records=shuffle_split))
        t.add_layer(shuffletask_list, starting_idx=n_map_tasks,
                    option=TaskLayerChoices.one_to_one)           
        # shuffle task (synchro barrier)
        t.add_layer(
            [Task(parent_prog=MReduceProg.distributedgrep,
                  task_id=n_map_tasks + n_shuffle_tasks, task_op=MReduceOp.barrier, n_records=n_shuffle_tasks*shuffle_split)],
            starting_idx=n_map_tasks + n_shuffle_tasks,
            option=TaskLayerChoices.fully_connected)
        # reduce tasks
        reducetask_list = []
        for i in range(n_reduce_tasks):
            reducetask_list.append(
                Task(parent_prog=MReduceProg.distributedgrep,
                     task_id=i+n_map_tasks + n_shuffle_tasks+1, task_op=MReduceOp.reduce, n_records=n_shuffle_tasks*shuffle_split )
        t.add_layer(reducetask_list, starting_idx=n_map_tasks + n_shuffle_tasks+1,
                    option=TaskLayerChoices.fully_connected)
        return t

    @staticmethod
    def _build_sort(n_map_tasks:int, n_shuffle_tasks:int, n_reduce_tasks:int, split_size:int) -> TaskGraph:
        t = TaskGraph()

        maptask_list = []
        for i in range(n_map_tasks):
            maptask_list.append(
                Task(parent_prog=MReduceProg.distributedsort,
                     task_id=i, task_op=MReduceOp.map, n_records=split_size))
        t.add_layer(maptask_list, starting_idx=0,
                    option=TaskLayerChoices.first_layer)
        shuffletask_list = []
        for i in range(n_shuffle_tasks):
            shuffletask_list.append(
                Task(parent_prog=MReduceProg.distributedsort,
                     task_id=i+n_map_tasks, task_op=MReduceOp.shuffle, n_records=split_size))
        t.add_layer(shuffletask_list, starting_idx=n_map_tasks,
                    option=TaskLayerChoices.one_to_one)           
        # shuffle task (synchro barrier)
        t.add_layer(
            [Task(parent_prog=MReduceProg.distributedsort,
                  task_id=n_map_tasks + n_shuffle_tasks, task_op=MReduceOp.barrier, n_records=n_rec)],
            starting_idx=n_map_tasks + n_shuffle_tasks,
            option=TaskLayerChoices.fully_connected)
        # reduce tasks
        reducetask_list = []
        for i in range(n_reduce_tasks):
            reducetask_list.append(
                Task(parent_prog=MReduceProg.distributedsort,
                     task_id=i+n_map_tasks + n_shuffle_tasks+1, task_op=MReduceOp.reduce, n_records=n_rec/n_reduce_tasks )
        t.add_layer(reducetask_list, starting_idx=n_map_tasks + n_shuffle_tasks+1,
                    option=TaskLayerChoices.fully_connected)
        t.print_graph()
        return t

    def run(self):
        self.scheduler.simulate()
