from random import shuffle
from .architecture import *
from .utils import *
from .types import *


class MRProcedure:

    def __init__(self,
                 num_machines: int,
                 n_records: int,
                 prog_type: MReduceProg = MReduceProg.sort):
        self.num_machines = num_machines
        self.n_records = n_records
        self.prog = prog_type
        self.workers = seed_workers(num_machines)
        self.task_graph = None
        if self.prog == MReduceProg.distributedgrep:
            self.task_graph = self._build_grep(
                self.num_machines, self.n_records)
        if self.prog == MReduceProg.distributedsort:
            self.task_graph = self._build_sort(
                self.num_machines, self.n_records)
        self.scheduler = Scheduler(self.task_graph, self.workers)

    @staticmethod
    def translate_op(op: MReduceOp) -> List[CommonOp]:
        if op == MReduceOp.map:
            pass
        elif op == MReduceOp.reduce:
            pass

    @staticmethod
    def _build_grep(num_mach: int, n_rec: int) -> TaskGraph:
        t = TaskGraph()
        # split initial records
        dist = n_rec / num_mach
        # build map tasks
        maptask_list = []
        for i in range(num_mach):
            maptask_list.append(
                Task(task_id=i, task_op=MReduceOp.map, n_records=dist))
        t.add_layer(maptask_list, option=TaskLayerChoices.first_layer)
        # reduce tasks
        reducetask_list = []
        for i in range(num_mach):
            reducetask_list.append(
                Task(task_id=i, task_op=MReduceOp.reduce, n_records=dist))
        t.add_layer(reducetask_list, option=TaskLayerChoices.one_to_one)
        return t

    @staticmethod
    def _build_sort(num_mach: int, n_rec: int) -> TaskGraph:
        t = TaskGraph()
        # split initial records
        dist = n_rec / num_mach
        # build map tasks
        maptask_list = []
        for i in range(num_mach):
            maptask_list.append(
                Task(task_id=i, task_op=MReduceOp.map, n_records=dist))
        t.add_layer(maptask_list, option=TaskLayerChoices.first_layer)
        # shuffle task (synchro barrier)
        t.add_layer(
            Task(task_id=num_mach, task_op=MReduceOp.shuffle, n_records=n_rec),
            option=TaskLayerChoices.fully_connected)
        # reduce tasks
        reducetask_list = []
        for i in range(num_mach):
            reducetask_list.append(
                Task(task_id=i+num_mach+1, task_op=MReduceOp.reduce, n_records=dist))
        t.add_layer(reducetask_list, option=TaskLayerChoices.one_to_one)
        return t

    def execute(self):
        self.scheduler.simulate()
