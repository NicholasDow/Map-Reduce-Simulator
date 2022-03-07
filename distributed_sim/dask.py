"""Uses classes and code from architecture.py to build the Dask specific task graph for grep and sort."""

# build an array of blocked array algorithms

# have some way to actually get the blocks

# The logic behind dask’s schedulers reduces to the following
# situation:
#
# - worker reports that it completed task and ready for another
# - update runtime state to record finished task
# - mark which new tasks can be run, data to be released
# - choose task to give worker from set of ready-to-run tasks

from .types import *


class BlockedAlgorithm:
    def __init__(self):
        raise "not implemented"


from .architecture import *
from .utils import *
from .types import *


class DaskProcedure:

    def __init__(self,
                 num_machines: int,
                 max_distance: int,
                 n_records: int,
                 prog_type: MReduceProg):
        self.num_machines = num_machines
        self.max_distance = max_distance
        self.n_records = n_records
        self.prog = prog_type
        self.workers = seed_workers(num_machines, max_distance)
        self.task_graph = None
        if self.prog == MReduceProg.distributedgrep:
            self.task_graph = self._build_grep(
                self.num_machines, self.n_records)
        if self.prog == MReduceProg.distributedsort:
            self.task_graph = self._build_sort(
                self.num_machines, self.n_records)
        self.scheduler = Scheduler(self.task_graph, self.workers)

    # @staticmethod
    # def translate_op(op: MReduceOp) -> List[CommonOp]:
    #     if op == MReduceOp.map:
    #         return [CommonOp.read_disk,
    #                 CommonOp.compute,
    #                 CommonOp.write_disk]
    #     elif op == MReduceOp.reduce:
    #         return [CommonOp.read_remote,
    #                 CommonOp.compute,
    #                 CommonOp.write_disk]

    # @staticmethod
    # def _build_grep(num_mach: int, n_rec: int) -> TaskGraph:
    #     t = TaskGraph()
    #     # split initial records
    #     dist = n_rec / num_mach
    #     # build map tasks
    #     maptask_list = []
    #     for i in range(num_mach):
    #         maptask_list.append(
    #             Task(parent_prog=MReduceProg.distributedgrep,
    #                  task_id=i, task_op=MReduceOp.map, n_records=dist))
    #     t.add_layer(maptask_list, starting_idx=0,
    #                 option=TaskLayerChoices.first_layer)
    #     # reduce tasks
    #     reducetask_list = []
    #     for i in range(num_mach):
    #         reducetask_list.append(
    #             Task(parent_prog=MReduceProg.distributedgrep,
    #                  task_id=i+num_mach, task_op=MReduceOp.reduce, n_records=dist))
    #     t.add_layer(reducetask_list, starting_idx=num_mach,
    #                 option=TaskLayerChoices.one_to_one)
    #     return t

    # @staticmethod
    # def _build_sort(num_mach: int, n_rec: int) -> TaskGraph:
    #     t = TaskGraph()
    #     # split initial records
    #     dist = n_rec / num_mach
    #     # build map tasks
    #     maptask_list = []
    #     for i in range(num_mach):
    #         maptask_list.append(
    #             Task(parent_prog=MReduceProg.distributedsort,
    #                  task_id=i, task_op=MReduceOp.map, n_records=dist))
    #     t.add_layer(maptask_list, starting_idx=0,
    #                 option=TaskLayerChoices.first_layer)
    #     # shuffle task (synchro barrier)
    #     t.add_layer(
    #         [Task(parent_prog=MReduceProg.distributedsort,
    #               task_id=num_mach, task_op=MReduceOp.shuffle, n_records=n_rec)],
    #         starting_idx=num_mach,
    #         option=TaskLayerChoices.fully_connected)
    #     # reduce tasks
    #     reducetask_list = []
    #     for i in range(num_mach):
    #         reducetask_list.append(
    #             Task(parent_prog=MReduceProg.distributedsort,
    #                  task_id=i+num_mach+1, task_op=MReduceOp.reduce, n_records=dist))
    #     t.add_layer(reducetask_list, starting_idx=num_mach+1,
    #                 option=TaskLayerChoices.fully_connected)
    #     return t

    # def run(self):
    #     self.scheduler.simulate()
