from enum import Enum, auto


class SystemOptions(Enum):
    dask = auto()
    mapreduce = auto()


class CommonOp(Enum):
    startup = auto()
    compute = auto()
    read_remote = auto()
    write_remote = auto()
    read_disk = auto()
    write_disk = auto()


class MReduceProg(Enum):
    distributedsort = auto()
    distributedgrep = auto()


class MReduceOp(Enum):
    map = auto()
    reduce = auto()
    sort = auto()
    shuffle = auto()


class DaskProg(Enum):
    qr = auto()
    svd = auto()
    blocked_algo = auto()


class DaskOp(Enum):
    inc = auto()
    add = auto()
    getitem = auto()
    dot = auto()
    dotmany = auto()
    vstack = auto()
    ones = auto()


class EventType(Enum):
    TERMINATE = auto()
    FAIL = auto()
    STRAGGLE = auto()


class WorkerStatus(Enum):
    FREE = auto()
    FAILED = auto()
    BUSY = auto()


class TaskStatus(Enum):
    COMPLETE = auto()
    PENDING = auto()
    UNASSIGNED = auto()
