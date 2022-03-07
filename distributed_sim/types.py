from enum import Enum, auto


class SystemOptions(Enum):
    dask = auto()
    mapreduce = auto()


class MReduceProg(Enum):
    sort = auto()
    grep = auto()


class MReduceOp(Enum):
    map = auto()
    reduce = auto()


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
    UNASIGNED = auto()
