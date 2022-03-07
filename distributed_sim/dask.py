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
