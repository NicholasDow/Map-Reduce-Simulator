"""For testing purposes only."""

from distributed_sim import *


def main():
    mr = MRProcedure(num_machines=1000,
                     n_records=1000000,
                     max_distance=10,
                     prog_type=MReduceProg.distributedsort)
    mr.run()


if __name__ == "__main__":
    main()
