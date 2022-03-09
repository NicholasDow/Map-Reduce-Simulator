"""For testing purposes only."""

from distributed_sim import *


def main():
    mr = MRProcedure(num_machines=100,
                     n_records=10000,
                     max_distance=10,
                     prog_type=MReduceProg.distributedsort)
    mr.run()


if __name__ == "__main__":
    main()
