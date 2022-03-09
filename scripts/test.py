"""For testing purposes only."""

from distributed_sim import *


def main():
<<<<<<< HEAD
    mr = MRProcedure(num_machines=100,
                     n_records=10000,
=======
    mr = MRProcedure(num_machines=1000,
                     n_records=1000000,
>>>>>>> 0bf8b3bb1ea76de377e44012568e109bc7a358f4
                     max_distance=10,
                     prog_type=MReduceProg.distributedsort)
    mr.run()


if __name__ == "__main__":
    main()
