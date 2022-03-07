import click
from distributed_sim import *


@click.command()
def system_input():
    click.echo("Available systems: ")
    for j in SystemOptions:
        click.echo(f"[{j.value}] {j.name}")
    sys_value = click.prompt('Please select a system', type=int)
    click.echo("Available Procedures: ")
<<<<<<< HEAD
    if sys_value == SystemOptions.mapreduce.value:
        for j in MReduceProg:
            click.echo(f"[{j.value}] {j.name}")
    elif sys_value == SystemOptions.dask.value:
        for j in DaskProg:
=======
    if sys_value == SystemOptions.dask.value:
        for j in DaskProg:
            click.echo(f"[{j.value}] {j.name}")
    elif sys_value == SystemOptions.mapreduce.value:
        for j in MReduceProg:
>>>>>>> 2ae47f32b3a87439d33a018d464447bc1383ff47
            click.echo(f"[{j.value}] {j.name}")
    prog_value = click.prompt('Please select a procedure', type=int)
    if sys_value == SystemOptions.mapreduce.value:
        sys = MRProcedure(num_machines=100,
                          n_records=1000,
                          max_distance=10,
                          prog_type=MReduceProg(prog_value))
        click.confirm('Do you want to continue?', abort=True)
        sys.run()


# meant as a command line program for users to specify different systems and operations
if __name__ == "__main__":
    system_input()
