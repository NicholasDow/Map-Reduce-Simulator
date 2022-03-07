import click
from distributed_sim.types import *


@click.command()
def system_input():
    click.echo("Available systems: ")
    for j in SystemOptions:
        click.echo(f"[{j.value}] {j.name}")
    value = click.prompt('Please select a system', type=int)
    click.confirm('Do you want to continue?', abort=True)


# meant as a command line program for users to specify different systems and operations
if __name__ == "__main__":
    system_input()
