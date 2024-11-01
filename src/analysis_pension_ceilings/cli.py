import typer
from typer import Typer

cli = Typer(no_args_is_help=True)


@cli.command()
def hello(name: str = typer.Argument("World", help="The name to say hello to.")):
    """
    Say hello to a name.
    """
    typer.echo(f"Hello {name}!")
    raise Exception("An error occurred.")


@cli.command()
def hello_world():
    """
    Say hello to the world.
    """
    typer.echo(f"Hello World!")