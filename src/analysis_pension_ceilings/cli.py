import typer
from typer import Typer

from analysis_pension_ceilings import logger

cli = Typer(no_args_is_help=True)


@cli.command()
def hello(name: str = typer.Argument("World", help="The name to say hello to.")):
    """
    Say hello to a name.
    """
    typer.echo(f"Hello {name}!")
    raise Exception("An error occurred.")


@cli.command()
def run(
    host: str = typer.Option("localhost", envvar="HOST", help="Adresse sur laquelle le serveur doit écouter."),
    port: int = typer.Option(7860, envvar="PORT", help="Port sur lequel le serveur doit écouter."),
):
    """
    Start the server with the given environment.

    Args:
        host (str): Host IP address of the server.
        port (int): Port number of host server.

    Raises:
        FileNotFoundError: If the SSL key or certificate file is not found.
        ValueError: If the SSL key or certificate file is missing.

    """
    global settings

    settings.host = host
    settings.port = port

    logger.info(f"Starting the server with host: {host} and port: {port}")
    logger.info(f"Postgres host: {settings.postgres_url}")
    logger.info(f"Redis host: {settings.redis_url}")

    # Run the Gradio application with the given environment
    from pipeforms.app import launch_app

    launch_app(
        host=host,
        port=port,
    )


if __name__ == "__main__":
    cli()