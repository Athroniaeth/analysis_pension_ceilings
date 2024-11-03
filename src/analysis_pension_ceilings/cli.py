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
    app: str = typer.Option("analysis_pension_ceilings.app:app", envvar="APP", help="Application to launch."),
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
    logger.info(f"Starting the server with host: '{host}' and port: '{port}'")

    # Run the Gradio application with the given environment
    launch_app(
        app=app,
        host=host,
        port=port,
    )


def launch_app(
    app: str = "analysis_pension_ceilings.app:app",
    host: str = "localhost",
    port: int = 7860,
    log_level: str = "info",
):
    """
    Launch the Gradio application with the given environment.

    Args:
        app (str): Application to launch.
        host (str): Host IP address of the server.
        port (int): Port number of host server.
        log_level (str): Logging level for the server.
    """
    import uvicorn

    uvicorn.run(
        app=app,
        host=host,
        port=port,
        log_level=log_level,
    )


if __name__ == "__main__":
    cli()
