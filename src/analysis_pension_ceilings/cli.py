import typer
from typer import Typer

from analysis_pension_ceilings import logger, DATA_PATH
from analysis_pension_ceilings.download import download_excel_from_url

cli = Typer(no_args_is_help=True)

DATASET_INPUT_URL = (
    "https://www.data.gouv.fr/fr/datasets/r/5f9c260a-5a9c-442a-8b30-6f9efebaeb16"
)


@cli.command()
def download(
    url: str = typer.Argument(DATASET_INPUT_URL, help="URL to download the file from."),
    filename: str = typer.Argument(
        "pension_ceilings.xlsx", help="Name of the file to save locally."
    ),
):
    """
    Download input data from the specified URL.

    Args:
        url (str): URL to download the file from.
        filename (str): Name of the file to save locally.

    Raises:
        RequestException: If there is an issue with the download request.

    """
    logger.info(f"Downloading the file from the URL: '{url}'")

    path = DATA_PATH / filename

    if path.exists():
        raise FileExistsError(
            f"File '{filename}' already exists in the data directory."
        )

    download_excel_from_url(url, path)
    logger.info(f"File successfully downloaded and saved as '{path}'")


@cli.command()
def run(
    app: str = typer.Option(
        "analysis_pension_ceilings.app:app", envvar="APP", help="Application to launch."
    ),
    host: str = typer.Option(
        "localhost", envvar="HOST", help="Address on which the server should listen."
    ),
    port: int = typer.Option(
        7860, envvar="PORT", help="Port on which the server should listen."
    ),
):
    """
    Start the server with the given environment.

    Args:
        app (str): Application to launch.
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
