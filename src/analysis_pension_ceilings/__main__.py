import sys
from pathlib import Path

from dotenv import load_dotenv

# Automatically add the working directory
path = Path(__file__).parents[1].absolute()
sys.path.append(f"{path}")

from analysis_pension_ceilings import logger  # noqa: E402
from analysis_pension_ceilings.cli import cli  # noqa: E402


def main():
    """
    Main function to run the application.

    Raises:
        SystemExit: If the program is exited.

    Returns:
        int: The return code of the program.

    """
    # Load the environment variables
    load_dotenv()

    # Get the arguments for the program
    arguments = " ".join(sys.argv[1:])

    # Add the user command to the logs (first is src path)
    logger.info(f"Arguments passed: {arguments}")

    try:
        cli()
    except Exception as exception:
        logger.error(f"An error occurred: '{exception}'")
        raise
    except KeyboardInterrupt as exception:
        logger.debug(f"Exiting the program: '{exception}'")


if __name__ == "__main__":
    main()
