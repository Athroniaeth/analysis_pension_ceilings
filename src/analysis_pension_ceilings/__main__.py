import sys
import traceback
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv

# Automatically add the working directory
path = Path(__file__).parents[1].absolute()
sys.path.append(f"{path}")

from analysis_pension_ceilings import logger  # noqa: E402
from analysis_pension_ceilings.cli import cli  # noqa: E402


def _typer_exception_to_python(exception: Exception) -> Tuple[str, str]:
    """
    Convert a Typer exception to a Python exception format.

    Args:
        exception (Exception): The exception to convert.

    Returns:
        Tuple[str, str]: A tuple with the readable exception and the readable traceback.

    """
    # Filter the stacktrace to include only the files from your project
    tb = traceback.extract_tb(exception.__traceback__)
    filtered_tb = [frame for frame in tb if r"src\analysis_pension_ceilings" in frame.filename]

    # Create a readable message
    stack_summary = traceback.StackSummary.from_list(filtered_tb)
    readable_exception = "".join(traceback.format_exception_only(type(exception), exception))
    readable_traceback = "".join(stack_summary.format())

    return readable_exception, readable_traceback


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
        # Convert the exception to a readable format
        readable_exception, readable_traceback = _typer_exception_to_python(exception)

        # Log the exception
        logger.error(f"Traceback (most recent call last):\n{readable_traceback}{readable_exception}")

    except KeyboardInterrupt:
        logger.debug("Exiting the program due to keyboard interrupt.")


if __name__ == "__main__":
    main()
