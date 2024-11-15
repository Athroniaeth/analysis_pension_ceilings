from os import PathLike
from pathlib import Path
from typing import Union

import requests


def download_excel_from_url(url: str, filename: Union[str, PathLike]):
    """
    Downloads an Excel file from a given URL and saves it locally with the specified filename.

    Args:
        url (str): The URL of the Excel file to download.
        filename (str): The name of the file to save locally.

    Raises:
        requests.exceptions.RequestException: If there is an issue with the download request.
    """
    path = Path(filename)
    print(f"{path.absolute()=}")
    print("*" * 50)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Send a GET request to the URL
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Write the content to the specified file
        path.write_bytes(response.content)

        print(f"File successfully downloaded and saved as '{filename}'.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file: {e}")
