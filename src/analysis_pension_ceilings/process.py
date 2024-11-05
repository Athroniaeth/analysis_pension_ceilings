import re
import statistics
from typing import Optional

import fastexcel
import polars as pl

from analysis_pension_ceilings import DATA_PATH


def extract_and_average_numbers(
    df: pl.DataFrame,
    input_column: str = "Montant\r\nde pension\r\n(en euros)",
    output_column: str = "average_pension",
) -> pl.DataFrame:
    """
    Extracts numbers from a given column, calculates the average of the numbers,
    and appends the result as a new column in the DataFrame.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        input_column (str): Name of the column containing strings with numbers.
        output_column (str): Name of the column to store the computed averages.

    Returns:
        pl.DataFrame: A new DataFrame with the computed averages added as a column.

    Raises:
        ValueError: If the input_column does not exist in the DataFrame.
    """
    # Check if the input column exists in the DataFrame
    if input_column not in df.columns:
        raise ValueError(f"The column '{input_column}' doesn't exist")

    def extract_numbers(text: Optional[str]) -> list[int]:
        """Extracts numbers from a given text string."""
        return re.findall(r"\d{1,6}", text) if text else []

    # Extract numbers from the input column
    df = df.with_columns(
        pl.col(input_column)
        .map_elements(extract_numbers, return_dtype=pl.List(pl.Utf8))
        .alias("extracted_numbers"),
    )

    # Transform strings to integers
    df = df.with_columns(
        pl.col("extracted_numbers")
        .map_elements(lambda x: [int(i) for i in x], return_dtype=pl.List(pl.UInt16))
        .alias("extracted_numbers"),
    )

    # Calculate the average of the extracted numbers
    df = df.with_columns(
        pl.col("extracted_numbers")
        .map_elements(statistics.mean, return_dtype=pl.UInt16)
        .alias(output_column),
    )

    return df


def preprocess_df(
    df: pl.DataFrame,
    colname_percentage: str = "percentage",
    colname_average_pension: str = "average_pension",
    average_open_ended: int = 8000,
) -> pl.DataFrame:
    """
    Preprocess the DataFrame by removing unnecessary rows and columns.

    Notes:
        Open-ended classes are intervals where the lower or upper limit is undefined
        (e.g., ">4500" or "<50"). These classes make it difficult to calculate precise
        statistics, as exact boundaries are unknown.

        If an open-ended class is present, an estimated value can be provided via
        the `adjust_mean` parameter to improve accuracy.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        colname_percentage (str): Name of the column containing the percentage values.
        colname_average_pension (str): Name of the column containing the average pension values.
        average_open_ended (int): The average value to use for the open-ended class.

    Returns:
        pl.DataFrame: The preprocessed DataFrame.
    """
    # Delete last lines (useless data)
    df = df.head(df.height - 4)

    # extract and average numbers
    df = extract_and_average_numbers(df, output_column=colname_average_pension)

    # Rename the columns
    df = df.with_columns(pl.col("Ensemble").alias(colname_percentage))

    # Keep only the relevant columns
    df = df.select([colname_percentage, colname_average_pension])

    # Last line is open-ended class, allow to decide the average
    df[-1, colname_average_pension] = average_open_ended

    return df


if __name__ == "__main__":
    path = DATA_PATH / "pension_ceilings.xlsx"

    parser = fastexcel.read_excel(path)
    sheets = [{"index": i + 1, "name": nm} for i, nm in enumerate(parser.sheet_names)]

    df = pl.read_excel(
        path,
        sheet_name="pension brute de droit direct",
        read_options={"header_row": 2},
    )
    print([dict_ for dict_ in df.columns])

    df = preprocess_df(df)
    print(df.head(5))
    print(df.tail(5))
