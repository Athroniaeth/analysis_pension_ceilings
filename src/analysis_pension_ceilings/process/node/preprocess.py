import re
import statistics
from functools import partial
from typing import Optional

import polars as pl


def extract_numbers(text: Optional[str], pattern: str = r"\d{1,6}") -> list[int]:
    """Extracts numbers from a given text string."""
    return re.findall(pattern, text) if text else []


def node_extract_numbers(
    df: pl.DataFrame,
    input_colname: str,
    output_colname: str,
    pattern: str = r"\d{1,6}",
    typing_output: pl.DataType = pl.Float32,
) -> pl.DataFrame:
    """
    Create column with extracted numbers from a text column.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        input_colname (str): The name of the input column.
        output_colname (str): The name of the output column.
        pattern (str): The regex pattern to extract numbers.
        typing_output (pl.DataType): The type of the output column.

    Examples:
        >>> df = pl.DataFrame({
        ...     "text": ["The price is 100€ - 200€", "The price is $200"],
        ... })
        >>> node_extract_numbers(df, input_colname="text", output_colname="price")
        shape: (2, 2)
        ┌──────────────────────────┬────────────────┐
        │ text                     ┆ price          │
        │ ---                      ┆ ---            │
        │ str                      ┆ list[f32]      │
        ╞══════════════════════════╪════════════════╡
        │ The price is 100€ - 200€ ┆ [100.0, 200.0] │
        │ The price is $200        ┆ [200.0]        │
        └──────────────────────────┴────────────────┘

    Raises:
        ValueError: If the input_column does not exist in the DataFrame.

    Returns:
        pl.DataFrame: A new DataFrame with the computed averages added as a column.
    """
    # Extract numbers from the input column
    df = df.with_columns(
        pl.col(input_colname)
        .map_elements(
            partial(extract_numbers, pattern=pattern), return_dtype=pl.List(pl.Utf8)
        )
        .alias(output_colname),
    )

    # Transform strings to numbers
    df = df.with_columns(
        pl.col(output_colname).map_elements(
            lambda x: [int(i) for i in x],
            return_dtype=pl.List(typing_output),
        )
    )

    return df


def node_average_list_numbers(
    df: pl.DataFrame,
    input_colname: str,
    output_colname: str,
) -> pl.DataFrame:
    """
    Create column with the average of a list of numbers.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.
        input_colname (str): The name of the input column.
        output_colname (str): The name of the output column.

    Examples:
        >>> df = pl.DataFrame({
        ...     "numbers": [[10, 20], [30, 40, 50]],
        ... })
        >>> node_average_list_numbers(df, input_colname="numbers", output_colname="average")
        shape: (2, 2)
        ┌──────────────┬─────────┐
        │ numbers      ┆ average │
        │ ---          ┆ ---     │
        │ list[i64]    ┆ f64     │
        ╞══════════════╪═════════╡
        │ [10, 20]     ┆ 15.0    │
        │ [30, 40, 50] ┆ 40.0    │
        └──────────────┴─────────┘

    Raises:
        ValueError: If the input_column does not exist in the DataFrame.

    Returns:
        pl.DataFrame: A new DataFrame with the computed averages added as a column.
    """
    # Calculate the average of the list of numbers
    df = df.with_columns(
        pl.col(input_colname)
        .map_elements(statistics.mean, return_dtype=pl.Float64)
        .alias(output_colname),
    )

    return df
