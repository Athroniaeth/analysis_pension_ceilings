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
        .map_elements(lambda x: [int(i) for i in x], return_dtype=pl.List(pl.Float32))
        .alias("extracted_numbers"),
    )

    # Calculate the average of the extracted numbers
    df = df.with_columns(
        pl.col("extracted_numbers")
        .map_elements(statistics.mean, return_dtype=pl.Float32)
        .alias(output_column),
    )

    return df


def calcul_nbr_pensioners(
    df: pl.DataFrame,
    colname_percentage: str = "percentage",
    colname_nbr_pensioners: str = "number_pensioners",
    nbr_pensioners: int = 14_900_000,
) -> pl.DataFrame:
    """
    Calculate the number of pensioners based on the percentage of the population.

    Args:
        df (pl.DataFrame): The input Polars DataFrame
        colname_percentage (str): Name of the column containing the percentage values
        colname_nbr_pensioners (str): Name of the column to store the calculated number of pensioners
        nbr_pensioners (int): The total number of pensioners in the population

    Returns:
        pl.DataFrame: The DataFrame with the calculated number of pensioners
    """
    # Calculate the number of pensioners based on the percentage
    df = df.with_columns(
        ((pl.col(colname_percentage) / 100) * nbr_pensioners)
        .alias(colname_nbr_pensioners)
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
    df = df.head(df.height - 5)

    # extract and average numbers
    df = extract_and_average_numbers(df, output_column=colname_average_pension)

    # Rename the columns
    df = df.with_columns(pl.col("Ensemble").alias(colname_percentage))

    # Keep only the relevant columns
    df = df.select([colname_percentage, colname_average_pension])

    # Last line is open-ended class, allow to decide the average
    df[-1, colname_average_pension] = average_open_ended

    return df


def apply_pension_ceiling(
        df: pl.DataFrame,
        ceiling: int = 2000,
        colname_benefits: str = "average_benefits",
) -> pl.DataFrame:
    """
    Apply a ceiling to the average pension values in the DataFrame.

    Args:
        df (pl.DataFrame): The input Polars DataFrame
        ceiling (int): The maximum value to cap the average pension values
        colname_benefits (str): Column name with avg pension after the ceiling

    Returns:
        pl.DataFrame: The DataFrame with the ceiling applied to the average pension values
    """
    # Apply the ceiling to the average to get benefits
    df = df.with_columns(
        pl.when((pl.col("average_pension") - ceiling) > 0)
        .then(pl.col("average_pension") - ceiling).otherwise(0)
        .alias(colname_benefits)
        .cast(pl.Int32)
    )

    return df


if __name__ == "__main__":
    path = DATA_PATH / "pension_ceilings.xlsx"

    parser = fastexcel.read_excel(path)
    sheets = [{"index": i + 1, "name": nm} for i, nm in enumerate(parser.sheet_names)]

    df = pl.read_excel(
        path,
        sheet_name="pension brute DD_carrière comp",
        read_options={"header_row": 2},
    )
    print([dict_ for dict_ in df.columns])

    df = preprocess_df(df)
    print(df.head(5))
    print(df.tail(5))

    df = calcul_nbr_pensioners(df, nbr_pensioners=14_900_000)
    print("Number of pensioners")
    print(df.head(5))
    print(df.tail(5))

    df = apply_pension_ceiling(df, ceiling=2000)
    print(df.head(5))
    print(df.tail(5))

    # Calcul total of average benefits
    df = df.with_columns(
        (pl.col("average_benefits") * pl.col("number_pensioners")).
        alias("total_average_benefits")
    )

    # Calcul total of amount of pension
    df = df.with_columns(
        (pl.col("average_pension") * pl.col("number_pensioners")).
        alias("total_average_pension")
    )
    print(df.head(5))
    print(df.tail(5))

    total_pension = df["total_average_pension"].sum()
    total_benefits = df["total_average_benefits"].sum()
    percentage_benefits = (total_benefits / total_pension)

    print(f"Total benefits: {total_benefits:,.0f}€ (per month)")
    print(f"Total benefits: {total_benefits * 12:,.0f}€ (per year)")
    print(f"Percentage of benefits: {percentage_benefits:.2%}")
