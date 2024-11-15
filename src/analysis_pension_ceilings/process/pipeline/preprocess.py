import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame, Series

from analysis_pension_ceilings import DATA_PATH
from analysis_pension_ceilings.process.node.preprocess import (
    node_extract_numbers,
    node_average_list_numbers,
)
from analysis_pension_ceilings.process.pipeline.clean import OutputSchema as InputSchema
from analysis_pension_ceilings.process.pipeline.clean import pipeline_clean


class OutputSchema(pa.DataFrameModel):
    percentage: Series[float] = pa.Field()
    average_pension: Series[float] = pa.Field()


@pa.check_types
def pipeline_preprocess(df: DataFrame[InputSchema], average_open_ended: float = 8000) -> DataFrame[OutputSchema]:
    """
    Transform bad format of Excel file to a clean DataFrame.

    Args:
        df (DataFrame[Schema]): The DataFrame to clean.
        average_open_ended (float): The value to use for the open-ended class.

    Returns:
        DataFrame[Schema]: The cleaned DataFrame.
    """
    # Cast the percentage column to float
    df = df.with_columns(pl.col("percentage").cast(pl.Float64))

    # Delete first lines (useless data)
    df = node_extract_numbers(df, input_colname="slice_amount", output_colname="extracted_numbers")

    # Calculate the average of the extracted numbers
    df = node_average_list_numbers(df, input_colname="extracted_numbers", output_colname="average_pension")

    # Keep only the relevant columns
    df = df.select(["percentage", "average_pension"])

    # Last line is open-ended class, allow to decide the average
    df[-1, "average_pension"] = average_open_ended

    return df


if __name__ == "__main__":
    path = DATA_PATH / "pension_ceilings.xlsx"

    df = pl.read_excel(
        path,
        sheet_name="pension brute DD_carrière comp",
    )

    df = pipeline_clean(df)
    df = pipeline_preprocess(df)
    print(df)
