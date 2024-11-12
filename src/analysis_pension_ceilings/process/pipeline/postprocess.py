import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame, Series

from analysis_pension_ceilings import DATA_PATH
from analysis_pension_ceilings.process.pipeline.clean import pipeline_clean
from analysis_pension_ceilings.process.pipeline.preprocess import (
    OutputSchema as InputSchema,
    pipeline_preprocess,
)


class OutputSchema(pa.DataFrameModel):
    percentage: Series[float] = pa.Field()
    average_pension: Series[float] = pa.Field()
    number_pensioners: Series[float] = pa.Field()
    average_benefits: Series[int] = pa.Field()
    total_average_benefits: Series[float] = pa.Field()
    total_average_pension: Series[float] = pa.Field()
    average_pension_after_ceiling: Series[float] = pa.Field()


def pipeline_postprocess(
    df: DataFrame[InputSchema],
    ceiling: int = 2_000,
    nbr_pensioners: int = 14_900_000,
) -> DataFrame[OutputSchema]:
    # Calculate the number of pensioners based on the percentage
    calcul = (pl.col("percentage") / 100) * nbr_pensioners
    df = df.with_columns(calcul.alias("number_pensioners"))

    # Apply the ceiling to the average to get benefits
    df = df.with_columns(
        pl.when((pl.col("average_pension") - ceiling) > 0)
        .then(pl.col("average_pension") - ceiling)
        .otherwise(0)
        .alias("average_benefits")
        .cast(pl.Int32)
    )

    # Calculate total of average benefits
    calcul = pl.col("average_benefits") * pl.col("number_pensioners")
    df = df.with_columns(calcul.alias("total_average_benefits"))

    # Calculate total of amount of pension
    calcul = pl.col("average_pension") * pl.col("number_pensioners")
    df = df.with_columns(calcul.alias("total_average_pension"))

    # Calculate average pension after the ceiling
    calcul = pl.col("average_pension") - pl.col("average_benefits")
    df = df.with_columns(calcul.alias("average_pension_after_ceiling"))

    return df


if __name__ == "__main__":
    path = DATA_PATH / "pension_ceilings.xlsx"

    df = pl.read_excel(
        path,
        sheet_name="pension brute DD_carrière comp",
    )

    df = pipeline_clean(df)
    df = pipeline_preprocess(df)
    df = pipeline_postprocess(df)

    print(df)
