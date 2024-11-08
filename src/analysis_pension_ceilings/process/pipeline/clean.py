from typing import Dict

import polars as pl

from analysis_pension_ceilings import DATA_PATH
from analysis_pension_ceilings.process.node.clean import node_drop_lines, node_refresh_header

MAPPING_COLUMNS = {
    "Montant\r\nde pension\r\n(en euros)": "slice_amount",
    "Ensemble": "percentage",
}


def pipeline_clean(
        df: pl.DataFrame,
        n_first: int = 1,
        n_last: int = 5,
        mapping_columns: Dict[str, str] = None,
):
    """
    Transform bad format of Excel file to a clean DataFrame.

    Args:
        df (DataFrame[Schema]): The DataFrame to clean.
        n_first (int): The number of first lines to drop.
        n_last (int): The number of last lines to drop.
        mapping_columns (Dict[str, str]): The mapping of columns to rename.

    Returns:
        DataFrame[Schema]: The cleaned DataFrame.
    """
    if mapping_columns is None:
        mapping_columns = MAPPING_COLUMNS

    # Delete first lines (useless data)
    df = node_drop_lines(df, n_first=n_first, n_last=n_last)

    # Drop useless columns
    df = node_refresh_header(df)

    # Rename columns to snake_case format
    df = df.rename(mapping_columns)

    # Drop useless columns
    df = df[['slice_amount', 'percentage']]

    return df


if __name__ == "__main__":
    path = DATA_PATH / "pension_ceilings.xlsx"

    df = pl.read_excel(
        path,
        sheet_name="pension brute DD_carrière comp",
    )

    df = pipeline_clean(df)
    print(df)