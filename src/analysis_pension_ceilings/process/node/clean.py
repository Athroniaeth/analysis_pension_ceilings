import polars as pl


def node_drop_lines(df: pl.DataFrame, n_first: int = 0, n_last: int = 0):
    """
    Clean the Excel file and return the cleaned DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to clean.
        n_first (int): The number of first lines to drop.
        n_last (int): The number of last lines to drop.

    Examples:
        >>> df = pl.DataFrame({
        ...     "height": [1, 2, 3, 4, 5],
        ...     "weight": [10, 20, 30, 40, 50],
        ... })
        >>> node_drop_lines(df, n_first=1, n_last=2)
        shape: (2, 2)
        ┌────────┬────────┐
        │ height ┆ weight │
        │ ---    ┆ ---    │
        │ i64    ┆ i64    │
        ╞════════╪════════╡
        │ 2      ┆ 20     │
        │ 3      ┆ 30     │
        └────────┴────────┘

    Returns:
        pl.DataFrame: The cleaned DataFrame.
    """
    # Delete first lines (useless data)
    df = df.tail(df.height - n_first)

    # Delete last lines (useless data)
    df = df.head(df.height - n_last)

    return df


def node_refresh_header(df: pl.DataFrame):
    """
    Replace column names by the first row of the DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to clean.

    Examples:
        >>> df = pl.DataFrame({
        ...     "A": ["C", "E"],
        ...     "B": ["D", "F"],
        ... })
        >>> node_refresh_header(df)
        shape: (1, 2)
        ┌─────┬─────┐
        │ C   ┆ D   │
        │ --- ┆ --- │
        │ str ┆ str │
        ╞═════╪═════╡
        │ E   ┆ F   │
        └─────┴─────┘

    Returns:
        pl.DataFrame: The cleaned DataFrame.
    """
    # Get first line of the df
    line = df.head(1).to_dicts()

    # List[Dict[str, Any]] -> Dict[str, Any]
    line = line.pop()

    # Cast all values to str (can have int, float, etc.)
    line = {f"{k}": f"{v}" for k, v in line.items()}

    # Delete first line of the df
    df = df.tail(df.height - 1)

    # Change df columns by first line
    df = df.rename(line)

    return df
