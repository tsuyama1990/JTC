from pathlib import Path

import duckdb
import polars as pl


def save_quotes(df: pl.DataFrame, file_path: str) -> None:
    path = Path(file_path)
    # Ensure directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(file_path)

def load_quotes(file_path: str) -> pl.DataFrame:
    return pl.read_parquet(file_path)

def query_quotes(file_path: str, query: str) -> pl.DataFrame:
    """
    Executes a DuckDB SQL query against the specified Parquet file.
    Example query: "SELECT * FROM data WHERE day_of_week = 1"
    The table name in the query must be 'data' (we'll replace 'data' with the file path
    or register the file as a view).
    Actually, let's just create a view or replace 'data' or let the user use the file path directly.
    Wait, the spec example says:
    "SELECT * FROM 'data/processed_quotes.parquet' WHERE day_of_week = 1"
    But in the test I wrote: "SELECT * FROM data WHERE day_of_week = 3". Let's support creating a view named 'data'.
    """
    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE VIEW data AS SELECT * FROM '{file_path}'")  # noqa: S608
        result = conn.execute(query).pl()
    finally:
        conn.close()
    return result
