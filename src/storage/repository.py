from pathlib import Path

import duckdb
import polars as pl


def save_quotes(df: pl.DataFrame, file_path: str) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(file_path)


def query_quotes(file_path: str, query: str) -> pl.DataFrame:
    # Use DuckDB to query the parquet file
    # Ensure the query uses single quotes around the path if necessary, but DuckDB can just query the path
    # Typically query is like: SELECT * FROM 'file_path'
    conn = duckdb.connect(database=":memory:")
    result = conn.execute(query).pl()
    conn.close()
    return result
