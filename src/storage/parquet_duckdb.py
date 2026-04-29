from pathlib import Path

import duckdb
import polars as pl


def save_to_parquet(df: pl.DataFrame, file_path: Path | str) -> None:
    path_obj = Path(file_path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path_obj)


def init_duckdb_with_parquet(
    file_path: Path | str, table_name: str = "quotes"
) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database=":memory:")
    query = f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}');"  # noqa: S608
    conn.execute(query)
    return conn
