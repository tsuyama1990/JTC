from pathlib import Path
from typing import Any

import duckdb
import polars as pl


def save_quotes(df: pl.DataFrame, file_path: str) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(file_path)


def query_quotes(file_path: str, query: str) -> list[dict[str, Any]]:
    # DuckDB can natively query Parquet files by referencing them in the FROM clause
    # But usually it's better to create a view or just replace 'data' with the file path
    # We'll automatically replace 'data' with the actual file path string for ease of use

    modified_query = query.replace("FROM data", f"FROM '{file_path}'")

    conn = duckdb.connect()
    try:
        result = conn.execute(modified_query).fetchall()
        columns = [desc[0] for desc in conn.description]

        dict_results = []
        for row in result:
            dict_results.append(dict(zip(columns, row, strict=False)))

        return dict_results
    finally:
        conn.close()
