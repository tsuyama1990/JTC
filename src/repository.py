"""Repository Module."""
from pathlib import Path

import duckdb
import polars as pl


class QuoteRepository:
    def save(self, df: pl.DataFrame, filepath: str) -> None:
        """Saves a Polars DataFrame to a Parquet file, creating directories if needed."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)

    def query(self, filepath: str, sql: str) -> pl.DataFrame:
        """Queries a Parquet file using DuckDB and returns a Polars DataFrame."""
        path = Path(filepath).resolve()

        # We replace the table name placeholder with the absolute file path
        # If the user passed '{filepath}', replace it.
        sql = sql.replace(filepath, str(path))

        return duckdb.sql(sql).pl()
