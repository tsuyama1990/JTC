import os
from pathlib import Path

import duckdb
import polars as pl


class QuotesRepository:
    """Repository for storing and querying processed quotes using Parquet and DuckDB."""

    def __init__(self, data_dir: str = "data") -> None:
        self.data_dir = Path(data_dir)
        self.file_path = self.data_dir / "processed_quotes.parquet"

    def save(self, df: pl.DataFrame) -> None:
        """Saves a Polars DataFrame to a highly compressed Parquet file."""
        # Gracefully create the required directory structures if they do not exist
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Write to highly compressed Parquet
        df.write_parquet(str(self.file_path))

    def query(self, sql_query: str) -> pl.DataFrame:
        """Queries the underlying Parquet file strictly using DuckDB SQL syntax."""
        if not self.file_path.exists():
            msg = f"Data file not found: {self.file_path}"
            raise FileNotFoundError(msg)

        # Connect to an in-memory DuckDB instance
        con = duckdb.connect(database=':memory:')

        cwd_orig = Path.cwd()
        try:
            os.chdir(self.data_dir)
            return con.execute(sql_query).pl()
        finally:
            os.chdir(cwd_orig)
            con.close()
