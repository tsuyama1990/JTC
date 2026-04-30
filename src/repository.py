from pathlib import Path

import duckdb
import polars as pl


class QuoteRepository:
    def __init__(self, data_dir: str = "data") -> None:
        self.data_dir = Path(data_dir)
        self.file_path = self.data_dir / "processed_quotes.parquet"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def save_processed_quotes(self, df: pl.DataFrame) -> None:
        """Saves the Polars DataFrame to a Parquet file."""
        df.write_parquet(str(self.file_path))

    def query_quotes(self, query: str) -> pl.DataFrame:
        """
        Executes a DuckDB SQL query against the stored Parquet file.
        The query should use 'data/processed_quotes.parquet' as the table name,
        which will be replaced by the actual path if customized, but standard duckdb
        allows selecting directly from parquet file strings.
        We will inject the actual path for safety.
        """
        # If the user provides a standard string, replace it to support custom test paths
        safe_query = query.replace("'data/processed_quotes.parquet'", f"'{self.file_path}'")

        # duckdb.sql(...).pl() returns a Polars DataFrame
        return duckdb.sql(safe_query).pl()
