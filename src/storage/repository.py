from pathlib import Path

import duckdb
import polars as pl


class QuoteRepository:
    def __init__(self, data_dir: str = "data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = self.data_dir / "processed_quotes.parquet"

    def save_processed_quotes(self, df: pl.DataFrame) -> None:
        """Saves a Polars DataFrame to a highly compressed Parquet file."""
        df.write_parquet(self.file_path)

    def query_quotes(self, query: str) -> pl.DataFrame:
        """
        Executes a DuckDB SQL query against the stored Parquet file.
        The Parquet file can be referred to as 'processed_quotes'.
        """
        if not self.file_path.exists():
            return pl.DataFrame()

        # Connect to an in-memory DuckDB instance
        con = duckdb.connect(database=':memory:')

        # We can create a view to make querying easier
        con.execute(f"CREATE VIEW processed_quotes AS SELECT * FROM read_parquet('{self.file_path}')") # noqa: S608

        # Execute query and convert directly to Polars
        return con.execute(query).pl()
