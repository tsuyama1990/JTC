import pathlib
from typing import Any

import duckdb
import polars as pl

from src.domain_models import ProcessedQuote


class QuotesRepository:
    """
    Handles permanent persistence of ProcessedQuote objects into Parquet files
    and provides a DuckDB interface for querying them.
    """

    def __init__(self, file_path: str = "data/processed_quotes.parquet") -> None:
        self.file_path = pathlib.Path(file_path)

    def save(self, quotes: list[ProcessedQuote]) -> None:
        """Saves a list of ProcessedQuote to a highly compressed Parquet file."""
        if not quotes:
            return

        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        quotes_dicts = [q.model_dump() for q in quotes]
        df = pl.DataFrame(quotes_dicts)

        df.write_parquet(self.file_path)

    def query(self, sql_query: str) -> list[dict[str, Any]]:
        """
        Executes a SQL query against the stored Parquet file using DuckDB.
        Example: SELECT * FROM 'data/processed_quotes.parquet' WHERE day_of_week = 1
        """
        if not self.file_path.exists():
            err_msg = f"Parquet file {self.file_path} does not exist."
            raise FileNotFoundError(err_msg)

        with duckdb.connect(database=":memory:") as conn:
            result = conn.execute(sql_query).df()

        return result.to_dict(orient="records") # type: ignore[no-any-return]
