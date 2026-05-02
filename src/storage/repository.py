from pathlib import Path

import duckdb
import polars as pl

from src.core.exceptions import StorageError


class DataRepository:
    def __init__(self, file_path: str | Path) -> None:
        self.file_path = Path(file_path).resolve()

    def save_processed_quotes(self, df: pl.DataFrame) -> None:
        if df.is_empty():
            return

        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(self.file_path)
        except Exception as e:
            msg = f"Failed to save data to {self.file_path}"
            raise StorageError(msg) from e

    def query_quotes(self, query: str) -> pl.DataFrame:
        if not self.file_path.exists():
            msg = f"Parquet file {self.file_path} does not exist"
            raise StorageError(msg)

        try:
            # We explicitly replace a placeholder table name with the absolute file path
            # to allow simple duckdb integration. If the user uses '{table}' we swap it.
            # In a real app we might just run `query` directly if the caller formats it.
            formatted_query = query.replace("{table}", f"'{self.file_path}'")
            return duckdb.sql(formatted_query).pl()
        except Exception as e:
            msg = f"Failed to execute query: {query}"
            raise StorageError(msg) from e
