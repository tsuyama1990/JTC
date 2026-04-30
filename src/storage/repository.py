import logging
from pathlib import Path

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class StorageRepository:
    """Handles persistent storage and querying of quote data."""

    def __init__(self, storage_path: str = "data/processed_quotes.parquet") -> None:
        self.storage_path = Path(storage_path)

    def save_data(self, df: pl.DataFrame) -> None:
        """Saves a Polars DataFrame to a highly compressed Parquet file."""
        # Ensure directory structure exists
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

        # Write parquet file
        df.write_parquet(self.storage_path)
        logger.info(f"Successfully saved data to {self.storage_path}")

    def query_data(self, query: str) -> pl.DataFrame:
        """
        Executes a SQL query against the stored Parquet file using DuckDB.
        Returns the result as a Polars DataFrame.
        """
        if not self.storage_path.exists():
            msg = f"Data file not found at {self.storage_path}"
            raise FileNotFoundError(msg)

        logger.info(f"Executing query: {query}")
        # duckdb.sql(query).pl() retrieves the result directly into a Polars DataFrame natively
        try:
            return duckdb.sql(query).pl()
        except Exception:
            logger.exception("Failed to query data")
            raise
