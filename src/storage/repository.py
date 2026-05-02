import logging
from pathlib import Path

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class StorageRepository:
    def save_parquet(self, df: pl.DataFrame, path: str) -> None:
        """Saves a Polars DataFrame as a deeply compressed Parquet file."""
        file_path = Path(path)

        # Ensure directory exists
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Saving dataframe to parquet: {path}")
        # Parquet compression defaults to snappy, zstd is deeply compressed.
        df.write_parquet(path, compression="zstd")

    def query_duckdb(self, query: str) -> pl.DataFrame:
        """Executes a DuckDB SQL query and returns a Polars DataFrame."""
        logger.info(f"Querying DuckDB: {query}")
        return duckdb.sql(query).pl()
