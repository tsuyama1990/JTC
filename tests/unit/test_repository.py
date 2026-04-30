from pathlib import Path

import polars as pl

from src.storage.repository import StorageRepository


def test_save_and_query_parquet(tmp_path: Path):
    # Setup temporary file path
    data_dir = tmp_path / "data"
    parquet_path = data_dir / "processed_quotes.parquet"

    repo = StorageRepository(storage_path=str(parquet_path))

    # Synthetic DataFrame matching ProcessedQuote roughly
    df = pl.DataFrame({
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "day_of_week": [1, 2, 3],
        "close": [100.0, 105.0, 102.0],
    })

    # Save data
    repo.save_data(df)

    # Verify file exists
    assert parquet_path.exists()

    # Query data via DuckDB
    # Simple query
    queried_df = repo.query_data(f"SELECT * FROM '{parquet_path}'")  # noqa: S608
    assert len(queried_df) == 3

    # Filter query
    filtered_df = repo.query_data(f"SELECT * FROM '{parquet_path}' WHERE day_of_week = 1")  # noqa: S608
    assert len(filtered_df) == 1
    assert filtered_df["close"][0] == 100.0
