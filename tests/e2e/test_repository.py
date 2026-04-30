from pathlib import Path

import polars as pl

from src.repository import QuoteRepository


def test_repository_save_and_query(tmp_path: Path) -> None:
    # Setup temporary directory
    data_dir = str(tmp_path / "data")
    repo = QuoteRepository(data_dir=data_dir)

    # Create synthetic Polars DataFrame
    data = {
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "day_of_week": [1, 2, 3],
        "close": [100.0, 105.0, 110.0],
    }
    df = pl.DataFrame(data)

    # Save to Parquet
    repo.save_processed_quotes(df)

    # Verify querying via DuckDB
    query = "SELECT * FROM 'data/processed_quotes.parquet' WHERE day_of_week = 1"
    result_df = repo.query_quotes(query)

    assert len(result_df) == 1
    assert result_df["date"].to_list()[0] == "2024-01-01"
    assert result_df["close"].to_list()[0] == 100.0
