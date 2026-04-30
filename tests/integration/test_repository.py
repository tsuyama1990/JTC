from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from src.storage.repository import StorageRepository


def test_repository_save_and_query(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "date": [datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 1, 2, tzinfo=UTC)],
            "day_of_week": [1, 2],
            "close": [100.0, 105.0],
        }
    )

    file_path = tmp_path / "test_quotes.parquet"
    repo = StorageRepository(str(file_path))

    repo.save(df)

    assert file_path.exists()

    # Query with duckdb through repo
    result_df = repo.query("SELECT * FROM 'data' WHERE day_of_week = 1")

    assert len(result_df) == 1
    assert result_df["close"][0] == 100.0
