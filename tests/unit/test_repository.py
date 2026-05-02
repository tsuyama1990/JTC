from datetime import date
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

from src.repository import QuoteRepository


def test_repository_save_and_query_parquet(tmp_path: Path) -> None:
    # Prepare dummy data
    data = {
        "date": [date(2023, 1, 1), date(2023, 1, 2)],
        "open": [100.0, 102.0],
        "high": [110.0, 115.0],
        "low": [90.0, 100.0],
        "close": [105.0, 110.0],
        "volume": [1000, 1200],
        "day_of_week": [1, 2],
        "is_month_start": [True, False],
        "is_month_end": [False, False],
        "daily_return": [None, 0.0476],
        "intraday_return": [0.05, 0.0784],
        "overnight_return": [None, -0.0285],
    }
    df = pl.DataFrame(data)

    repo = QuoteRepository()
    file_path = tmp_path / "test_data.parquet"

    # Save to parquet
    repo.save(df, str(file_path))

    assert file_path.exists()

    # Query back
    # Disable S608 because we construct a safe query for testing duckdb
    query_str = f"SELECT * FROM '{file_path}' WHERE day_of_week = 1" # noqa: S608
    queried_df = repo.query(str(file_path), query_str)

    expected_df = df.filter(pl.col("day_of_week") == 1)

    assert_frame_equal(queried_df, expected_df)

def test_repository_creates_directory_if_not_exists(tmp_path: Path) -> None:
    data = {
        "date": [date(2023, 1, 1)],
        "close": [100.0]
    }
    df = pl.DataFrame(data)

    repo = QuoteRepository()

    nested_path = tmp_path / "deep" / "nested" / "dir" / "test.parquet"

    repo.save(df, str(nested_path))

    assert nested_path.exists()
