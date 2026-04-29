from pathlib import Path

import polars as pl

from src.repository import query_quotes, save_quotes


def test_repository_save_and_query(tmp_path: Path) -> None:
    # Create synthetic DataFrame
    df = pl.DataFrame(
        {
            "date": ["2023-10-01", "2023-10-02"],
            "open": [100.0, 105.0],
            "high": [110.0, 115.0],
            "low": [90.0, 95.0],
            "close": [105.0, 110.0],
            "volume": [1000, 1500],
            "day_of_week": [1, 2],
            "is_month_start": [True, False],
            "is_month_end": [False, False],
            "daily_return": [None, 0.0476],
            "intraday_return": [5.0, 5.0],
            "overnight_return": [None, 0.0],
        }
    )

    file_path = tmp_path / "processed_quotes.parquet"

    # Save
    save_quotes(df, str(file_path))

    assert file_path.exists()

    # Query with DuckDB
    result = query_quotes(str(file_path), "SELECT * FROM data WHERE day_of_week = 1")

    assert len(result) == 1
    assert result[0]["day_of_week"] == 1
