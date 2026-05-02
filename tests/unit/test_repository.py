from datetime import date
from pathlib import Path

import polars as pl
from src.repository.repository import load_quotes, query_quotes, save_quotes


def test_repository_save_and_query(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "date": [date(2023, 1, 31), date(2023, 2, 1)],
            "open": [100.0, 105.0],
            "high": [110.0, 115.0],
            "low": [90.0, 95.0],
            "close": [105.0, 110.0],
            "volume": [1000, 1200],
            "day_of_week": [2, 3],
            "is_month_start": [False, True],
            "is_month_end": [True, False],
            "daily_return": [0.0, 0.047619],
            "intraday_return": [0.05, 0.047619],
            "overnight_return": [0.0, 0.0],
        }
    )

    file_path = tmp_path / "processed_quotes.parquet"

    # Test saving
    save_quotes(df, str(file_path))
    assert file_path.exists()

    # Test loading
    loaded_df = load_quotes(str(file_path))
    from polars.testing import assert_frame_equal
    assert_frame_equal(df, loaded_df)

    # Test DuckDB query
    result_df = query_quotes(str(file_path), "SELECT * FROM data WHERE day_of_week = 3")
    assert result_df.height == 1
    assert result_df["date"][0] == date(2023, 2, 1)
