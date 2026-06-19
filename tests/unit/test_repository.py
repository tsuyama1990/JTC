from pathlib import Path

import polars as pl

from src.storage.repository import query_quotes, save_quotes


def test_save_and_query_quotes(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "code": ["1234"],
            "open": [100.0],
            "high": [110.0],
            "low": [90.0],
            "close": [105.0],
            "volume": [1000.0],
            "day_of_week": [1],
            "is_month_start": [False],
            "is_month_end": [False],
            "daily_return": [0.05],
            "intraday_return": [0.05],
            "overnight_return": [0.0],
        }
    )

    file_path = tmp_path / "processed_quotes.parquet"
    save_quotes(df, str(file_path))

    assert file_path.exists()

    result_df = query_quotes(str(file_path), f"SELECT * FROM '{file_path}' WHERE day_of_week = 1")

    assert isinstance(result_df, pl.DataFrame)
    assert len(result_df) == 1
    assert result_df.select("code").to_series()[0] == "1234"
