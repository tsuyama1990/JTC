from pathlib import Path

import polars as pl

from src.storage.repository import QuoteRepository


def test_repository_save_and_query(tmp_path: Path) -> None:
    repo = QuoteRepository(data_dir=str(tmp_path))

    data = {
        "date": ["2023-11-01", "2023-11-02", "2023-11-03"],
        "open": [100.0, 105.0, 110.0],
        "high": [110.0, 115.0, 120.0],
        "low": [90.0, 95.0, 100.0],
        "close": [105.0, 110.0, 115.0],
        "volume": [1000, 1100, 1200],
        "day_of_week": [3, 4, 5],
        "is_month_start": [True, False, False],
        "is_month_end": [False, False, False],
        "daily_return": [0.05, 0.0476, 0.0454],
        "intraday_return": [0.05, 0.0476, 0.0454],
        "overnight_return": [0.0, 0.0, 0.0]
    }
    df = pl.DataFrame(data)

    # Save it
    repo.save_processed_quotes(df)

    assert repo.file_path.exists()

    # Query it back
    query = "SELECT * FROM processed_quotes WHERE day_of_week = 3"
    result_df = repo.query_quotes(query)

    assert len(result_df) == 1
    assert result_df["date"][0] == "2023-11-01"

def test_repository_query_empty(tmp_path: Path) -> None:
    repo = QuoteRepository(data_dir=str(tmp_path))
    # query before saving
    query = "SELECT * FROM processed_quotes WHERE day_of_week = 3"
    result_df = repo.query_quotes(query)
    assert len(result_df) == 0
