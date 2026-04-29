from pathlib import Path

import polars as pl
from repository import QuotesRepository


def test_repository_save_and_query(tmp_path: Path) -> None:
    # 1. Create a synthetic Polars DataFrame
    df = pl.DataFrame(
        {
            "date": ["2023-10-31", "2023-11-01"],
            "open": [100.0, 105.0],
            "high": [110.0, 115.0],
            "low": [90.0, 100.0],
            "close": [105.0, 110.0],
            "volume": [1000.0, 1200.0],
            "daily_return": [None, 0.0476],
            "intraday_return": [0.05, 0.0476],
            "overnight_return": [None, 0.0],
            "day_of_week": [2, 3],
            "is_month_start": [False, True],
            "is_month_end": [True, False],
        }
    )

    # Ensure directory doesn't exist yet to test auto-creation
    repo_dir = tmp_path / "data"
    repo = QuotesRepository(data_dir=str(repo_dir))

    # 2. Save using repository
    repo.save(df)

    # 3. Query back using repository (DuckDB integration)
    retrieved_df = repo.query("SELECT * FROM 'processed_quotes.parquet' WHERE day_of_week = 3")

    assert isinstance(retrieved_df, pl.DataFrame)
    assert len(retrieved_df) == 1
    assert retrieved_df["day_of_week"][0] == 3
    assert retrieved_df["is_month_start"][0] is True
