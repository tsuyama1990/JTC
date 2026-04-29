from datetime import date
from typing import Any

import pandas as pd
import pytest

from src.domain_models import ProcessedQuote
from src.storage.repository import QuotesRepository


def test_repository_save_and_query(tmp_path: Any) -> None:
    file_path = tmp_path / "data" / "processed_quotes.parquet"
    repo = QuotesRepository(str(file_path))

    quotes = [
        ProcessedQuote(
            date=date(2023, 1, 2),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=100,
            day_of_week=1,
            is_month_start=True,
            is_month_end=False,
            daily_return=0.05,
            intraday_return=5.0,
            overnight_return=0.0,
        ),
        ProcessedQuote(
            date=date(2023, 1, 3),
            open=105.0,
            high=115.0,
            low=100.0,
            close=110.0,
            volume=200,
            day_of_week=2,
            is_month_start=False,
            is_month_end=True,
            daily_return=0.047,
            intraday_return=5.0,
            overnight_return=0.0,
        ),
    ]

    repo.save(quotes)

    assert file_path.exists()

    # query
    results = repo.query(f"SELECT * FROM '{file_path}' WHERE day_of_week = 1")  # noqa: S608
    assert len(results) == 1

    # DuckDB/pandas returns Timestamps for date columns, convert to date
    res_date = results[0]["date"]
    if isinstance(res_date, pd.Timestamp):
        res_date = res_date.date()

    assert res_date == date(2023, 1, 2)
    assert results[0]["day_of_week"] == 1


def test_repository_query_not_exists() -> None:
    repo = QuotesRepository("nonexistent.parquet")
    with pytest.raises(FileNotFoundError):
        repo.query("SELECT * FROM 'nonexistent.parquet'")


def test_repository_save_empty(tmp_path: Any) -> None:
    file_path = tmp_path / "data" / "processed_quotes.parquet"
    repo = QuotesRepository(str(file_path))
    repo.save([])
    assert not file_path.exists()
