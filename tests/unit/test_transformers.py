from datetime import date

import polars as pl
import pytest

from src.domain_models.quote import RawQuote
from src.processing.transformers import transform_quotes


def test_transform_quotes_empty() -> None:
    df = transform_quotes([])
    assert len(df) == 0

def test_transform_quotes_logic() -> None:
    quotes = [
        RawQuote(date="2023-10-31", open=100.0, high=105.0, low=95.0, close=100.0, volume=1000),
        RawQuote(date="2023-11-01", open=102.0, high=110.0, low=100.0, close=105.0, volume=1100),
        RawQuote(date="2023-11-02", open=104.0, high=115.0, low=102.0, close=110.0, volume=1200)
    ]
    df = transform_quotes(quotes)

    assert len(df) == 3

    row_1 = df.filter(pl.col("date") == date(2023, 11, 1)).to_dicts()[0]
    assert row_1["daily_return"] == pytest.approx(0.05)
    assert row_1["intraday_return"] == pytest.approx((105.0 - 102.0) / 102.0)
    assert row_1["overnight_return"] == pytest.approx(0.02)

    row_0 = df.filter(pl.col("date") == date(2023, 10, 31)).to_dicts()[0]
    assert row_0["day_of_week"] == 2
    assert row_0["is_month_start"] is False
    assert row_0["is_month_end"] is True

    assert row_1["day_of_week"] == 3
    assert row_1["is_month_start"] is True
    assert row_1["is_month_end"] is False
