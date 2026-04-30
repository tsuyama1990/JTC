from datetime import UTC, datetime

import polars as pl
import pytest

from src.domain_models.quote import ProcessedQuote, RawQuote
from src.processing.transformers import process_quotes


def test_process_quotes_calculates_returns_and_flags() -> None:
    quotes = [
        RawQuote(
            date=datetime(2023, 10, 31, tzinfo=UTC),  # Tuesday, month end
            open=100.0,
            high=110.0,
            low=90.0,
            close=100.0,
            volume=1000,
        ),
        RawQuote(
            date=datetime(2023, 11, 1, tzinfo=UTC),  # Wednesday, month start
            open=105.0,  # Overnight return from 100 to 105 is +5%
            high=115.0,
            low=100.0,
            close=110.0,  # Daily return from 100 to 110 is +10%
            # Intraday return from 105 to 110 is ~+4.76%
            volume=1200,
        ),
    ]

    df = process_quotes(quotes)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2

    row_tuesday = df.row(0, named=True)
    assert row_tuesday["day_of_week"] == 2
    assert row_tuesday["is_month_start"] is False
    assert row_tuesday["is_month_end"] is True
    assert row_tuesday["daily_return"] is None  # First row has no previous close
    assert row_tuesday["overnight_return"] is None
    assert row_tuesday["intraday_return"] == pytest.approx(0.0)  # (100 - 100) / 100

    row_wednesday = df.row(1, named=True)
    assert row_wednesday["day_of_week"] == 3
    assert row_wednesday["is_month_start"] is True
    assert row_wednesday["is_month_end"] is False
    assert row_wednesday["daily_return"] == pytest.approx(0.1)  # (110 - 100) / 100
    assert row_wednesday["overnight_return"] == pytest.approx(0.05)  # (105 - 100) / 100
    assert row_wednesday["intraday_return"] == pytest.approx((110.0 - 105.0) / 105.0)

    # Validate against ProcessedQuote model
    # Skip first row because returns are None
    processed_quote_data = row_wednesday.copy()
    processed_quote = ProcessedQuote(**processed_quote_data)
    assert processed_quote.day_of_week == 3


def test_process_quotes_empty_input() -> None:
    df = process_quotes([])
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0
