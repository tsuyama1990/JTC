from datetime import date

import pytest
from pydantic import ValidationError

from src.domain_models import ProcessedQuote, RawQuote


def test_raw_quote_valid() -> None:
    q = RawQuote(date=date(2023, 1, 1), open=100.0, high=110.0, low=90.0, close=105.0, volume=1000)
    assert q.date == date(2023, 1, 1)

def test_raw_quote_invalid_prices() -> None:
    with pytest.raises(ValidationError, match="High price must be >= low price"):
        RawQuote(date=date(2023, 1, 1), open=100.0, high=80.0, low=90.0, close=105.0, volume=1000)

    with pytest.raises(ValidationError, match="High price must be >= open price"):
        RawQuote(date=date(2023, 1, 1), open=120.0, high=110.0, low=90.0, close=105.0, volume=1000)

    with pytest.raises(ValidationError, match="High price must be >= close price"):
        RawQuote(date=date(2023, 1, 1), open=100.0, high=110.0, low=90.0, close=115.0, volume=1000)

def test_processed_quote_valid() -> None:
    q = ProcessedQuote(
        date=date(2023, 1, 2),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1000,
        day_of_week=1,
        is_month_start=True,
        is_month_end=False,
        daily_return=0.05,
        intraday_return=5.0,
        overnight_return=0.0,
    )
    assert q.day_of_week == 1

def test_processed_quote_invalid_day() -> None:
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date=date(2023, 1, 1),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=6,
            is_month_start=True,
            is_month_end=False,
            daily_return=0.05,
            intraday_return=5.0,
            overnight_return=0.0,
        )
