from datetime import date

import pytest
from pydantic import ValidationError

from src.domain_models.quote import ProcessedQuote, RawQuote


def test_raw_quote_valid() -> None:
    quote = RawQuote(
        date=date(2023, 10, 1),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1000,
    )
    assert quote.high >= quote.low
    assert quote.volume >= 0

def test_raw_quote_invalid_high() -> None:
    with pytest.raises(ValidationError) as exc_info:
        RawQuote(
            date=date(2023, 10, 1),
            open=100.0,
            high=80.0,  # Invalid: less than low, open, close
            low=90.0,
            close=105.0,
            volume=1000,
        )
    assert "high (80.0) cannot be less than low (90.0)" in str(exc_info.value)

def test_processed_quote_valid() -> None:
    quote = ProcessedQuote(
        date=date(2023, 10, 2),  # Monday
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1000,
        day_of_week=1,
        is_month_start=False,
        is_month_end=False,
        daily_return=0.05,
        intraday_return=0.05,
        overnight_return=0.0,
    )
    assert quote.day_of_week == 1

def test_processed_quote_invalid_day() -> None:
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date=date(2023, 10, 1),  # Sunday
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=7,  # Invalid day of week for trading
            is_month_start=True,
            is_month_end=False,
            daily_return=0.0,
            intraday_return=0.05,
            overnight_return=0.0,
        )
