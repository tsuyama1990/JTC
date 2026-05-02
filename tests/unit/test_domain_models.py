from datetime import date

import pytest
from pydantic import ValidationError

from src.domain_models.processed_quote import ProcessedQuote
from src.domain_models.raw_quote import RawQuote


def test_raw_quote_valid() -> None:
    quote = RawQuote(
        date=date(2023, 1, 1),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1000
    )
    assert quote.open == 100.0

def test_raw_quote_invalid_high() -> None:
    with pytest.raises(ValidationError):
        RawQuote(
            date=date(2023, 1, 1),
            open=100.0,
            high=95.0,  # Invalid: high < open
            low=90.0,
            close=105.0,
            volume=1000
        )

def test_processed_quote_valid() -> None:
    quote = ProcessedQuote(
        date=date(2023, 1, 2),  # Monday
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
        overnight_return=0.0
    )
    assert quote.day_of_week == 1

def test_processed_quote_invalid_day() -> None:
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date=date(2023, 1, 1), # Sunday
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=7, # type: ignore[arg-type]
            is_month_start=False,
            is_month_end=False,
            daily_return=0.05,
            intraday_return=0.05,
            overnight_return=0.0
        )
