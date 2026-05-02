from datetime import date

import pytest
from pydantic import ValidationError

from src.domain_models.models import ProcessedQuote, RawQuote


def test_raw_quote_valid() -> None:
    quote = RawQuote(
        date=date(2023, 1, 1),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1000,
    )
    assert quote.date == date(2023, 1, 1)
    assert quote.high == 110.0


def test_raw_quote_invalid_high_price() -> None:
    with pytest.raises(ValidationError, match="high price cannot be less than low price"):
        RawQuote(
            date=date(2023, 1, 1),
            open=100.0,
            high=80.0,  # Invalid: high < low
            low=90.0,
            close=105.0,
            volume=1000,
        )

    with pytest.raises(ValidationError, match="high price cannot be less than open price"):
        RawQuote(
            date=date(2023, 1, 1),
            open=120.0,
            high=110.0,  # Invalid: high < open
            low=90.0,
            close=105.0,
            volume=1000,
        )

    with pytest.raises(ValidationError, match="high price cannot be less than close price"):
        RawQuote(
            date=date(2023, 1, 1),
            open=100.0,
            high=110.0,  # Invalid: high < close
            low=90.0,
            close=115.0,
            volume=1000,
        )


def test_processed_quote_valid() -> None:
    quote = ProcessedQuote(
        date=date(2023, 1, 2),
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
    assert not quote.is_month_start


def test_processed_quote_invalid_day_of_week() -> None:
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date=date(2023, 1, 2),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=6,  # Invalid: must be <= 5
            is_month_start=False,
            is_month_end=False,
            daily_return=0.05,
            intraday_return=0.05,
            overnight_return=0.0,
        )
