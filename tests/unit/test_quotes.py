import pytest
from pydantic import ValidationError

from src.domain_models.quotes import ProcessedQuote, RawQuote


def test_raw_quote_valid() -> None:
    quote = RawQuote(date="2023-01-01", open=100.0, high=110.0, low=90.0, close=105.0, volume=1000)
    assert quote.date == "2023-01-01"
    assert quote.high == 110.0


def test_raw_quote_invalid_high_price() -> None:
    with pytest.raises(ValidationError) as exc:
        RawQuote(
            date="2023-01-01",
            open=100.0,
            high=95.0,  # Invalid: high < open
            low=90.0,
            close=105.0,
            volume=1000,
        )
    assert "High price must be greater than or equal to low, open, and close prices." in str(
        exc.value
    )


def test_raw_quote_invalid_type() -> None:
    with pytest.raises(ValidationError):
        RawQuote(
            date="2023-01-01",
            open="not-a-float",  # type: ignore
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
        )


def test_processed_quote_valid() -> None:
    quote = ProcessedQuote(
        date="2023-01-02",
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
    assert quote.daily_return == 0.05


def test_processed_quote_invalid_day_of_week() -> None:
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date="2023-01-02",
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=6,  # Invalid: day_of_week must be <= 5
            is_month_start=False,
            is_month_end=False,
            daily_return=0.05,
            intraday_return=0.05,
            overnight_return=0.0,
        )
