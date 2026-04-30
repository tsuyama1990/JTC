from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.domain_models import ProcessedQuote, RawQuote


def test_raw_quote_valid() -> None:
    quote = RawQuote(
        date=datetime(2023, 1, 1, tzinfo=UTC),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1000,
    )
    assert quote.high >= quote.low


def test_raw_quote_invalid_high_low() -> None:
    with pytest.raises(ValidationError):
        RawQuote(
            date=datetime(2023, 1, 1, tzinfo=UTC),
            open=100.0,
            high=80.0,
            low=90.0,
            close=105.0,
            volume=1000,
        )


def test_raw_quote_invalid_high_open() -> None:
    with pytest.raises(ValidationError):
        RawQuote(
            date=datetime(2023, 1, 1, tzinfo=UTC),
            open=120.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
        )


def test_raw_quote_invalid_high_close() -> None:
    with pytest.raises(ValidationError):
        RawQuote(
            date=datetime(2023, 1, 1, tzinfo=UTC),
            open=100.0,
            high=110.0,
            low=90.0,
            close=115.0,
            volume=1000,
        )


def test_raw_quote_extra_fields() -> None:
    with pytest.raises(ValidationError):
        RawQuote(
            date=datetime(2023, 1, 1, tzinfo=UTC),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            extra_field="should fail",  # type: ignore[call-arg]
        )


def test_processed_quote_valid() -> None:
    quote = ProcessedQuote(
        date=datetime(2023, 1, 2, tzinfo=UTC),  # Monday
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


def test_processed_quote_invalid_day_of_week() -> None:
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date=datetime(2023, 1, 1, tzinfo=UTC),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=0,  # Invalid
            is_month_start=False,
            is_month_end=False,
            daily_return=0.0,
            intraday_return=0.0,
            overnight_return=0.0,
        )
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date=datetime(2023, 1, 2, tzinfo=UTC),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=6,  # Invalid
            is_month_start=False,
            is_month_end=False,
            daily_return=0.0,
            intraday_return=0.0,
            overnight_return=0.0,
        )
