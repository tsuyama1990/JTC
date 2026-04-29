from datetime import date

import pytest
from pydantic import ValidationError

from src.domain_models import ProcessedQuote, RawQuote


def test_raw_quote_valid() -> None:
    data = {
        "date": date(2023, 1, 1),
        "code": "1234",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000.0,
    }
    quote = RawQuote(**data)  # type: ignore[arg-type]
    assert quote.code == "1234"
    assert quote.high == 110.0


def test_raw_quote_invalid_high_low() -> None:
    data = {
        "date": date(2023, 1, 1),
        "code": "1234",
        "open": 100.0,
        "high": 90.0,
        "low": 110.0,  # Invalid: low > high
        "close": 105.0,
        "volume": 1000.0,
    }
    with pytest.raises(ValidationError) as exc_info:
        RawQuote(**data)  # type: ignore[arg-type]
    assert "high price must be greater than or equal to low price" in str(exc_info.value)


def test_raw_quote_forbid_extra() -> None:
    data = {
        "date": date(2023, 1, 1),
        "code": "1234",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000.0,
        "extra_field": "not allowed",
    }
    with pytest.raises(ValidationError):
        RawQuote(**data)  # type: ignore[arg-type]


def test_processed_quote_valid() -> None:
    data = {
        "date": date(2023, 1, 2),  # Monday
        "code": "1234",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000.0,
        "day_of_week": 1,
        "is_month_start": False,
        "is_month_end": False,
        "daily_return": 0.05,
        "intraday_return": 0.05,
        "overnight_return": 0.0,
    }
    quote = ProcessedQuote(**data)  # type: ignore[arg-type]
    assert quote.day_of_week == 1


def test_processed_quote_invalid_day_of_week() -> None:
    data = {
        "date": date(2023, 1, 1),  # Sunday
        "code": "1234",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000.0,
        "day_of_week": 7,  # Invalid day of week
        "is_month_start": False,
        "is_month_end": False,
        "daily_return": 0.0,
        "intraday_return": 0.0,
        "overnight_return": 0.0,
    }
    with pytest.raises(ValidationError):
        ProcessedQuote(**data)  # type: ignore[arg-type]
