from typing import Any

import pytest
from pydantic import ValidationError

from src.domain_models.quote import ProcessedQuote, RawQuote


def test_raw_quote_valid() -> None:
    data: dict[str, Any] = {
        "date": "2023-01-01",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000
    }
    quote = RawQuote(**data)
    assert quote.open == 100.0

def test_raw_quote_invalid_high_low() -> None:
    data: dict[str, Any] = {
        "date": "2023-01-01",
        "open": 100.0,
        "high": 80.0,
        "low": 90.0,
        "close": 85.0,
        "volume": 1000
    }
    with pytest.raises(ValidationError):
        RawQuote(**data)

def test_raw_quote_invalid_type() -> None:
    data: dict[str, Any] = {
        "date": "2023-01-01",
        "open": "string_instead_of_float",
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000
    }
    with pytest.raises(ValidationError):
        RawQuote(**data)

def test_processed_quote_valid() -> None:
    data: dict[str, Any] = {
        "date": "2023-01-02",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000,
        "day_of_week": 1,
        "is_month_start": False,
        "is_month_end": False,
        "daily_return": 0.05,
        "intraday_return": 0.05,
        "overnight_return": 0.0
    }
    quote = ProcessedQuote(**data)
    assert quote.day_of_week == 1

def test_processed_quote_invalid_day_of_week() -> None:
    data: dict[str, Any] = {
        "date": "2023-01-01",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000,
        "day_of_week": 6, # Invalid, > 5
        "is_month_start": False,
        "is_month_end": False,
        "daily_return": 0.05,
        "intraday_return": 0.05,
        "overnight_return": 0.0
    }
    with pytest.raises(ValidationError):
        ProcessedQuote(**data)
