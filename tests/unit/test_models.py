import pytest
from pydantic import ValidationError

from domain_models import AppSettings, ProcessedQuote, RawQuote


def test_app_settings_missing_token() -> None:
    with pytest.raises(ValidationError):
        AppSettings(_env_file=None)


def test_app_settings_valid_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "valid_token")
    settings = AppSettings(_env_file=None)
    assert settings.JQUANTS_REFRESH_TOKEN == "valid_token"


def test_raw_quote_valid() -> None:
    data = {
        "date": "2023-10-25",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000.0,
    }
    quote = RawQuote(**data)
    assert quote.date == "2023-10-25"
    assert quote.high == 110.0


def test_raw_quote_invalid_high_low() -> None:
    data = {
        "date": "2023-10-25",
        "open": 100.0,
        "high": 90.0,  # High < Low
        "low": 110.0,
        "close": 105.0,
        "volume": 1000.0,
    }
    with pytest.raises(
        ValidationError, match="high price must be greater than or equal to low price"
    ):
        RawQuote(**data)


def test_raw_quote_extra_fields() -> None:
    data = {
        "date": "2023-10-25",
        "extra_field": "should not be here",
    }
    with pytest.raises(ValidationError):
        RawQuote(**data)


def test_raw_quote_missing_required_date() -> None:
    with pytest.raises(ValidationError):
        RawQuote()


def test_raw_quote_invalid_types() -> None:
    data = {
        "date": "2023-10-25",
        "open": "string_not_float",
    }
    with pytest.raises(ValidationError):
        RawQuote(**data)


def test_processed_quote_valid() -> None:
    data = {
        "date": "2023-10-25",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000.0,
        "daily_return": 0.05,
        "intraday_return": 0.05,
        "overnight_return": 0.0,
        "day_of_week": 3,
        "is_month_start": False,
        "is_month_end": False,
    }
    quote = ProcessedQuote(**data)
    assert quote.day_of_week == 3


def test_processed_quote_invalid_day_of_week() -> None:
    data = {
        "date": "2023-10-25",
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 1000.0,
        "daily_return": 0.05,
        "intraday_return": 0.05,
        "overnight_return": 0.0,
        "day_of_week": 6,  # 6 is invalid
        "is_month_start": False,
        "is_month_end": False,
    }
    with pytest.raises(ValidationError):
        ProcessedQuote(**data)
