from datetime import date

import pytest
from pydantic import ValidationError

from src.domain_models.config import AppSettings
from src.domain_models.models import ProcessedQuote, RawQuote


def test_app_settings_validation_missing_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    with pytest.raises(ValidationError):
        AppSettings() # type: ignore[call-arg]

def test_app_settings_validation_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "dummy_token")
    settings = AppSettings() # type: ignore[call-arg]
    assert settings.JQUANTS_REFRESH_TOKEN == "dummy_token"

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
    assert quote.open == 100.0

def test_raw_quote_invalid_high_low() -> None:
    with pytest.raises(ValidationError) as exc_info:
        RawQuote(
            date=date(2023, 1, 1),
            open=100.0,
            high=80.0,
            low=90.0,
            close=105.0,
            volume=1000,
        )
    assert "High price cannot be strictly less than low price" in str(exc_info.value)

def test_raw_quote_invalid_high_open() -> None:
    with pytest.raises(ValidationError) as exc_info:
        RawQuote(
            date=date(2023, 1, 1),
            open=120.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
        )
    assert "High price cannot be strictly less than open price" in str(exc_info.value)

def test_raw_quote_invalid_high_close() -> None:
    with pytest.raises(ValidationError) as exc_info:
        RawQuote(
            date=date(2023, 1, 1),
            open=100.0,
            high=110.0,
            low=90.0,
            close=115.0,
            volume=1000,
        )
    assert "High price cannot be strictly less than close price" in str(exc_info.value)

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
        overnight_return=0.0,
    )
    assert quote.day_of_week == 1
    assert quote.daily_return == 0.05
