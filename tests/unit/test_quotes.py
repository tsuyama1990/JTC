from datetime import date

import pytest
from pydantic import ValidationError

from src.domain_models.config import AppSettings
from src.domain_models.quotes import ProcessedQuote, RawQuote


def test_raw_quote_valid():
    quote = RawQuote(
        date=date(2023, 1, 1),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1000
    )
    assert quote.open == 100.0
    assert quote.volume == 1000


def test_raw_quote_invalid_high_less_than_low():
    with pytest.raises(ValidationError, match="High price must be greater than or equal to low price."):
        RawQuote(
            date=date(2023, 1, 1),
            open=100.0,
            high=90.0,
            low=110.0,
            close=105.0,
            volume=1000
        )


def test_raw_quote_invalid_high_less_than_open():
    with pytest.raises(ValidationError, match="High price must be greater than or equal to open price."):
        RawQuote(
            date=date(2023, 1, 1),
            open=120.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000
        )


def test_raw_quote_invalid_low_greater_than_close():
    with pytest.raises(ValidationError, match="Low price must be less than or equal to close price."):
        RawQuote(
            date=date(2023, 1, 1),
            open=110.0,
            high=115.0,
            low=106.0,
            close=105.0,
            volume=1000
        )


def test_processed_quote_valid():
    quote = ProcessedQuote(
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
        intraday_return=0.05,
        overnight_return=0.0
    )
    assert quote.day_of_week == 1
    assert quote.is_month_start is True


def test_processed_quote_invalid_day_of_week():
    with pytest.raises(ValidationError):
        ProcessedQuote(
            date=date(2023, 1, 2),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000,
            day_of_week=6,  # Invalid, must be 1-5
            is_month_start=True,
            is_month_end=False,
            daily_return=0.05,
            intraday_return=0.05,
            overnight_return=0.0
        )

def test_app_settings_missing_token(monkeypatch):
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    with pytest.raises(ValidationError):
        AppSettings()

def test_app_settings_valid_token(monkeypatch):
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "valid_token")
    settings = AppSettings()
    assert settings.JQUANTS_REFRESH_TOKEN == "valid_token"
