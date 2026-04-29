from datetime import date
from pathlib import Path
from unittest.mock import Mock

import httpx
import polars as pl
import pytest
from pydantic import ValidationError

from src.clients.jquants_client import APIConnectionError, JQuantsClient
from src.domain_models import AppSettings, RawQuote
from src.processing.transformers import transform_quotes
from src.storage.repository import query_quotes, save_quotes


def test_uat_missing_refresh_token(monkeypatch: pytest.MonkeyPatch) -> None:
    # GIVEN the user has deliberately NOT provided a valid JQUANTS_REFRESH_TOKEN
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)

    # THEN the highly strict Pydantic AppSettings configuration model must immediately intercept
    # AND instantaneously throw a ValidationError
    with pytest.raises(ValidationError):
        AppSettings()  # type: ignore[call-arg]


def test_uat_network_outage_retry(mocker: Mock) -> None:
    # GIVEN the user has successfully configured a valid J-Quants API refresh token
    client = JQuantsClient("valid_token")

    # AND intentionally disables local internet (simulated via mock raising error)
    mock_get = mocker.patch("httpx.get")
    mock_response = httpx.Response(
        500, request=httpx.Request("GET", "https://api.jquants.com/v1/quotes/daily")
    )
    mock_get.side_effect = httpx.HTTPStatusError(
        "Internal Server Error", request=mock_response.request, response=mock_response
    )

    mock_post = mocker.patch("httpx.post")
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"idToken": "fake"}

    # WHEN the user attempts to execute the data ingestion
    # THEN the robust client engages retry logic and eventually fails gracefully
    with pytest.raises(APIConnectionError):
        client.fetch_quotes("1234")

    # Assert retry happened
    assert mock_get.call_count > 1


def test_uat_transformations() -> None:
    # GIVEN a raw dataset
    quotes = [
        RawQuote(
            date=date(2023, 1, 31),
            code="1234",
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000.0,
        ),
        RawQuote(
            date=date(2023, 2, 1),  # Wed
            code="1234",
            open=106.0,
            high=115.0,
            low=102.0,
            close=110.0,
            volume=1200.0,
        ),
    ]

    # WHEN transformed
    df = transform_quotes(quotes)

    # THEN valid polars df
    assert isinstance(df, pl.DataFrame)

    # Original columns present
    assert "date" in df.columns
    assert "code" in df.columns

    # New columns present
    assert "day_of_week" in df.columns
    assert "is_month_start" in df.columns
    assert "is_month_end" in df.columns
    assert "daily_return" in df.columns
    assert "intraday_return" in df.columns
    assert "overnight_return" in df.columns

    # Types and bounds
    assert df.select("day_of_week").to_series().dtype in [pl.Int8, pl.Int32, pl.Int64]
    assert df.select("is_month_start").to_series().dtype == pl.Boolean
    assert df.select("daily_return").to_series().dtype == pl.Float64


def test_uat_storage_repository(tmp_path: Path) -> None:
    # GIVEN a completely enriched Polars DataFrame
    df = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "code": ["1234"],
            "open": [100.0],
            "high": [110.0],
            "low": [90.0],
            "close": [105.0],
            "volume": [1000.0],
            "day_of_week": [1],
            "is_month_start": [False],
            "is_month_end": [False],
            "daily_return": [0.05],
            "intraday_return": [0.05],
            "overnight_return": [0.0],
        }
    )

    file_path = tmp_path / "data" / "processed_quotes.parquet"

    # WHEN commanded to persist
    save_quotes(df, str(file_path))

    # THEN the file is written exactly as compressed Parquet
    assert file_path.exists()

    # AND DuckDB can query it
    result = query_quotes(str(file_path), f"SELECT * FROM '{file_path}'")
    assert len(result) == 1
