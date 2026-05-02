from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import polars as pl
import pytest
import pytest_mock
from pydantic import ValidationError

from src.domain_models.config import AppSettings
from src.domain_models.models import RawQuote
from src.jquants_client import APIConnectionError, JQuantsClient
from src.repository import QuoteRepository
from src.transformers import transform_quotes


def test_uat_scenario_missing_token_aborts(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    GIVEN the user has deliberately NOT provided a valid JQUANTS_REFRESH_TOKEN
    WHEN the user attempts to execute the primary data ingestion script
    THEN the system must throw a ValidationError and terminate
    """
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)

    with pytest.raises(ValidationError) as exc_info:
        AppSettings() # type: ignore[call-arg]

    assert "JQUANTS_REFRESH_TOKEN" in str(exc_info.value)

def test_uat_scenario_network_outage_retries(mocker: pytest_mock.MockerFixture) -> None:
    """
    GIVEN the user has configured .env with a valid token
    AND temporarily disables their internet connection
    WHEN the user attempts to execute the script
    THEN the system must engage tenacity retry logic and fail gracefully
    """
    client = JQuantsClient("dummy")
    client._id_token = "dummy"

    mock_get = mocker.patch("httpx.Client.get")
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 500
    mock_request = httpx.Request('GET', 'url')
    mock_response.request = mock_request

    def raise_500() -> None:
        msg = "500 Internal Server Error"
        raise httpx.HTTPStatusError(msg, request=mock_request, response=mock_response)

    mock_response.raise_for_status.side_effect = raise_500
    mock_get.return_value = mock_response

    mocker.patch("time.sleep")

    with pytest.raises(APIConnectionError):
        client.fetch_daily_quotes()

    assert mock_get.call_count >= 1

def test_uat_scenario_transformation_and_storage(tmp_path: Path) -> None:
    """
    GIVEN a valid dataset from J-Quants API
    WHEN the transformation finishes executing
    THEN the resulting dataframe must contain new columns, match expected values, and be perfectly saved to parquet.
    """
    quotes = [
        RawQuote(
            date=date(2023, 1, 3), # Tuesday
            open=100.0,
            high=105.0,
            low=95.0,
            close=100.0,
            volume=1000,
        ),
        RawQuote(
            date=date(2023, 1, 4), # Wednesday
            open=102.0,
            high=115.0,
            low=100.0,
            close=110.0,
            volume=1200,
        )
    ]

    # Transform
    df = transform_quotes(quotes)

    assert "day_of_week" in df.columns
    assert df.filter(pl.col("date") == date(2023, 1, 4))["day_of_week"][0] == 3
    assert "is_month_start" in df.columns
    assert "is_month_end" in df.columns
    assert "daily_return" in df.columns
    assert "intraday_return" in df.columns
    assert "overnight_return" in df.columns

    # Storage
    repo = QuoteRepository()
    file_path = tmp_path / "uat_data.parquet"
    repo.save(df, str(file_path))

    assert file_path.exists()

    # Query
    query_str = f"SELECT * FROM '{file_path}'" # noqa: S608
    queried_df = repo.query(str(file_path), query_str)

    assert queried_df.height == 2
