import os

import httpx
import polars as pl
import pytest
from httpx import Response

from src.ingestion.jquants_client import APIConnectionError, JQuantsClient
from src.storage.repository import StorageRepository


from typing import Any
def test_jquants_client_success(mocker: Any) -> None:
    mock_post = mocker.patch("httpx.Client.post")
    mock_get = mocker.patch("httpx.Client.get")

    # Mock /token/auth_refresh
    mock_post.return_value = Response(
        200, request=httpx.Request("POST", "url"), json={"idToken": "fake_id_token"}
    )

    # Mock /quotes/prices
    mock_get.return_value = Response(
        200,
        request=httpx.Request("POST", "url"),
        json={
            "quotes": [
                {
                    "Date": "2023-01-01",
                    "Open": 100.0,
                    "High": 110.0,
                    "Low": 90.0,
                    "Close": 105.0,
                    "Volume": 1000,
                }
            ]
        },
    )

    client = JQuantsClient("fake_refresh_token")
    quotes = client.fetch_historical_quotes()

    assert len(quotes) == 1
    assert quotes[0].date == "2023-01-01"
    assert quotes[0].high == 110.0


def test_jquants_client_retry_429_then_success(mocker: Any) -> None:
    mock_post = mocker.patch("httpx.Client.post")
    mock_get = mocker.patch("httpx.Client.get")

    mock_post.return_value = Response(
        200, request=httpx.Request("POST", "url"), json={"idToken": "fake_id_token"}
    )

    # Fail with 429 once, then succeed
    response_429 = Response(
        429, request=httpx.Request("GET", "url"), json={"message": "Too Many Requests"}
    )
    response_200 = Response(200, request=httpx.Request("POST", "url"), json={"quotes": []})
    mock_get.side_effect = [response_429, response_200]

    client = JQuantsClient("fake_refresh_token")
    quotes = client.fetch_historical_quotes()

    assert len(quotes) == 0
    assert mock_get.call_count == 2


def test_jquants_client_retry_500_exhaustion(mocker: Any) -> None:
    mock_post = mocker.patch("httpx.Client.post")
    mock_get = mocker.patch("httpx.Client.get")

    mock_post.return_value = Response(
        200, request=httpx.Request("POST", "url"), json={"idToken": "fake_id_token"}
    )

    # Fail consistently with 500
    mock_get.return_value = Response(
        500, request=httpx.Request("GET", "url"), json={"message": "Internal Server Error"}
    )

    client = JQuantsClient("fake_refresh_token")

    with pytest.raises(APIConnectionError):
        client.fetch_historical_quotes()


from pathlib import Path
def test_storage_repository(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "date": ["2023-01-01", "2023-01-02"],
            "open": [100.0, 105.0],
            "high": [110.0, 115.0],
            "low": [90.0, 95.0],
            "close": [105.0, 110.0],
            "volume": [1000, 1200],
            "day_of_week": [1, 2],
            "is_month_start": [True, False],
            "is_month_end": [False, False],
            "daily_return": [0.0, 0.05],
            "intraday_return": [5.0, 5.0],
            "overnight_return": [0.0, 0.0],
        }
    )

    file_path = str(tmp_path / "test_quotes.parquet")
    repo = StorageRepository()

    # Write to parquet
    repo.save_parquet(df, file_path)

    # Read via duckdb
    query = f"SELECT * FROM '{file_path}' WHERE day_of_week = 1"  # noqa: S608
    result_df = repo.query_duckdb(query)

    assert len(result_df) == 1
    assert result_df["date"][0] == "2023-01-01"


@pytest.mark.live
def test_live_jquants_api() -> None:
    refresh_token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if not refresh_token:
        pytest.skip("JQUANTS_REFRESH_TOKEN not set")

    client = JQuantsClient(refresh_token)
    quotes = client.fetch_historical_quotes()
    assert len(quotes) > 0
