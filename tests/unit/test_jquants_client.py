from typing import Any

import pytest
from httpx import Request, Response

from src.core.config import AppSettings
from src.core.exceptions import IngestionError
from src.ingestion.jquants_client import JQuantsClient


@pytest.fixture
def mock_settings() -> AppSettings:
    return AppSettings(JQUANTS_REFRESH_TOKEN="test_token")

def test_jquants_auth_success(mocker: Any, mock_settings: AppSettings) -> None:
    client = JQuantsClient(mock_settings)
    mock_post = mocker.patch.object(client.client, "post")

    mock_response = Response(200, json={"idToken": "fake_id_token"}, request=Request("POST", "url"))
    mock_post.return_value = mock_response

    client._get_id_token()
    assert client.id_token == "fake_id_token"

def test_jquants_auth_failure(mocker: Any, mock_settings: AppSettings) -> None:
    client = JQuantsClient(mock_settings)
    mock_post = mocker.patch.object(client.client, "post")

    mock_response = Response(401, request=Request("POST", "url"))
    mock_post.return_value = mock_response

    with pytest.raises(IngestionError, match="Failed to get idToken: 401"):
        client._get_id_token()

def test_jquants_fetch_quotes_success(mocker: Any, mock_settings: AppSettings) -> None:
    client = JQuantsClient(mock_settings)
    client.id_token = "fake_id_token"

    mock_get = mocker.patch.object(client.client, "get")
    mock_response = Response(
        200,
        json={
            "daily_quotes": [
                {
                    "Date": "2023-01-01",
                    "Open": 100.0,
                    "High": 110.0,
                    "Low": 90.0,
                    "Close": 105.0,
                    "Volume": 1000
                }
            ]
        },
        request=Request("GET", "url")
    )
    mock_get.return_value = mock_response

    quotes = client.fetch_historical_quotes()
    assert len(quotes) == 1
    assert quotes[0].open == 100.0

def test_jquants_retry_on_429(mocker: Any, mock_settings: AppSettings) -> None:
    client = JQuantsClient(mock_settings)
    client.id_token = "fake_id_token"

    mock_get = mocker.patch.object(client.client, "get")

    # Fail 2 times with 429, succeed on 3rd
    mock_get.side_effect = [
        Response(429, request=Request("GET", "url")),
        Response(429, request=Request("GET", "url")),
        Response(
            200,
            json={
                "daily_quotes": [{"Date": "2023-01-01", "Open": 100, "High": 100, "Low": 100, "Close": 100, "Volume": 100}]
            },
            request=Request("GET", "url")
        )
    ]

    quotes = client.fetch_historical_quotes()
    assert len(quotes) == 1
    assert mock_get.call_count == 3
