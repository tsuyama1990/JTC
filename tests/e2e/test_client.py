from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from src.domain_models import APIConnectionError
from src.ingestion.jquants_client import JQuantsClient


def test_client_authentication_success(mocker: Any) -> None:
    client = JQuantsClient("fake_refresh_token")

    mock_post = mocker.patch("requests.post")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"idToken": "fake_id_token"}
    mock_post.return_value = mock_response

    client._authenticate()

    assert client.id_token == "fake_id_token"  # noqa: S105


def test_client_authentication_failure(mocker: Any) -> None:
    client = JQuantsClient("fake_refresh_token")

    mock_post = mocker.patch("requests.post")
    mock_post.side_effect = requests.exceptions.RequestException("Network error")

    with pytest.raises(APIConnectionError, match="Failed to authenticate"):
        client._authenticate()


def test_client_fetch_quotes_success(mocker: Any) -> None:
    client = JQuantsClient("fake_refresh_token")
    client.id_token = "fake_id_token"  # noqa: S105

    mock_get = mocker.patch("requests.get")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = [
        {
            "daily_quotes": [
                {
                    "Date": "2023-01-01",
                    "Open": 100.0,
                    "High": 110.0,
                    "Low": 90.0,
                    "Close": 105.0,
                    "Volume": 1000,
                }
            ],
            "pagination_key": "page2",
        },
        {
            "daily_quotes": [
                {
                    "Date": "2023-01-02",
                    "Open": 105.0,
                    "High": 115.0,
                    "Low": 95.0,
                    "Close": 110.0,
                    "Volume": 2000,
                }
            ]
            # no pagination_key, ends loop
        },
    ]
    mock_get.return_value = mock_response

    quotes = client.fetch_historical_quotes_12_weeks()

    assert len(quotes) == 2
    assert quotes[0].date == date(2023, 1, 1)
    assert quotes[1].date == date(2023, 1, 2)
    assert mock_get.call_count == 2
