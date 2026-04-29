from unittest.mock import Mock

import httpx
import pytest

from src.clients.jquants_client import APIConnectionError, JQuantsClient


def test_jquants_client_success(mocker: Mock) -> None:
    mock_post = mocker.patch("httpx.post")
    mock_get = mocker.patch("httpx.get")

    # Mock refresh token response
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"idToken": "fake_id_token"}
    mock_post.return_value.raise_for_status = Mock()

    # Mock daily quotes response
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "daily_quotes": [
            {
                "Date": "2023-01-01",
                "Code": "1234",
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 105.0,
                "Volume": 1000.0,
            }
        ]
    }
    mock_get.return_value.raise_for_status = Mock()

    client = JQuantsClient(refresh_token="fake_refresh_token")
    quotes = client.fetch_quotes("1234")

    assert len(quotes) == 1
    assert quotes[0].code == "1234"
    assert mock_post.called
    assert mock_get.called


def test_jquants_client_retry_and_fail(mocker: Mock) -> None:
    mock_post = mocker.patch("httpx.post")
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"idToken": "fake_id_token"}
    mock_post.return_value.raise_for_status = Mock()

    mock_get = mocker.patch("httpx.get")
    # Simulate a network failure or 500 error consistently
    mock_response = httpx.Response(
        500, request=httpx.Request("GET", "https://api.jquants.com/v1/quotes/daily")
    )
    mock_get.side_effect = httpx.HTTPStatusError(
        "Internal Server Error", request=mock_response.request, response=mock_response
    )

    client = JQuantsClient(refresh_token="fake_refresh_token")

    with pytest.raises(APIConnectionError):
        client.fetch_quotes("1234")

    # It should have retried multiple times before failing
    assert mock_get.call_count > 1
