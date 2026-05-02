from datetime import date
from unittest.mock import MagicMock

import httpx
import pytest
import pytest_mock

from src.jquants_client import APIConnectionError, JQuantsClient


def test_jquants_client_auth_success(mocker: pytest_mock.MockerFixture) -> None:
    mock_post = mocker.patch("httpx.Client.post")

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"idToken": "fake_id_token"}
    mock_response.raise_for_status.return_value = None
    mock_response.request = httpx.Request('POST', 'url')

    mock_post.return_value = mock_response

    client = JQuantsClient("dummy_refresh_token")
    client._authenticate()

    assert client._id_token == "fake_id_token"
    mock_post.assert_called_once()

def test_jquants_client_fetch_success(mocker: pytest_mock.MockerFixture) -> None:
    client = JQuantsClient("dummy_refresh_token")
    client._id_token = "fake_id_token"

    mock_get = mocker.patch("httpx.Client.get")
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "daily_quotes": [
            {
                "Date": "2023-01-01",
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 105.0,
                "Volume": 1000,
            }
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_response.request = httpx.Request('GET', 'url')

    mock_get.return_value = mock_response

    quotes = client.fetch_daily_quotes()

    assert len(quotes) == 1
    assert quotes[0].date == date(2023, 1, 1)
    assert quotes[0].open == 100.0

def test_jquants_client_retry_logic_429(mocker: pytest_mock.MockerFixture) -> None:
    client = JQuantsClient("dummy_refresh_token")
    client._id_token = "fake_id_token"

    mock_get = mocker.patch("httpx.Client.get")

    # Simulate two 429 errors followed by a success
    mock_response_429 = MagicMock(spec=httpx.Response)
    mock_response_429.status_code = 429

    mock_request = httpx.Request('GET', 'url')
    mock_response_429.request = mock_request

    # We must explicitly define a side effect to raise an error for raise_for_status
    def raise_429() -> None:
        msg = "429 Too Many Requests"
        raise httpx.HTTPStatusError(msg, request=mock_request, response=mock_response_429)

    mock_response_429.raise_for_status.side_effect = raise_429

    mock_response_200 = MagicMock(spec=httpx.Response)
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"daily_quotes": []}
    mock_response_200.raise_for_status.return_value = None
    mock_response_200.request = mock_request

    mock_get.side_effect = [mock_response_429, mock_response_429, mock_response_200]

    # Patch time.sleep to avoid actually waiting during tests
    mocker.patch("time.sleep")

    quotes = client.fetch_daily_quotes()

    assert len(quotes) == 0
    assert mock_get.call_count == 3

def test_jquants_client_max_retries_exceeded(mocker: pytest_mock.MockerFixture) -> None:
    client = JQuantsClient("dummy_refresh_token")
    client._id_token = "fake_id_token"

    mock_get = mocker.patch("httpx.Client.get")

    # Simulate persistent 500 errors
    mock_response_500 = MagicMock(spec=httpx.Response)
    mock_response_500.status_code = 500
    mock_request = httpx.Request('GET', 'url')
    mock_response_500.request = mock_request

    def raise_500() -> None:
        msg = "500 Internal Server Error"
        raise httpx.HTTPStatusError(msg, request=mock_request, response=mock_response_500)

    mock_response_500.raise_for_status.side_effect = raise_500

    mock_get.return_value = mock_response_500

    # Patch time.sleep to avoid actually waiting during tests
    mocker.patch("time.sleep")

    with pytest.raises(APIConnectionError) as exc_info:
        client.fetch_daily_quotes()

    assert "Failed to fetch data from J-Quants API after retries" in str(exc_info.value)
    # 5 retries is default for tenacity stop_after_attempt in our impl
    assert mock_get.call_count >= 1
