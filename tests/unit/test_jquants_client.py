from datetime import UTC, datetime
from unittest.mock import MagicMock

import httpx
import pytest

from src.core.exceptions import APIConnectionError
from src.ingestion.jquants_client import JQuantsClient


@pytest.fixture
def mock_httpx_post(mocker):
    return mocker.patch("httpx.post")

@pytest.fixture
def mock_httpx_get(mocker):
    return mocker.patch("httpx.get")

def test_jquants_client_auth_success(mock_httpx_post):
    # Mock the authentication response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"idToken": "fake_id_token"}
    mock_httpx_post.return_value = mock_response

    client = JQuantsClient(refresh_token="fake_refresh_token")
    token = client.get_id_token()

    assert token == "fake_id_token"
    mock_httpx_post.assert_called_once_with(
        "https://api.jquants.com/v1/token/auth_refresh",
        params={"refresh_token": "fake_refresh_token"}
    )

def test_jquants_client_fetch_historical_data_success(mock_httpx_post, mock_httpx_get, mocker):
    # Mock auth
    mock_auth_response = MagicMock()
    mock_auth_response.status_code = 200
    mock_auth_response.json.return_value = {"idToken": "fake_id_token"}
    mock_httpx_post.return_value = mock_auth_response

    # Mock historical data response
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
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
    }
    mock_httpx_get.return_value = mock_data_response

    # Mock datetime to have a fixed "today"
    fixed_today = datetime(2023, 3, 26, tzinfo=UTC)
    mock_datetime = mocker.patch("src.ingestion.jquants_client.datetime")
    mock_datetime.now.return_value = fixed_today
    mock_datetime.strptime = datetime.strptime # keep strptime working

    client = JQuantsClient(refresh_token="fake_refresh_token")
    quotes = client.fetch_historical_data()

    assert len(quotes) == 1
    assert quotes[0].date == datetime(2023, 1, 1, 0, 0, tzinfo=UTC)
    assert quotes[0].open == 100.0

    # Ensure 12 weeks calculation is correct
    expected_from_date = "20230101"
    expected_to_date = "20230326"

    mock_httpx_get.assert_called_once_with(
        "https://api.jquants.com/v1/prices/daily_quotes",
        headers={"Authorization": "Bearer fake_id_token"},
        params={"code": "0000", "from": expected_from_date, "to": expected_to_date}
    )

def test_jquants_client_retry_on_429_then_success(mock_httpx_post, mock_httpx_get, mocker):
    # Shorten sleep for tests
    mocker.patch("tenacity.nap.time.sleep")

    mock_auth_response = MagicMock()
    mock_auth_response.status_code = 200
    mock_auth_response.json.return_value = {"idToken": "fake_id_token"}
    mock_httpx_post.return_value = mock_auth_response

    mock_429 = MagicMock()
    mock_429.status_code = 429
    mock_429.raise_for_status.side_effect = httpx.HTTPStatusError("429 Too Many Requests", request=MagicMock(), response=mock_429)

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = {"daily_quotes": []}

    mock_httpx_get.side_effect = [mock_429, mock_429, mock_200]

    client = JQuantsClient(refresh_token="fake_refresh_token")
    client.fetch_historical_data()

    assert mock_httpx_get.call_count == 3

def test_jquants_client_catastrophic_failure(mock_httpx_post, mock_httpx_get, mocker):
    mocker.patch("tenacity.nap.time.sleep")

    mock_auth_response = MagicMock()
    mock_auth_response.status_code = 200
    mock_auth_response.json.return_value = {"idToken": "fake_id_token"}
    mock_httpx_post.return_value = mock_auth_response

    mock_500 = MagicMock()
    mock_500.status_code = 500
    mock_500.raise_for_status.side_effect = httpx.HTTPStatusError("500 Internal Server Error", request=MagicMock(), response=mock_500)

    mock_httpx_get.side_effect = [mock_500] * 5  # Ensure it exhausts retries

    client = JQuantsClient(refresh_token="fake_refresh_token")

    with pytest.raises(APIConnectionError):
        client.fetch_historical_data()
