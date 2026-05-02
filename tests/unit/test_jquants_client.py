from datetime import date
from unittest.mock import MagicMock

import httpx
from src.jquants_client import JQuantsClient

from src.domain_models.models import RawQuote


def test_jquants_client_success(mocker: MagicMock) -> None:
    mock_post = mocker.patch("httpx.Client.post")
    mock_get = mocker.patch("httpx.Client.get")

    # Mock Token Response
    mock_post.return_value = httpx.Response(
        200,
        json={"idToken": "mock_id_token"},
        request=httpx.Request("POST", "https://api.jquants.com/v1/token/auth_refresh")
    )

    # Mock Quotes Response
    mock_get.return_value = httpx.Response(
        200,
        json={
            "daily_quotes": [
                {
                    "Date": "2023-01-31",
                    "Open": 100.0,
                    "High": 110.0,
                    "Low": 90.0,
                    "Close": 105.0,
                    "Volume": 1000,
                }
            ]
        },
        request=httpx.Request("GET", "https://api.jquants.com/v1/prices/daily_quotes")
    )

    client = JQuantsClient(refresh_token="mock_refresh_token")
    quotes = client.fetch_daily_quotes(code="86970") # Japan Exchange Group

    assert len(quotes) == 1
    assert isinstance(quotes[0], RawQuote)
    assert quotes[0].date == date(2023, 1, 31)
    assert quotes[0].high == 110.0


def test_jquants_client_retry_logic(mocker: MagicMock) -> None:
    mock_post = mocker.patch("httpx.Client.post")
    mock_get = mocker.patch("httpx.Client.get")

    # Mock Token Response
    mock_post.return_value = httpx.Response(
        200,
        json={"idToken": "mock_id_token"},
        request=httpx.Request("POST", "https://api.jquants.com/v1/token/auth_refresh")
    )

    # Mock Quotes Response - Fail with 429 twice, then succeed
    response_429 = httpx.Response(
        429,
        request=httpx.Request("GET", "https://api.jquants.com/v1/prices/daily_quotes")
    )
    response_200 = httpx.Response(
        200,
        json={"daily_quotes": []},
        request=httpx.Request("GET", "https://api.jquants.com/v1/prices/daily_quotes")
    )

    mock_get.side_effect = [response_429, response_429, response_200]

    client = JQuantsClient(refresh_token="mock_refresh_token")
    # Patch the sleep function in tenacity to speed up tests
    mocker.patch("tenacity.nap.time.sleep")

    quotes = client.fetch_daily_quotes(code="86970")

    assert len(quotes) == 0
    assert mock_get.call_count == 3
