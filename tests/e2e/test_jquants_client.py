from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from tenacity import RetryError

from src.domain_models.quotes import RawQuote
from src.jquants_client import JQuantsClient


def test_jquants_client_success(mocker: MockerFixture) -> None:
    # Mocking requests.post for token exchange
    mock_post = mocker.patch("requests.post")
    mock_post_resp = MagicMock()
    mock_post_resp.status_code = 200
    mock_post_resp.json.return_value = {"idToken": "fake_id_token"}
    mock_post.return_value = mock_post_resp

    # Mocking requests.get for fetching data
    mock_get = mocker.patch("requests.get")
    mock_get_resp = MagicMock()
    mock_get_resp.status_code = 200
    mock_get_resp.json.return_value = {
        "quotes": [
            {
                "Date": "2024-01-01",
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 105.0,
                "Volume": 1000,
            }
        ]
    }
    mock_get.return_value = mock_get_resp

    # Intentional dummy token for isolated testing; real token loaded from .env in production
    client = JQuantsClient(refresh_token="test_token")
    quotes = client.fetch_quotes()

    assert len(quotes) == 1
    assert isinstance(quotes[0], RawQuote)
    assert quotes[0].high == 110.0


def test_jquants_client_retry_failure(mocker: MockerFixture) -> None:
    # Mocking requests.post for token exchange
    mock_post = mocker.patch("requests.post")
    mock_post_resp = MagicMock()
    mock_post_resp.status_code = 200
    mock_post_resp.json.return_value = {"idToken": "fake_id_token"}
    mock_post.return_value = mock_post_resp

    # Mocking requests.get to simulate 500 error consistently
    mock_get = mocker.patch("requests.get")
    mock_get_resp = MagicMock()
    mock_get_resp.status_code = 500
    mock_get.return_value = mock_get_resp

    # Intentional dummy token for isolated testing; real token loaded from .env in production
    client = JQuantsClient(refresh_token="test_token")

    with pytest.raises(RetryError):
        client.fetch_quotes()

    # We should have retried multiple times
    assert mock_get.call_count > 1
