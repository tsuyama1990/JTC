from unittest.mock import MagicMock

import httpx
import pytest
from pytest_mock import MockerFixture
from tenacity import RetryError

from domain_models import AppSettings
from jquants_client import JQuantsClient


@pytest.fixture
def client() -> JQuantsClient:
    settings = AppSettings(JQUANTS_REFRESH_TOKEN="dummy_token")
    return JQuantsClient(settings)


def test_jquants_client_fetch_success(client: JQuantsClient, mocker: MockerFixture) -> None:
    # Mock authentication step
    mocker.patch.object(client, "_get_id_token", return_value="dummy_id_token")

    # Mock data fetching step
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "quotes": [
            {
                "Date": "2023-10-25",
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 105.0,
                "Volume": 1000.0,
            }
        ]
    }

    mocker.patch("httpx.get", return_value=mock_response)

    # Call the actual method
    quotes = client.fetch_quotes(symbol="1234")

    assert len(quotes) == 1
    assert quotes[0].date == "2023-10-25"
    assert quotes[0].high == 110.0


def test_jquants_client_retry_on_429(client: JQuantsClient, mocker: MockerFixture) -> None:
    mocker.patch.object(client, "_get_id_token", return_value="dummy_id_token")

    # First response fails with 429, second succeeds
    mock_response_fail = MagicMock()
    mock_response_fail.status_code = 429
    mock_response_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        "429 Too Many Requests", request=MagicMock(), response=mock_response_fail
    )

    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {"quotes": []}

    mock_get = mocker.patch("httpx.get", side_effect=[mock_response_fail, mock_response_success])

    # Should succeed on the second try
    quotes = client.fetch_quotes(symbol="1234")
    assert quotes == []
    assert mock_get.call_count == 2


def test_jquants_client_retry_exhausted(client: JQuantsClient, mocker: MockerFixture) -> None:
    mocker.patch.object(client, "_get_id_token", return_value="dummy_id_token")

    # Fail with 500 constantly
    mock_response_fail = MagicMock()
    mock_response_fail.status_code = 500
    mock_response_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500 Internal Server Error", request=MagicMock(), response=mock_response_fail
    )

    mocker.patch("httpx.get", return_value=mock_response_fail)

    # Eventually it should raise an error
    with pytest.raises((RetryError, httpx.HTTPStatusError, Exception)):
        client.fetch_quotes(symbol="1234")
