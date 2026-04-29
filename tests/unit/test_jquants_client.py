from unittest.mock import MagicMock

import requests

from src.domain_models.config import AppSettings
from src.jquants_client import JQuantsClient


def test_jquants_client_fetch_success(mocker: MagicMock) -> None:
    settings = AppSettings(jquants_refresh_token="fake")  # noqa: S106
    client = JQuantsClient(settings)

    mock_post = mocker.patch("requests.post")
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"idToken": "fake_id_token"}

    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "quotes": [
            {
                "Date": "2023-10-01",
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 105.0,
                "Volume": 1000,
            }
        ]
    }

    quotes = client.fetch_quotes()
    assert len(quotes) == 1
    assert quotes[0].open == 100.0
    assert mock_post.called
    assert mock_get.called


def test_jquants_client_retry_logic(mocker: MagicMock) -> None:
    settings = AppSettings(jquants_refresh_token="fake")  # noqa: S106
    client = JQuantsClient(settings)

    mock_post = mocker.patch("requests.post")
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"idToken": "fake_id_token"}

    mock_get = mocker.patch("requests.get")
    # First two fail with 500, third succeeds
    resp_500 = MagicMock()
    resp_500.status_code = 500
    resp_500.raise_for_status.side_effect = requests.exceptions.HTTPError()

    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = {"quotes": []}

    mock_get.side_effect = [resp_500, resp_500, resp_200]

    quotes = client.fetch_quotes()
    assert len(quotes) == 0
    assert mock_get.call_count == 3
