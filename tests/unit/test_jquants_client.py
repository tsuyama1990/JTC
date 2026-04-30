from datetime import UTC, datetime

import httpx
import pytest
from pytest_mock import MockerFixture

from src.core.exceptions import APIConnectionError
from src.ingestion.jquants_client import JQuantsClient


def test_jquants_client_fetch_id_token_success(mocker: MockerFixture) -> None:
    mock_post = mocker.patch("httpx.post")
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"idToken": "fake_id_token"}
    mock_post.return_value = mock_response

    client = JQuantsClient(refresh_token="fake_refresh_token")  # noqa: S106
    token = client.get_id_token()

    assert token == "fake_id_token"  # noqa: S105
    mock_post.assert_called_once_with(
        "https://api.jquants.com/v1/token/auth_refresh",
        params={"refresh_token": "fake_refresh_token"},
    )


def test_jquants_client_fetch_id_token_failure(mocker: MockerFixture) -> None:
    mock_post = mocker.patch("httpx.post")
    mock_response = mocker.Mock()
    mock_response.status_code = 401
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Unauthorized", request=mocker.Mock(), response=mock_response
    )
    mock_post.return_value = mock_response

    client = JQuantsClient(refresh_token="fake_refresh_token")  # noqa: S106

    with pytest.raises(APIConnectionError):
        client.get_id_token()


def test_jquants_client_fetch_quotes_success(mocker: MockerFixture) -> None:
    mock_get = mocker.patch("httpx.get")
    mock_response = mocker.Mock()
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
    mock_get.return_value = mock_response

    client = JQuantsClient(refresh_token="fake_refresh_token")  # noqa: S106
    client._id_token = "fake_id_token"  # noqa: S105

    quotes = client.fetch_daily_quotes(
        code="86970",
        start_date=datetime(2023, 1, 1, tzinfo=UTC),
        end_date=datetime(2023, 1, 2, tzinfo=UTC),
    )

    assert len(quotes) == 1
    assert quotes[0].open == 100.0
    mock_get.assert_called_once()
    assert mock_get.call_args[1]["headers"] == {"Authorization": "Bearer fake_id_token"}
    assert mock_get.call_args[1]["params"]["code"] == "86970"
    assert mock_get.call_args[1]["params"]["from"] == "20230101"
    assert mock_get.call_args[1]["params"]["to"] == "20230102"


def test_jquants_client_fetch_quotes_retry_then_fail(mocker: MockerFixture) -> None:
    mock_get = mocker.patch("httpx.get")
    mock_response = mocker.Mock()
    mock_response.status_code = 429
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Too Many Requests", request=mocker.Mock(), response=mock_response
    )
    mock_get.return_value = mock_response

    client = JQuantsClient(refresh_token="fake_refresh_token")  # noqa: S106
    client._id_token = "fake_id_token"  # noqa: S105

    with pytest.raises(APIConnectionError):
        client.fetch_daily_quotes(
            code="86970",
            start_date=datetime(2023, 1, 1, tzinfo=UTC),
            end_date=datetime(2023, 1, 2, tzinfo=UTC),
        )

    assert mock_get.call_count > 1  # Verified tenacity retry
