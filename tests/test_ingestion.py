import pytest
import responses

from src.ingestion.jquants_client import JQuantsClient


def test_init_no_token() -> None:
    with pytest.raises(ValueError, match="JQUANTS_REFRESH_TOKEN must be provided"):
        JQuantsClient(refresh_token="")


@responses.activate
def test_get_id_token_success() -> None:
    responses.add(
        responses.POST,
        "https://api.jquants.com/v1/token/auth_refresh",
        json={"idToken": "fake_id_token"},
        status=200,
    )
    client = JQuantsClient("fake_refresh_token")
    token = client._get_id_token()
    assert token == "fake_id_token"  # noqa: S105


@responses.activate
def test_get_id_token_retry() -> None:
    responses.add(responses.POST, "https://api.jquants.com/v1/token/auth_refresh", status=500)
    responses.add(
        responses.POST,
        "https://api.jquants.com/v1/token/auth_refresh",
        json={"idToken": "fake_id_token2"},
        status=200,
    )
    client = JQuantsClient("fake_refresh_token")

    if hasattr(client._get_id_token, "retry"):
        client._get_id_token.retry.wait.wait_fixed = 0
        client._get_id_token.retry.wait.wait_funcs = []

    token = client._get_id_token()
    assert token == "fake_id_token2"  # noqa: S105
    assert len(responses.calls) == 2


@responses.activate
def test_get_historical_quotes() -> None:
    responses.add(
        responses.POST,
        "https://api.jquants.com/v1/token/auth_refresh",
        json={"idToken": "fake_id_token"},
        status=200,
    )

    mock_data = {
        "daily_quotes": [
            {
                "Date": "2023-01-01",
                "Code": "1234",
                "Open": 100,
                "High": 110,
                "Low": 90,
                "Close": 105,
                "Volume": 1000,
            }
        ]
    }

    responses.add(
        responses.GET, "https://api.jquants.com/v1/prices/daily_quotes", json=mock_data, status=200
    )

    client = JQuantsClient("fake_refresh_token")
    quotes = client.get_historical_quotes(weeks=1)

    assert len(quotes) == 1
    assert quotes[0].Code == "1234"
    assert quotes[0].Volume == 1000

    auth_header = responses.calls[1].request.headers.get("Authorization")
    assert auth_header == "Bearer fake_id_token"


@responses.activate
def test_get_historical_quotes_pagination() -> None:
    responses.add(
        responses.POST,
        "https://api.jquants.com/v1/token/auth_refresh",
        json={"idToken": "fake_id_token"},
        status=200,
    )

    page1 = {
        "daily_quotes": [
            {
                "Date": "2023-01-01",
                "Code": "1234",
                "Open": 100,
                "High": 110,
                "Low": 90,
                "Close": 105,
                "Volume": 1000,
            }
        ],
        "pagination_key": "page2",
    }

    page2 = {
        "daily_quotes": [
            {
                "Date": "2023-01-02",
                "Code": "1234",
                "Open": 105,
                "High": 115,
                "Low": 95,
                "Close": 110,
                "Volume": 2000,
            }
        ]
    }

    responses.add(
        responses.GET, "https://api.jquants.com/v1/prices/daily_quotes", json=page1, status=200
    )

    responses.add(
        responses.GET, "https://api.jquants.com/v1/prices/daily_quotes", json=page2, status=200
    )

    client = JQuantsClient("fake_refresh_token")
    quotes = client.get_historical_quotes(weeks=1)

    assert len(quotes) == 2
    assert quotes[0].Date == "2023-01-01"
    assert quotes[1].Date == "2023-01-02"
