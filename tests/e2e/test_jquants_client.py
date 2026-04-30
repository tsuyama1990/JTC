import datetime

import pytest
import requests_mock as rm

from src.core.exceptions import APIConnectionError
from src.ingestion.jquants_client import JQuantsClient


def test_jquants_client_auth_success(requests_mock: rm.Mocker) -> None:
    requests_mock.post("https://api.jquants.com/v1/token/auth_refresh", json={"idToken": "fake_id_token"})
    client = JQuantsClient("fake_refresh_token")
    client._authenticate()
    assert client.id_token == "fake_id_token" # noqa: S105
    assert client.session.headers["Authorization"] == "Bearer fake_id_token"

def test_jquants_client_auth_failure_retry(requests_mock: rm.Mocker) -> None:
    requests_mock.post("https://api.jquants.com/v1/token/auth_refresh",
                       [{"status_code": 500}, {"status_code": 500}, {"json": {"idToken": "fake_id_token_2"}}])
    client = JQuantsClient("fake_refresh_token")
    client._authenticate()
    assert client.id_token == "fake_id_token_2" # noqa: S105

def test_jquants_client_fetch_quotes_success(requests_mock: rm.Mocker) -> None:
    requests_mock.post("https://api.jquants.com/v1/token/auth_refresh", json={"idToken": "fake_id_token"})

    quotes_response = {
        "daily_quotes": [
            {
                "Date": "2023-11-01",
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 105.0,
                "Volume": 1000
            }
        ]
    }
    requests_mock.get("https://api.jquants.com/v1/prices/daily_quotes", json=quotes_response)

    client = JQuantsClient("fake_refresh_token")
    end_date = datetime.date(2023, 11, 1)
    quotes = client.fetch_last_12_weeks(end_date=end_date)

    assert len(quotes) == 1
    assert quotes[0].date == "2023-11-01"
    assert quotes[0].open == 100.0

def test_jquants_client_fetch_quotes_rate_limit(requests_mock: rm.Mocker) -> None:
    requests_mock.post("https://api.jquants.com/v1/token/auth_refresh", json={"idToken": "fake_id_token"})

    quotes_response = {
        "daily_quotes": [
            {
                "Date": "2023-11-01",
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 105.0,
                "Volume": 1000
            }
        ]
    }
    requests_mock.get("https://api.jquants.com/v1/prices/daily_quotes",
                      [{"status_code": 429}, {"json": quotes_response}])

    client = JQuantsClient("fake_refresh_token")
    end_date = datetime.date(2023, 11, 1)
    quotes = client.fetch_last_12_weeks(end_date=end_date)

    assert len(quotes) == 1
    assert quotes[0].date == "2023-11-01"

def test_jquants_client_fetch_quotes_exhaust_retries(requests_mock: rm.Mocker) -> None:
    requests_mock.post("https://api.jquants.com/v1/token/auth_refresh", json={"idToken": "fake_id_token"})
    requests_mock.get("https://api.jquants.com/v1/prices/daily_quotes", status_code=500)

    client = JQuantsClient("fake_refresh_token")
    end_date = datetime.date(2023, 11, 1)

    with pytest.raises(APIConnectionError, match="Transient error 500"):
        client.fetch_last_12_weeks(end_date=end_date)
