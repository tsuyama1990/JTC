import datetime
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.core.exceptions import APIConnectionError
from src.domain_models.quote import RawQuote


class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, refresh_token: str) -> None:
        self.refresh_token = refresh_token
        self.id_token: str | None = None
        self.session = requests.Session()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, APIConnectionError)),
        reraise=True
    )
    def _authenticate(self) -> None:
        url = f"{self.BASE_URL}/token/auth_refresh"
        params = {"refresh_token": self.refresh_token}
        try:
            response = self.session.post(url, params=params, timeout=10)
            if response.status_code == 403:
                err_msg = "Authentication failed. Invalid refresh token."
                raise APIConnectionError(err_msg)
            response.raise_for_status()
            data = response.json()
            if "idToken" not in data:
                err_msg = "Authentication failed. Missing idToken in response."
                raise APIConnectionError(err_msg)
            self.id_token = data["idToken"]
            self.session.headers.update({"Authorization": f"Bearer {self.id_token}"})
        except requests.exceptions.RequestException as e:
            err_msg = f"Authentication request failed: {e}"
            raise APIConnectionError(err_msg) from e

    def _parse_quotes(self, quotes_data: list[dict[str, Any]]) -> list[RawQuote]:
        raw_quotes: list[RawQuote] = []
        for item in quotes_data:
            try:
                q = RawQuote(
                    date=item["Date"],
                    open=float(item["Open"]) if item["Open"] is not None else 0.0,
                    high=float(item["High"]) if item["High"] is not None else 0.0,
                    low=float(item["Low"]) if item["Low"] is not None else 0.0,
                    close=float(item["Close"]) if item["Close"] is not None else 0.0,
                    volume=int(float(item["Volume"])) if item["Volume"] is not None else 0
                )
                if q.open > 0 and q.high >= q.low and q.high >= q.open and q.high >= q.close:
                    raw_quotes.append(q)
            except (ValueError, KeyError, TypeError):
                continue
        return raw_quotes

    def fetch_last_12_weeks(self, end_date: datetime.date | None = None) -> list[RawQuote]:
        if not self.id_token:
            self._authenticate()

        if end_date is None:
            end_date = datetime.datetime.now(tz=datetime.UTC).date()

        start_date = end_date - datetime.timedelta(days=84)

        url = f"{self.BASE_URL}/prices/daily_quotes"
        params = {
            "from": start_date.strftime("%Y%m%d"),
            "to": end_date.strftime("%Y%m%d")
        }

        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=20),
            retry=retry_if_exception_type((requests.exceptions.RequestException, APIConnectionError)),
            reraise=True
        )
        def _fetch_range() -> list[RawQuote]:
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 401:
                    self._authenticate()
                    err_msg = "Token expired. Re-authenticating..."
                    raise APIConnectionError(err_msg)
                if response.status_code in (429, 500, 502, 503, 504):
                    err_msg = f"Transient error {response.status_code} from J-Quants API."
                    raise APIConnectionError(err_msg)
                response.raise_for_status()
                data = response.json()
                return self._parse_quotes(data.get("daily_quotes", []))
            except requests.exceptions.RequestException as e:
                err_msg = f"Data fetch request failed: {e}"
                raise APIConnectionError(err_msg) from e

        return _fetch_range()
