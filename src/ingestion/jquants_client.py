from datetime import datetime
from typing import Any

import httpx
from pydantic import ValidationError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.core.exceptions import APIConnectionError
from src.domain_models.quote import RawQuote


class JQuantsClient:
    def __init__(self, refresh_token: str) -> None:
        self.refresh_token = refresh_token
        self._id_token: str | None = None
        self.base_url = "https://api.jquants.com/v1"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True,
    )
    def _post_with_retry(self, url: str, **kwargs: Any) -> httpx.Response:
        response = httpx.post(url, **kwargs)
        if response.status_code == 429 or response.status_code >= 500:
            response.raise_for_status()
        return response

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True,
    )
    def _get_with_retry(self, url: str, **kwargs: Any) -> httpx.Response:
        response = httpx.get(url, **kwargs)
        if response.status_code == 429 or response.status_code >= 500:
            response.raise_for_status()
        return response

    def get_id_token(self) -> str:
        url = f"{self.base_url}/token/auth_refresh"
        try:
            # J-Quants API expects 'refreshtoken' instead of 'refresh_token' per testing
            response = self._post_with_retry(url, params={"refreshtoken": self.refresh_token})
            # Also catch 400 Bad Request which is what it returns for invalid refresh token
            if response.status_code in {400, 401, 403}:
                response.raise_for_status()
        except httpx.HTTPStatusError as e:
            msg = f"Failed to authenticate with J-Quants API: {e}"
            raise APIConnectionError(msg) from e
        except httpx.RequestError as e:
            msg = f"Network error during authentication: {e}"
            raise APIConnectionError(msg) from e

        data = response.json()
        self._id_token = data.get("idToken")
        if not self._id_token:
            msg = "ID token not found in response"
            raise APIConnectionError(msg)

        return self._id_token

    def fetch_daily_quotes(
        self, code: str, start_date: datetime, end_date: datetime
    ) -> list[RawQuote]:
        if not self._id_token:
            self.get_id_token()

        url = f"{self.base_url}/quotes/daily_quotes"
        params = {
            "code": code,
            "from": start_date.strftime("%Y%m%d"),
            "to": end_date.strftime("%Y%m%d"),
        }

        def _make_request() -> httpx.Response:
            headers = {"Authorization": f"Bearer {self._id_token}"}
            return self._get_with_retry(url, params=params, headers=headers)

        try:
            response = _make_request()
            if response.status_code in {401, 403}:
                # Try refreshing token once
                self.get_id_token()
                response = _make_request()
                if response.status_code in {401, 403}:
                    response.raise_for_status()
        except httpx.HTTPStatusError as e:
            msg = f"Failed to fetch quotes from J-Quants API: {e}"
            raise APIConnectionError(msg) from e
        except httpx.RequestError as e:
            msg = f"Network error during fetching quotes: {e}"
            raise APIConnectionError(msg) from e

        data = response.json()
        raw_quotes_data = data.get("daily_quotes", [])

        quotes = []
        for q in raw_quotes_data:
            try:
                quote = RawQuote(
                    date=datetime.strptime(q["Date"], "%Y-%m-%d").replace(tzinfo=start_date.tzinfo),
                    open=float(q["Open"]),
                    high=float(q["High"]),
                    low=float(q["Low"]),
                    close=float(q["Close"]),
                    volume=int(q["Volume"]),
                )
                quotes.append(quote)
            except (ValueError, KeyError, ValidationError) as e:
                msg = f"Failed to parse quote data: {e}"
                raise APIConnectionError(msg) from e

        return quotes
