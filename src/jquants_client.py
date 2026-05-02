from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from src.domain_models.models import RawQuote


class JQuantsClientError(Exception):
    pass

class APIConnectionError(JQuantsClientError):
    pass

def is_retryable_exception(exception: BaseException) -> bool:
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code == 429 or exception.response.status_code >= 500
    return isinstance(exception, (httpx.ConnectError, httpx.TimeoutException))

class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, refresh_token: str) -> None:
        self._refresh_token = refresh_token
        self._id_token: str | None = None
        self._client = httpx.Client(timeout=30.0)

    def _authenticate(self) -> None:
        url = f"{self.BASE_URL}/token/auth_refresh"
        response = self._client.post(url, params={"refresh_token": self._refresh_token})
        response.raise_for_status()
        data = response.json()
        self._id_token = data["idToken"]

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception(is_retryable_exception),
        reraise=True
    )
    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> httpx.Response:
        if not self._id_token:
            self._authenticate()

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"Authorization": f"Bearer {self._id_token}"}

        response = self._client.get(url, headers=headers, params=params)

        if response.status_code in (401, 403):
            self._id_token = None
            self._authenticate()
            headers = {"Authorization": f"Bearer {self._id_token}"}
            response = self._client.get(url, headers=headers, params=params)

        response.raise_for_status()
        return response

    def fetch_daily_quotes(self, code: str) -> list[RawQuote]:
        end_date = datetime.now(tz=UTC).date()
        start_date = end_date - timedelta(weeks=12)

        params = {
            "code": code,
            "from": start_date.strftime("%Y%m%d"),
            "to": end_date.strftime("%Y%m%d"),
        }

        try:
            response = self._get("prices/daily_quotes", params=params)
        except httpx.HTTPStatusError as e:
            msg = f"API connection failed after retries: {e}"
            raise APIConnectionError(msg) from e
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            msg = f"Connection error: {e}"
            raise APIConnectionError(msg) from e
        except Exception as e:
            msg = f"Unexpected error: {e}"
            raise APIConnectionError(msg) from e

        data = response.json()
        raw_quotes_data = data.get("daily_quotes", [])

        quotes = []
        for item in raw_quotes_data:
            quotes.append(
                RawQuote(
                    date=date.fromisoformat(item["Date"]),
                    open=float(item["Open"]),
                    high=float(item["High"]),
                    low=float(item["Low"]),
                    close=float(item["Close"]),
                    volume=int(float(item["Volume"])),
                )
            )

        return quotes
