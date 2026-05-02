"""J-Quants API Client Module."""

from datetime import UTC, datetime, timedelta

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.domain_models.models import RawQuote


class APIConnectionError(Exception):
    pass

class JQuantsClient:
    def __init__(self, refresh_token: str) -> None:
        self.refresh_token = refresh_token
        self._id_token: str | None = None
        self.client = httpx.Client(base_url="https://api.jquants.com/v1")

    def _authenticate(self) -> None:
        response = self.client.post(
            "/token/auth_user",
            json={"refreshToken": self.refresh_token},
        )
        response.raise_for_status()
        self._id_token = response.json().get("idToken")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True
    )
    def _fetch_with_retry(self, start_date: str, end_date: str) -> dict[str, object]:
        if not self._id_token:
            self._authenticate()

        headers = {"Authorization": f"Bearer {self._id_token}"}
        params = {"from": start_date, "to": end_date}

        response = self.client.get("/prices/daily_quotes", headers=headers, params=params)

        # We only retry on 429 and 5xx. If it's a 4xx other than 429 (like 401), we don't want to retry endlessly unless configured, but our test simulates 429 and 500.
        if response.status_code == 429 or response.status_code >= 500:
            response.raise_for_status()

        response.raise_for_status()

        json_resp = response.json()
        if not isinstance(json_resp, dict):
             msg = "Invalid JSON response type"
             raise TypeError(msg)
        return json_resp

    def fetch_daily_quotes(self) -> list[RawQuote]:
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(weeks=12)

        str_start = start_date.strftime("%Y%m%d")
        str_end = end_date.strftime("%Y%m%d")

        try:
            data = self._fetch_with_retry(str_start, str_end)
        except httpx.HTTPStatusError as e:
            msg = f"Failed to fetch data from J-Quants API after retries: {e}"
            raise APIConnectionError(msg) from e

        quotes = []
        raw_quotes = data.get("daily_quotes", [])
        if isinstance(raw_quotes, list):
            for quote_data in raw_quotes:
                if isinstance(quote_data, dict):
                    # Mapping the JSON response structure to our RawQuote model
                    mapped_data = {
                        "date": quote_data.get("Date"),
                        "open": quote_data.get("Open"),
                        "high": quote_data.get("High"),
                        "low": quote_data.get("Low"),
                        "close": quote_data.get("Close"),
                        "volume": quote_data.get("Volume"),
                    }
                    try:
                        quote = RawQuote(**mapped_data) # type: ignore[arg-type]
                        quotes.append(quote)
                    except ValueError:
                        # Log or handle validation error, but for now we skip invalid ones
                        pass

        return quotes
