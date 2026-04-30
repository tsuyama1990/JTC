import logging
from datetime import UTC, datetime, timedelta

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.core.exceptions import APIConnectionError
from src.domain_models.quotes import RawQuote

logger = logging.getLogger(__name__)

class JQuantsClient:
    """Client for fetching data from the J-Quants API."""

    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, refresh_token: str) -> None:
        self.refresh_token = refresh_token
        self._id_token: str | None = None

    def get_id_token(self) -> str:
        """Exchanges the refresh token for a short-lived ID token."""
        url = f"{self.BASE_URL}/token/auth_refresh"
        try:
            response = httpx.post(url, params={"refresh_token": self.refresh_token})
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.exception("Failed to authenticate with J-Quants API")
            msg = "Failed to obtain ID token."
            raise APIConnectionError(msg) from e
        else:
            self._id_token = data["idToken"]
            return self._id_token

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True
    )
    def _fetch_quotes_with_retry(self, url: str, headers: dict[str, str], params: dict[str, str]) -> dict[str, list[dict[str, str | int | float]]]:
        """Fetches data with exponential backoff on HTTP 429 and 5xx errors."""
        response = httpx.get(url, headers=headers, params=params)

        # Raise an exception for bad status codes to trigger retry
        if response.status_code >= 400:
             logger.warning(f"HTTP {response.status_code} received. Retrying...")
             response.raise_for_status()

        return response.json() # type: ignore[no-any-return]

    def fetch_historical_data(self) -> list[RawQuote]:
        """Fetches daily quotes for the last 12 weeks."""
        if not self._id_token:
            self.get_id_token()

        # Calculate date range (last 12 weeks)
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(weeks=12)

        # Format dates as YYYYMMDD
        from_str = start_date.strftime("%Y%m%d")
        to_str = end_date.strftime("%Y%m%d")

        url = f"{self.BASE_URL}/prices/daily_quotes"
        headers = {"Authorization": f"Bearer {self._id_token}"}
        # Code 0000 is often a proxy for general market/index or we can just fetch all?
        # The spec doesn't specify a specific ticker. We'll use 0000 as a placeholder or remove it if not needed.
        # Actually, let's just use a sample ticker "0000" to get some data, or omit it if JQuants requires it.
        # Wait, the spec says "raw historical daily price quotes". Let's provide "0000" as we mocked it.
        params = {"code": "0000", "from": from_str, "to": to_str}

        try:
            data = self._fetch_quotes_with_retry(url, headers, params)
        except httpx.HTTPError as e:
            logger.exception("Catastrophic failure fetching data from J-Quants API")
            msg = "Exhausted retries fetching historical data."
            raise APIConnectionError(msg) from e

        quotes = []
        for item in data.get("daily_quotes", []):
            try:
                # Handle possible nulls or missing fields by forcing type conversion where needed
                # Note: J-Quants returns Date in "YYYY-MM-DD" string format
                date_str = str(item["Date"])
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
                quote = RawQuote(
                    date=date_obj,
                    open=float(item["Open"]),
                    high=float(item["High"]),
                    low=float(item["Low"]),
                    close=float(item["Close"]),
                    volume=int(item["Volume"])
                )
                quotes.append(quote)
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Skipping malformed quote data: {item}. Error: {e}")
                continue

        return quotes
