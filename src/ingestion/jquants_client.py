import logging
from datetime import UTC, datetime, timedelta

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.domain_models.quotes import RawQuote

logger = logging.getLogger(__name__)


class APIConnectionError(Exception):
    """Custom exception for API connection failures."""


class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, refresh_token: str) -> None:
        self.refresh_token = refresh_token
        self.id_token = ""
        self.client = httpx.Client()

    def _get_id_token(self) -> None:
        url = f"{self.BASE_URL}/token/auth_refresh"
        response = self.client.post(url, params={"refresh_token": self.refresh_token})
        response.raise_for_status()
        data = response.json()
        self.id_token = data["idToken"]

    @staticmethod
    def _is_retryable_exception(exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code == 429 or exc.response.status_code >= 500
        return isinstance(exc, httpx.RequestError)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
        reraise=True,
    )
    def _fetch_quotes_with_retry(self, url: str, params: dict[str, str]) -> httpx.Response:
        logger.warning(f"Fetching from {url} with params {params}")
        response = self.client.get(
            url, params=params, headers={"Authorization": f"Bearer {self.id_token}"}
        )
        if response.status_code == 429 or response.status_code >= 500:
            response.raise_for_status()

        return response

    def fetch_historical_quotes(self) -> list[RawQuote]:
        if not self.id_token:
            try:
                self._get_id_token()
            except Exception as e:
                msg = f"Failed to authenticate: {e}"
                raise APIConnectionError(msg) from e

        # Calculate dates: 12 weeks (84 days)
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(weeks=12)

        # J-Quants requires date in YYYYMMDD
        params = {"from": start_date.strftime("%Y%m%d"), "to": end_date.strftime("%Y%m%d")}

        url = f"{self.BASE_URL}/quotes/prices"
        try:
            response = self._fetch_quotes_with_retry(url, params)
            response.raise_for_status()
        except Exception as e:
            msg = f"Failed to fetch quotes after retries: {e}"
            raise APIConnectionError(msg) from e

        data = response.json()
        raw_quotes = []
        for item in data.get("quotes", []):
            try:
                # Map JQuants API keys (Date, Open, High, Low, Close, Volume) to our RawQuote
                quote = RawQuote(
                    date=item["Date"],
                    open=float(item["Open"]),
                    high=float(item["High"]),
                    low=float(item["Low"]),
                    close=float(item["Close"]),
                    volume=int(item["Volume"]),
                )
                raw_quotes.append(quote)
            except Exception as e:
                logger.warning(f"Failed to parse quote: {item}. Error: {e}")

        return raw_quotes
