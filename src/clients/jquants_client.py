import logging
from datetime import date, datetime, timedelta

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.domain_models import RawQuote

logger = logging.getLogger(__name__)


class APIConnectionError(Exception):
    pass


class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, refresh_token: str):
        self.refresh_token = refresh_token
        self.id_token: str | None = None

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True,
    )
    def _authenticate(self) -> None:
        try:
            response = httpx.post(
                f"{self.BASE_URL}/token/auth_refresh?refreshtoken={self.refresh_token}"
            )
            response.raise_for_status()
            data = response.json()
            self.id_token = data["idToken"]
        except httpx.HTTPStatusError as e:
            if e.response.status_code in [429, 500, 502, 503, 504]:
                logger.warning(f"Retryable error during authentication: {e}")
                raise
            raise APIConnectionError(f"Authentication failed: {e}") from e
        except Exception as e:
            raise APIConnectionError(f"Network error during authentication: {e}") from e

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        reraise=True,
    )
    def _fetch_daily_quotes(self, code: str, from_date: str, to_date: str) -> list[RawQuote]:
        if not self.id_token:
            self._authenticate()

        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {"code": code, "from": from_date, "to": to_date}

        try:
            response = httpx.get(f"{self.BASE_URL}/quotes/daily", headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            quotes = []
            for item in data.get("daily_quotes", []):
                dt_str = item.get("Date", "").split("T")[0]
                quotes.append(
                    RawQuote(
                        date=date.fromisoformat(dt_str)
                        if "-" in dt_str
                        else datetime.strptime(dt_str, "%Y%m%d").date(),
                        code=item.get("Code", ""),
                        open=float(item.get("Open", 0.0) or 0.0),
                        high=float(item.get("High", 0.0) or 0.0),
                        low=float(item.get("Low", 0.0) or 0.0),
                        close=float(item.get("Close", 0.0) or 0.0),
                        volume=float(item.get("Volume", 0.0) or 0.0),
                    )
                )
            return quotes
        except httpx.HTTPStatusError as e:
            if e.response.status_code in [401, 403]:
                # Token might be expired, re-authenticate and fail to let retry catch it
                self.id_token = None
                raise
            if e.response.status_code in [429, 500, 502, 503, 504]:
                logger.warning(f"Retryable error during fetching quotes: {e}")
                raise
            raise APIConnectionError(f"Failed to fetch quotes: {e}") from e
        except Exception as e:
            if not isinstance(e, httpx.HTTPStatusError):
                raise APIConnectionError(f"Network error during fetching quotes: {e}") from e
            raise

    def fetch_quotes(self, code: str) -> list[RawQuote]:
        to_date = datetime.now()
        from_date = to_date - timedelta(weeks=12)

        from_str = from_date.strftime("%Y%m%d")
        to_str = to_date.strftime("%Y%m%d")

        try:
            return self._fetch_daily_quotes(code, from_str, to_str)
        except Exception as e:
            raise APIConnectionError(f"Fatal error fetching quotes: {e}") from e
