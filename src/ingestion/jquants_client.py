import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.domain_models import APIConnectionError, RawQuote

logger = logging.getLogger(__name__)

class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, refresh_token: str) -> None:
        self.refresh_token = refresh_token
        self.id_token: str | None = None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, APIConnectionError)),
        reraise=True
    )
    def _authenticate(self) -> None:
        """Exchanges refresh token for an ID token."""
        url = f"{self.BASE_URL}/token/auth_refresh"
        try:
            response = requests.post(url, params={"refresh_token": self.refresh_token}, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "idToken" not in data:
                err_msg = "Invalid response from JQuants API: missing idToken"
                raise APIConnectionError(err_msg)
            self.id_token = data["idToken"]
            logger.info("Successfully authenticated with J-Quants API.")
        except requests.exceptions.RequestException as e:
            # Need to carefully handle the case where response isn't set yet due to connection failure
            status_code = getattr(getattr(e, 'response', None), 'status_code', None)
            if status_code == 429 or (status_code is not None and status_code >= 500):
                logger.warning("Network error or rate limit hit, retrying authentication: %s", str(e))
            err_msg = f"Failed to authenticate with J-Quants API: {e}"
            raise APIConnectionError(err_msg) from e
        except json.JSONDecodeError as e:
            err_msg = "Failed to parse JSON response from J-Quants API."
            raise APIConnectionError(err_msg) from e

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, APIConnectionError)),
        reraise=True
    )
    def fetch_historical_quotes_12_weeks(self) -> list[RawQuote]:
        """Fetches 12 weeks of historical data."""
        if not self.id_token:
            self._authenticate()

        now = datetime.now(UTC)
        start_date = now - timedelta(weeks=12)

        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = now.strftime("%Y%m%d")

        url = f"{self.BASE_URL}/prices/daily_quotes"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params: dict[str, Any] = {"from": start_date_str, "to": end_date_str}

        all_quotes: list[RawQuote] = []

        try:
            while True:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                quotes_data = data.get("daily_quotes", [])
                for q in quotes_data:
                    date_str = q.get("Date")
                    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC).date()
                    quote = RawQuote(
                        date=dt,
                        open=float(q.get("Open", 0.0)),
                        high=float(q.get("High", 0.0)),
                        low=float(q.get("Low", 0.0)),
                        close=float(q.get("Close", 0.0)),
                        volume=int(q.get("Volume", 0))
                    )
                    all_quotes.append(quote)

                pagination_key = data.get("pagination_key")
                if pagination_key:
                    params["pagination_key"] = pagination_key
                else:
                    break
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None and (e.response.status_code == 429 or e.response.status_code >= 500):
                logger.warning("Network error or rate limit hit while fetching quotes: %s", str(e))
            err_msg = f"Failed to fetch quotes from J-Quants API: {e}"
            raise APIConnectionError(err_msg) from e
        else:
            return all_quotes
