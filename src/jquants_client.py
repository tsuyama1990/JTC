import datetime

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from domain_models import AppSettings, RawQuote


class JQuantsClient:
    """Client for fetching data from J-Quants API."""

    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, settings: AppSettings) -> None:
        self.refresh_token = settings.JQUANTS_REFRESH_TOKEN

    def _get_id_token(self) -> str:
        """Exchanges refresh token for an ID token."""
        url = f"{self.BASE_URL}/token/auth_refresh"
        params = {"refresh_token": self.refresh_token}
        response = httpx.post(url, params=params)
        response.raise_for_status()
        return response.json()["idToken"]

    def _calculate_date_range(self) -> tuple[str, str]:
        """Calculates start and end dates for the most recent 12 weeks."""
        end_date = datetime.datetime.now(tz=datetime.UTC).date()
        start_date = end_date - datetime.timedelta(weeks=12)
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def fetch_quotes(self, symbol: str) -> list[RawQuote]:
        """Fetches quote data for a symbol using 2-step auth and exponential backoff."""
        id_token = self._get_id_token()
        start_date, end_date = self._calculate_date_range()

        url = f"{self.BASE_URL}/prices/daily_quotes"
        headers = {"Authorization": f"Bearer {id_token}"}
        params = {"code": symbol, "from": start_date, "to": end_date}

        response = httpx.get(url, headers=headers, params=params)

        # Raise HTTPStatusError for 4xx/5xx responses
        response.raise_for_status()

        data = response.json()
        raw_quotes = []
        for item in data.get("quotes", []):
            try:
                # Format from JQuants is Date: YYYY-MM-DD
                quote = RawQuote(
                    date=item.get("Date", ""),
                    open=item.get("Open"),
                    high=item.get("High"),
                    low=item.get("Low"),
                    close=item.get("Close"),
                    volume=item.get("Volume"),
                )
                raw_quotes.append(quote)
            except ValueError:
                # Skip invalid quotes or handle them based on strictness
                pass

        return raw_quotes
