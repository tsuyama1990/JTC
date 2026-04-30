from datetime import UTC, datetime, timedelta

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.domain_models.quotes import RawQuote


class JQuantsAPIError(Exception):
    pass


class JQuantsClient:
    def __init__(self, refresh_token: str) -> None:
        self.refresh_token = refresh_token
        self.id_token: str | None = None

    def authenticate(self) -> None:
        url = "https://api.jquants.com/v1/token/auth_refresh"
        response = requests.post(url, params={"refresh_token": self.refresh_token}, timeout=10)
        response.raise_for_status()
        data = response.json()
        self.id_token = data["idToken"]

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(JQuantsAPIError),
    )
    def fetch_quotes(self) -> list[RawQuote]:
        if not self.id_token:
            self.authenticate()

        # Target most recent 12 weeks
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(weeks=12)

        url = "https://api.jquants.com/v1/prices/daily_quotes"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {"from": start_date.strftime("%Y%m%d"), "to": end_date.strftime("%Y%m%d")}

        response = requests.get(url, headers=headers, params=params, timeout=10)

        if response.status_code in (429, 500, 502, 503, 504):
            msg = f"API Error {response.status_code}"
            raise JQuantsAPIError(msg)

        response.raise_for_status()

        data = response.json()
        quotes_data = data.get("quotes", [])

        quotes = []
        for q in quotes_data:
            # Map JQuants API keys to our model if they differ (Date vs date, etc.)
            quotes.append(
                RawQuote(
                    date=q.get("Date"),
                    open=float(q.get("Open")),
                    high=float(q.get("High")),
                    low=float(q.get("Low")),
                    close=float(q.get("Close")),
                    volume=int(q.get("Volume")),
                )
            )
        return quotes
