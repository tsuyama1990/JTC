import datetime
import logging

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.domain_models.config import AppSettings
from src.domain_models.quote import RawQuote

logger = logging.getLogger(__name__)


class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, settings: AppSettings) -> None:
        self.refresh_token = settings.jquants_refresh_token
        self.id_token: str | None = None

    def authenticate(self) -> None:
        url = f"{self.BASE_URL}/token/auth_refresh"
        params = {"refresh_token": self.refresh_token}
        response = requests.post(url, params=params, timeout=10)
        response.raise_for_status()
        self.id_token = response.json().get("idToken")

    def _get_headers(self) -> dict[str, str]:
        if not self.id_token:
            self.authenticate()
        return {"Authorization": f"Bearer {self.id_token}"}

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            )
        ),
    )
    def fetch_quotes(self) -> list[RawQuote]:
        if not self.id_token:
            self.authenticate()

        # 12 weeks calculation
        end_date = datetime.datetime.now(tz=datetime.UTC).date()
        start_date = end_date - datetime.timedelta(weeks=12)

        url = f"{self.BASE_URL}/prices/daily_quotes"
        params = {"from": start_date.strftime("%Y%m%d"), "to": end_date.strftime("%Y%m%d")}

        response = requests.get(url, headers=self._get_headers(), params=params, timeout=10)

        if response.status_code == 401:
            self.authenticate()
            response = requests.get(url, headers=self._get_headers(), params=params, timeout=10)

        response.raise_for_status()

        data = response.json()
        quotes = []
        for q in data.get("quotes", []):
            try:
                date_str = q["Date"]
                year, month, day = map(int, date_str.split("-"))
                date_obj = datetime.date(year, month, day)
                quote = RawQuote(
                    date=date_obj,
                    open=float(q["Open"]),
                    high=float(q["High"]),
                    low=float(q["Low"]),
                    close=float(q["Close"]),
                    volume=int(q["Volume"]),
                )
                quotes.append(quote)
            except Exception as e:
                logger.warning(f"Skipping invalid quote {q}: {e}")
                continue
        return quotes
