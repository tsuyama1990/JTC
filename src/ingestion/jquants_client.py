from datetime import UTC, datetime, timedelta

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.core.config import AppSettings
from src.core.exceptions import IngestionError
from src.domain_models.raw_quote import RawQuote


class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, settings: AppSettings) -> None:
        self.refresh_token = settings.JQUANTS_REFRESH_TOKEN
        self.id_token: str | None = None
        self.client = httpx.Client(base_url=self.BASE_URL)

    def _get_id_token(self) -> None:
        try:
            response = self.client.post(
                "/token/auth_refresh",
                params={"refresh_token": self.refresh_token}
            )
            response.raise_for_status()
            data = response.json()
            self.id_token = data.get("idToken")
            if not self.id_token:
                msg = "No idToken in response"
                raise IngestionError(msg)
        except httpx.HTTPStatusError as e:
            msg = f"Failed to get idToken: {e.response.status_code}"
            raise IngestionError(msg) from e
        except httpx.RequestError as e:
            msg = f"Network error during auth: {e}"
            raise IngestionError(msg) from e

    def _get_date_range(self) -> tuple[str, str]:
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(weeks=12)
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def _fetch_quotes_with_retry(self, params: dict[str, str]) -> httpx.Response:
        headers = {"Authorization": f"Bearer {self.id_token}"}
        response = self.client.get("/prices/daily_quotes", headers=headers, params=params)

        # Only retry on 429 or 5xx
        if response.status_code == 429 or response.status_code >= 500:
            response.raise_for_status()

        return response

    def fetch_historical_quotes(self) -> list[RawQuote]:
        if not self.id_token:
            self._get_id_token()

        start, end = self._get_date_range()
        params = {"from": start, "to": end}

        try:
            response = self._fetch_quotes_with_retry(params)
            # Raise for any other error statuses (400, 401, 403, etc)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            msg = f"Failed to fetch quotes: {e.response.status_code}"
            raise IngestionError(msg) from e
        except httpx.RequestError as e:
            msg = f"Network error during fetch: {e}"
            raise IngestionError(msg) from e

        data = response.json()
        quotes_data = data.get("daily_quotes", [])

        quotes: list[RawQuote] = []
        for q in quotes_data:
            try:
                date_obj = datetime.strptime(q["Date"], "%Y-%m-%d").replace(tzinfo=UTC).date()
                quote = RawQuote(
                    date=date_obj,
                    open=float(q["Open"]),
                    high=float(q["High"]),
                    low=float(q["Low"]),
                    close=float(q["Close"]),
                    volume=int(q["Volume"])
                )
                quotes.append(quote)
            except (ValueError, KeyError, TypeError):
                continue

        return quotes
