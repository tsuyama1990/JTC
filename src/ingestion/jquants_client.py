from datetime import UTC, datetime, timedelta

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.domain_models.raw_quote import RawQuote


class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, refresh_token: str | None = None) -> None:
        if not refresh_token:
            msg = "JQUANTS_REFRESH_TOKEN must be provided"
            raise ValueError(msg)
        self._refresh_token = refresh_token
        self._id_token: str | None = None

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def _get_id_token(self) -> str:
        url = f"{self.BASE_URL}/token/auth_refresh"
        params = {"refreshtoken": self._refresh_token}
        response = requests.post(url, params=params, timeout=10)

        response.raise_for_status()

        data = response.json()
        if "idToken" not in data:
            msg = "Response did not contain idToken"
            raise ValueError(msg)

        id_token = data["idToken"]
        if not isinstance(id_token, str):
            msg = "idToken is not a string"
            raise TypeError(msg)
        return id_token

    def _ensure_authenticated(self) -> None:
        if not self._id_token:
            self._id_token = self._get_id_token()

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.RequestException),
    )
    def _fetch_page(self, url: str, params: dict[str, str]) -> requests.Response:
        self._ensure_authenticated()
        headers = {"Authorization": f"Bearer {self._id_token}"}
        response = requests.get(url, headers=headers, params=params, timeout=10)

        if response.status_code in {401, 403}:
            self._id_token = None
            response.raise_for_status()

        response.raise_for_status()
        return response

    def get_historical_quotes(self, weeks: int = 12) -> list[RawQuote]:
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(weeks=weeks)

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        url = f"{self.BASE_URL}/prices/daily_quotes"
        params = {"from": start_str, "to": end_str}

        all_quotes: list[RawQuote] = []

        while True:
            response = self._fetch_page(url, params)
            data = response.json()

            quotes_data = data.get("daily_quotes", [])
            for q in quotes_data:
                all_quotes.append(RawQuote(**q))

            pagination_key = data.get("pagination_key")
            if pagination_key:
                params["pagination_key"] = pagination_key
            else:
                break

        return all_quotes
