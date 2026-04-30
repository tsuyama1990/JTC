import datetime

import polars as pl
import pytest
from pydantic import ValidationError

from src.core.config import AppSettings
from src.core.exceptions import APIConnectionError
from src.domain_models.quote import ProcessedQuote
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import QuoteRepository


def test_scenario_1_no_token_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    with pytest.raises(ValidationError, match="JQUANTS_REFRESH_TOKEN"):
        AppSettings() # type: ignore[call-arg]

def test_scenario_1_network_failure_retries(monkeypatch: pytest.MonkeyPatch, requests_mock) -> None: # type: ignore[no-untyped-def]
    # Configure with token
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "valid_token")
    settings = AppSettings() # type: ignore[call-arg]

    # Simulate network outage for all endpoints
    requests_mock.post("https://api.jquants.com/v1/token/auth_refresh", exc=APIConnectionError("Simulated network failure"))

    client = JQuantsClient(settings.JQUANTS_REFRESH_TOKEN)
    with pytest.raises(APIConnectionError, match="Simulated network failure"):
        client.fetch_last_12_weeks()

def test_scenario_2_feature_engineering_and_storage(tmp_path) -> None: # type: ignore[no-untyped-def]
    # GIVEN a completely raw dataset of absolutely valid Japanese stock quotes
    # (Mocked from successful J-Quants fetch)
    raw_quotes_data = [
        {"Date": "2023-10-30", "Open": 100.0, "High": 105.0, "Low": 95.0, "Close": 100.0, "Volume": 1000},
        {"Date": "2023-10-31", "Open": 102.0, "High": 110.0, "Low": 100.0, "Close": 105.0, "Volume": 1100},
        {"Date": "2023-11-01", "Open": 104.0, "High": 115.0, "Low": 102.0, "Close": 110.0, "Volume": 1200}
    ]

    client = JQuantsClient("fake_token")
    # Bypass auth and directly use _parse_quotes
    raw_quotes = client._parse_quotes(raw_quotes_data)

    # WHEN the highly complex transformation process entirely completely finishes executing
    df = transform_quotes(raw_quotes)

    # THEN the absolutely resulting DataFrame must contain ... day_of_week, is_month_start, etc.
    assert "day_of_week" in df.columns
    assert "is_month_start" in df.columns
    assert "is_month_end" in df.columns
    assert "daily_return" in df.columns
    assert "intraday_return" in df.columns
    assert "overnight_return" in df.columns

    # Validate against ProcessedQuote (done implicitly in transform_quotes, but verify manually here)
    dicts = df.to_dicts()
    for row in dicts:
        _ = ProcessedQuote(**row)

    # Spot check 2023-10-31
    row_10_31 = df.filter(pl.col("date") == datetime.date(2023, 10, 31)).to_dicts()[0]
    assert row_10_31["day_of_week"] == 2 # Tuesday
    assert row_10_31["is_month_start"] is False
    assert row_10_31["is_month_end"] is True

    # Spot check 2023-11-01
    row_11_01 = df.filter(pl.col("date") == datetime.date(2023, 11, 1)).to_dicts()[0]
    assert row_11_01["day_of_week"] == 3 # Wednesday
    assert row_11_01["is_month_start"] is True
    assert row_11_01["is_month_end"] is False
    assert row_11_01["daily_return"] == pytest.approx((110.0 - 105.0) / 105.0)

    # AND WHEN the highly sophisticated storage repository is commanded to persist
    repo = QuoteRepository(data_dir=str(tmp_path))
    repo.save_processed_quotes(df)

    # THEN it must be formatted as Parquet and querying works perfectly
    assert repo.file_path.exists()
    assert repo.file_path.suffix == ".parquet"

    result_df = repo.query_quotes("SELECT * FROM processed_quotes WHERE day_of_week = 3")
    assert len(result_df) == 1
    assert result_df["date"][0].strftime("%Y-%m-%d") == "2023-11-01"
