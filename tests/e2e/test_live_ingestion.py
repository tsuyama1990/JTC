import os

import pytest

from src.core.config import AppSettings
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes_to_dataframe


@pytest.mark.live
@pytest.mark.skipif(
    not os.getenv("JQUANTS_REFRESH_TOKEN") or os.getenv("JQUANTS_REFRESH_TOKEN") in ("dummy", "test_token"),
    reason="Requires live API token"
)
def test_live_ingestion_api() -> None:
    """
    Validates E2E functionality with the actual J-Quants API.
    Zero token logs are permitted.
    """
    settings = AppSettings() # type: ignore[call-arg]
    client = JQuantsClient(settings)

    # Check that ingestion succeeds and data is returned
    quotes = client.fetch_historical_quotes()
    assert len(quotes) > 0, "No data fetched from live API"

    # Process the quotes to dataframe
    df = transform_quotes_to_dataframe(quotes)

    # Verify basic transformation completeness and expected time windows
    assert len(df) > 0
    assert "daily_return" in df.columns
    assert "is_month_start" in df.columns

    # Calculate difference between min and max date ensuring we got substantial data (around 12 weeks worth)
    # The API might be restricted by holidays/weekends, but we should definitely have more than a few days
    min_date = df["date"].min()
    max_date = df["date"].max()

    if min_date and max_date:
        delta = max_date - min_date # type: ignore[operator]
        assert getattr(delta, "days", 0) > 30
