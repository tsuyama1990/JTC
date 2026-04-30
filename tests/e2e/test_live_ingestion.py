import os
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import process_quotes


@pytest.mark.live
def test_live_ingestion_and_processing() -> None:
    refresh_token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if not refresh_token:
        pytest.skip("JQUANTS_REFRESH_TOKEN not found in environment, skipping live test.")

    client = JQuantsClient(refresh_token=refresh_token)

    # Authenticate
    client.get_id_token()

    # Fetch data for the last 12 weeks
    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(weeks=12)

    # Use Toyota (72030) as a stable test stock
    quotes = client.fetch_daily_quotes(code="72030", start_date=start_date, end_date=end_date)

    assert len(quotes) > 0, "No quotes fetched from live API"

    # Process quotes
    df = process_quotes(quotes)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == len(quotes)
    assert "day_of_week" in df.columns
    assert "daily_return" in df.columns
