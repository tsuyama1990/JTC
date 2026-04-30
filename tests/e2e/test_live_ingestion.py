import os
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from src.core.exceptions import APIConnectionError
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import process_quotes


@pytest.mark.live
def test_live_ingestion_and_processing() -> None:
    refresh_token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if not refresh_token:
        pytest.skip("JQUANTS_REFRESH_TOKEN not found in environment, skipping live test.")

    client = JQuantsClient(refresh_token=refresh_token)

    try:
        # Authenticate
        client.get_id_token()

        # Fetch data for the last 12 weeks
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(weeks=12)

        # Use Toyota (72030) as a stable test stock
        quotes = client.fetch_daily_quotes(code="72030", start_date=start_date, end_date=end_date)

        # JQuants returns empty on weekends or some holidays, but over 12 weeks we should have some
        assert len(quotes) > 0, "No quotes fetched from live API"

        # Process quotes
        df = process_quotes(quotes)

        assert isinstance(df, pl.DataFrame)
        assert len(df) == len(quotes)
        assert "day_of_week" in df.columns
        assert "daily_return" in df.columns
    except APIConnectionError as e:
        # If the API connection fails here, specifically check if it's the token missing
        if "ID token not found in response" in str(e) or "Failed to authenticate" in str(e):
            pytest.skip(
                f"JQUANTS_REFRESH_TOKEN is invalid or API is rejecting it, skipping live test. Details: {e}"
            )
        else:
            raise
