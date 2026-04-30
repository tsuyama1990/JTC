import os
from pathlib import Path

import pytest

from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import StorageRepository


@pytest.mark.live
def test_live_pipeline_e2e(tmp_path: Path):
    """
    End-to-End Live API Pipeline Test.
    """
    token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if (
        not token
        or token == ""
        or "Target Project Secrets" in token
        or "dummy" in token
        or token == "AAAAAAAAAAAAAAAAAAAAAAA"
    ):
        pytest.skip("Skipping live test because JQUANTS_REFRESH_TOKEN is not valid.")

    # 1. Ingestion
    client = JQuantsClient(refresh_token=token)
    quotes = client.fetch_historical_data()

    assert len(quotes) > 0, "Should have fetched some historical data."

    # 2. Transformation
    df = transform_quotes(quotes)

    assert "day_of_week" in df.columns
    assert "daily_return" in df.columns
    assert len(df) == len(quotes)

    # 3. Storage
    data_dir = tmp_path / "data"
    parquet_path = data_dir / "live_processed_quotes.parquet"

    repo = StorageRepository(storage_path=str(parquet_path))
    repo.save_data(df)

    assert parquet_path.exists(), "Parquet file should be created."

    # Verify DuckDB querying
    result_df = repo.query_data(f"SELECT * FROM '{parquet_path}' LIMIT 5")  # noqa: S608
    assert len(result_df) > 0, "Should be able to query the saved Parquet file."
