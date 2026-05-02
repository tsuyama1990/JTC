from pathlib import Path
from typing import Any

import pytest
from httpx import Request, Response

from src.core.config import AppSettings
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes_to_dataframe
from src.storage.repository import DataRepository


def test_full_etl_pipeline_mocked(mocker: Any, tmp_path: Path) -> None:
    """
    Simulates the entire ETL pipeline safely offline.
    """
    # 1. Configuration
    settings = AppSettings(JQUANTS_REFRESH_TOKEN="test_token")

    # 2. Ingestion Mocking
    client = JQuantsClient(settings)

    mock_post = mocker.patch.object(client.client, "post")
    mock_post.return_value = Response(200, json={"idToken": "fake_id"}, request=Request("POST", "url"))

    mock_get = mocker.patch.object(client.client, "get")
    mock_get.return_value = Response(
        200,
        json={
            "daily_quotes": [
                {"Date": "2023-01-02", "Open": 100, "High": 110, "Low": 90, "Close": 105, "Volume": 1000},
                {"Date": "2023-01-03", "Open": 105, "High": 115, "Low": 95, "Close": 110, "Volume": 2000}
            ]
        },
        request=Request("GET", "url")
    )

    # Execute Ingestion
    raw_quotes = client.fetch_historical_quotes()
    assert len(raw_quotes) == 2

    # 3. Processing
    df = transform_quotes_to_dataframe(raw_quotes)
    assert len(df) == 2
    assert "daily_return" in df.columns

    # 4. Storage
    db_path = tmp_path / "data" / "processed_quotes.parquet"
    repo = DataRepository(db_path)

    repo.save_processed_quotes(df)
    assert db_path.exists()

    # 5. Query Validation
    # We query for the second day (Jan 3) to see the calculated return
    # The return should be 110/105 - 1 = 0.047619
    queried_df = repo.query_quotes("SELECT daily_return FROM {table} WHERE volume = 2000")
    assert len(queried_df) == 1
    assert pytest.approx(queried_df["daily_return"][0], abs=1e-5) == 0.047619

@pytest.mark.live
def test_full_etl_pipeline_live(tmp_path: Path) -> None:
    """
    Live API E2E Pipeline Verification. Skip if no real credentials.
    """
    try:
        settings = AppSettings() # type: ignore[call-arg]
    except Exception:
        pytest.skip("No real environment configurations. Skipping live test.")

    if settings.JQUANTS_REFRESH_TOKEN in ("dummy", "test_token"):
        pytest.skip("Dummy token detected. Skipping live test.")

    client = JQuantsClient(settings)
    raw_quotes = client.fetch_historical_quotes()

    if not raw_quotes:
        pytest.skip("No historical data fetched. Might be holiday or no permissions.")

    df = transform_quotes_to_dataframe(raw_quotes)

    db_path = tmp_path / "data" / "live_processed_quotes.parquet"
    repo = DataRepository(db_path)
    repo.save_processed_quotes(df)

    assert db_path.exists()
    queried_df = repo.query_quotes("SELECT * FROM {table}")
    assert len(queried_df) > 0
