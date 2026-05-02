from pathlib import Path

import pytest
from pydantic_core import ValidationError

from src.domain_models.config import AppSettings
from src.jquants_client import JQuantsClient
from src.repository import QuoteRepository
from src.transformers import transform_quotes


@pytest.mark.live
def test_live_etl_pipeline(tmp_path: Path) -> None:
    # 1. Config validation
    try:
        settings = AppSettings() # type: ignore[call-arg]
    except ValidationError:
        pytest.skip("JQUANTS_REFRESH_TOKEN not found in environment, skipping live test.")

    refresh_token = settings.JQUANTS_REFRESH_TOKEN

    # We should also skip if a test dummy token is provided, as the sandbox CI sets it.
    if refresh_token in ("test_token", "dummy", "test"):
        pytest.skip(f"JQUANTS_REFRESH_TOKEN is set to a dummy value ('{refresh_token}'), skipping live test.")

    # 2. Extract
    client = JQuantsClient(refresh_token)
    try:
        raw_quotes = client.fetch_daily_quotes()
    except Exception as e:
        pytest.fail(f"API fetch failed: {e}")

    assert len(raw_quotes) > 0, "No data fetched from API"

    # 3. Transform
    processed_df = transform_quotes(raw_quotes)

    assert processed_df.height > 0
    assert "daily_return" in processed_df.columns
    assert "day_of_week" in processed_df.columns
    assert "is_month_start" in processed_df.columns

    # 4. Load
    repo = QuoteRepository()
    file_path = tmp_path / "live_data.parquet"
    repo.save(processed_df, str(file_path))

    assert file_path.exists()

    # 5. Verify Query
    # Disable S608 as we construct a safe internal path string
    query_str = f"SELECT COUNT(*) FROM '{file_path}'"  # noqa: S608
    count_df = repo.query(str(file_path), query_str)

    assert count_df.item(0, 0) == processed_df.height
