import os
from pathlib import Path

import pytest

from src.core.config import AppSettings
from src.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.repository.repository import query_quotes, save_quotes


@pytest.mark.live
def test_live_end_to_end(tmp_path: Path) -> None:
    # Check if we have the token
    token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if not token or token == "dummy_token":  # noqa: S105
        pytest.skip("JQUANTS_REFRESH_TOKEN is missing or dummy. Skipping live test.")

    # 1. Configuration Check
    settings = AppSettings()  # type: ignore[call-arg]

    # 2. Ingestion
    client = JQuantsClient(refresh_token=settings.JQUANTS_REFRESH_TOKEN)
    raw_quotes = client.fetch_daily_quotes(code="86970") # Japan Exchange Group

    assert len(raw_quotes) > 0

    # 3. Transformation
    df = transform_quotes(raw_quotes)

    assert df.height == len(raw_quotes)
    assert "daily_return" in df.columns
    assert "day_of_week" in df.columns
    assert "is_month_start" in df.columns

    # 4. Storage
    file_path = tmp_path / "processed_quotes.parquet"
    save_quotes(df, str(file_path))

    assert file_path.exists()

    # 5. Query
    result = query_quotes(str(file_path), "SELECT * FROM data LIMIT 5")
    assert result.height > 0
