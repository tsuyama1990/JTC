import os
from pathlib import Path

import pytest

from src.domain_models.config import AppSettings
from src.jquants_client import JQuantsClient
from src.repository import query_quotes, save_quotes
from src.transformers import transform_quotes


@pytest.mark.live
def test_live_pipeline(tmp_path: Path) -> None:
    token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if not token or token in {"dummy_token", "your_refresh_token_here"}:
        pytest.skip("JQUANTS_REFRESH_TOKEN not set or invalid, skipping live E2E test")

    settings = AppSettings(jquants_refresh_token=token)
    client = JQuantsClient(settings)

    # 1. Ingestion
    raw_quotes = client.fetch_quotes()
    assert len(raw_quotes) > 0

    # 2. Transformation
    df = transform_quotes(raw_quotes)
    assert df.height > 0
    assert "day_of_week" in df.columns

    # 3. Storage
    file_path = tmp_path / "live_processed_quotes.parquet"
    save_quotes(df, str(file_path))
    assert file_path.exists()

    # 4. Query
    results = query_quotes(str(file_path), "SELECT * FROM data LIMIT 5")
    assert len(results) <= 5
