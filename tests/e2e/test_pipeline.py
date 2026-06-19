import os
from pathlib import Path

import pytest

from src.clients.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import query_quotes, save_quotes


@pytest.mark.live
def test_live_pipeline_e2e(tmp_path: Path) -> None:
    token = os.getenv("JQUANTS_REFRESH_TOKEN")
    if not token:
        pytest.skip("JQUANTS_REFRESH_TOKEN is not set")

    client = JQuantsClient(token)
    quotes = client.fetch_quotes("8697")  # JPX

    df = transform_quotes(quotes)

    file_path = tmp_path / "processed_quotes.parquet"
    save_quotes(df, str(file_path))

    result = query_quotes(str(file_path), "SELECT * FROM '{}'")
    assert len(result) > 0
