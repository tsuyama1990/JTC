import os

import pytest
from repository import QuotesRepository
from transformers import process_quotes

from domain_models import AppSettings
from jquants_client import JQuantsClient


@pytest.mark.live
@pytest.mark.skipif(
    not os.environ.get("JQUANTS_REFRESH_TOKEN"),
    reason="Requires JQUANTS_REFRESH_TOKEN in environment",
)
def test_live_end_to_end(tmp_path) -> None:
    # 1. Init Client
    settings = AppSettings()
    client = JQuantsClient(settings)

    # 2. Fetch raw quotes (e.g. for Toyota: 7203 or another symbol)
    raw_quotes = client.fetch_quotes(symbol="72030")  # using 72030 standard code

    # Assert we got some data
    assert len(raw_quotes) > 0

    # 3. Transform
    processed_df = process_quotes(raw_quotes)

    # Assert transformations happened
    assert "day_of_week" in processed_df.columns
    assert len(processed_df) == len(raw_quotes)

    # 4. Save to repository
    repo_dir = tmp_path / "data"
    repo = QuotesRepository(data_dir=str(repo_dir))
    repo.save(processed_df)

    # 5. Query
    queried_df = repo.query("SELECT COUNT(*) as cnt FROM 'processed_quotes.parquet'")
    assert queried_df["cnt"][0] == len(raw_quotes)
