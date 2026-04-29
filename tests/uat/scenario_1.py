import os
from typing import Any

import pytest
from pydantic import ValidationError

from src.domain_models import AppSettings


@pytest.mark.live
def test_scenario_1_no_token() -> None:
    """
    Scenario 1: UAT verify system fails fast if JQUANTS_REFRESH_TOKEN is missing.
    """
    original_token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if "JQUANTS_REFRESH_TOKEN" in os.environ:
        del os.environ["JQUANTS_REFRESH_TOKEN"]

    try:
        with pytest.raises(ValidationError):
            AppSettings()  # type: ignore[call-arg]
    finally:
        if original_token is not None:
            os.environ["JQUANTS_REFRESH_TOKEN"] = original_token


@pytest.mark.live
def test_scenario_1_live_ingestion(tmp_path: Any) -> None:
    """
    Scenario 1: Live integration test (requires token).
    """
    from src.ingestion.jquants_client import JQuantsClient

    token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if not token:
        pytest.skip("JQUANTS_REFRESH_TOKEN not found, skipping live test.")

    client = JQuantsClient(token)
    quotes = client.fetch_historical_quotes_12_weeks()

    assert len(quotes) > 0
