import os
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import ValidationError

from src.core.config import AppSettings
from src.jquants_client import JQuantsClient


def test_uat_missing_token_behavior() -> None:
    # GIVEN the user has successfully cloned the project repository...
    # AND the user has deliberately NOT provided a valid JQUANTS_REFRESH_TOKEN
    original = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if "JQUANTS_REFRESH_TOKEN" in os.environ:
        del os.environ["JQUANTS_REFRESH_TOKEN"]

    try:
        # WHEN the user attempts to execute the primary data ingestion script
        # THEN the highly strict Pydantic AppSettings must immediately and forcefully intercept
        with pytest.raises(ValidationError):
            AppSettings() # type: ignore[call-arg]
    finally:
        if original is not None:
            os.environ["JQUANTS_REFRESH_TOKEN"] = original

def test_uat_network_failure_retry_behavior(mocker: MagicMock) -> None:
    # GIVEN valid token
    # AND network outage
    mock_post = mocker.patch("httpx.Client.post")
    mock_post.side_effect = httpx.ConnectError("Network is down")

    # WHEN attempting to execute
    client = JQuantsClient(refresh_token="mock_refresh_token")

    # Patch sleep to not wait 10 seconds during tests
    mocker.patch("tenacity.nap.time.sleep")

    # THEN system must engage retry logic and eventually fail gracefully with APIConnectionError
    from src.jquants_client import APIConnectionError
    with pytest.raises(APIConnectionError, match="Connection error"):
        client.fetch_daily_quotes(code="86970")

    # verify retries
    assert mock_post.call_count == 5

# UAT Scenario 2 is covered by the integration/unit tests for transformations and schemas
