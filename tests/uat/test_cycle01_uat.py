from datetime import UTC
from pathlib import Path

import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from src.domain_models.config import AppSettings
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import process_quotes
from src.storage.repository import StorageRepository


def test_uat_scenario_1_missing_token_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    with pytest.raises(ValidationError):
        AppSettings()  # type: ignore[call-arg]


# The UAT scenario requires mocking JQuants for full E2E offline test
# because real API may not be available or may hit rate limits in CI
def test_uat_scenario_1_and_2_full_pipeline(
    mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # 1. Mock the AppSettings to have a token
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "valid_fake_token")
    settings = AppSettings()  # type: ignore[call-arg]

    # 2. Mock API Client responses
    mock_post = mocker.patch("httpx.post")
    mock_post_response = mocker.Mock()
    mock_post_response.status_code = 200
    mock_post_response.json.return_value = {"idToken": "fake_id_token"}
    mock_post.return_value = mock_post_response

    mock_get = mocker.patch("httpx.get")
    mock_get_response = mocker.Mock()
    mock_get_response.status_code = 200
    mock_get_response.json.return_value = {
        "daily_quotes": [
            {
                "Date": "2023-10-31",  # Tuesday, Month End
                "Open": 100.0,
                "High": 110.0,
                "Low": 90.0,
                "Close": 100.0,
                "Volume": 1000,
            },
            {
                "Date": "2023-11-01",  # Wednesday, Month Start
                "Open": 105.0,
                "High": 115.0,
                "Low": 100.0,
                "Close": 110.0,
                "Volume": 1200,
            },
        ]
    }
    mock_get.return_value = mock_get_response

    # Run Pipeline
    client = JQuantsClient(refresh_token=settings.JQUANTS_REFRESH_TOKEN)
    client.get_id_token()
    from datetime import datetime

    quotes = client.fetch_daily_quotes(
        code="86970",
        start_date=datetime(2023, 10, 31, tzinfo=UTC),
        end_date=datetime(2023, 11, 1, tzinfo=UTC),
    )

    df = process_quotes(quotes)

    parquet_path = tmp_path / "data" / "processed_quotes.parquet"
    repo = StorageRepository(str(parquet_path))
    repo.save(df)

    # Assert Storage
    assert parquet_path.exists()

    # Assert Querying
    result_df = repo.query("SELECT * FROM 'data' WHERE day_of_week = 3")
    assert len(result_df) == 1

    row = result_df.row(0, named=True)
    assert row["is_month_start"] is True
    assert row["daily_return"] == pytest.approx(0.1)
