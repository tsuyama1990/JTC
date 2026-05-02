import os

import httpx
import polars as pl
import pytest
from httpx import Response
from pydantic import ValidationError

from src.domain_models.config import AppSettings
from src.domain_models.quotes import ProcessedQuote, RawQuote
from src.ingestion.jquants_client import APIConnectionError, JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import StorageRepository


def test_uat_missing_token_raises_validation_error() -> None:
    """
    GIVEN the user has deliberately NOT provided a valid JQUANTS_REFRESH_TOKEN
    WHEN the user attempts to execute the primary data ingestion script
    THEN the highly strict Pydantic AppSettings configuration model must immediately intercept
    AND throw a ValidationError.
    """
    if "JQUANTS_REFRESH_TOKEN" in os.environ:
        del os.environ["JQUANTS_REFRESH_TOKEN"]

    with pytest.raises(ValidationError) as exc_info:
        AppSettings()  # type: ignore[call-arg]
    assert "JQUANTS_REFRESH_TOKEN" in str(exc_info.value)


from typing import Any
def test_uat_network_outage_retries_and_fails(mocker: Any) -> None:
    """
    GIVEN the user has configured the vital .env file with a valid token
    AND intentionally disables local internet connection (network outage)
    WHEN the user explicitly attempts to execute the ingestion script
    THEN the system must intelligently engage tenacity retry logic
    AND eventually fail gracefully, raising an Exception.
    """
    mock_post = mocker.patch("httpx.Client.post")
    mock_post.return_value = Response(
        200, request=httpx.Request("POST", "url"), json={"idToken": "fake"}
    )

    mock_get = mocker.patch("httpx.Client.get")
    mock_get.return_value = Response(
        500, request=httpx.Request("GET", "url"), json={"message": "Network Outage"}
    )

    client = JQuantsClient("valid_token")

    with pytest.raises(APIConnectionError):
        client.fetch_historical_quotes()

    assert mock_get.call_count > 1


def test_uat_transformation_produces_valid_processed_quotes() -> None:
    """
    GIVEN a highly complex, completely raw dataset of absolutely valid Japanese stock quotes
    WHEN the highly optimized Polars data transformation engine is completely finishes
    THEN the resulting dataframe must contain new columns like day_of_week, daily_return
    AND must effortlessly pass extremely strict validation rules by ProcessedQuote.
    """
    raw_quotes = [
        RawQuote(date="2023-01-31", open=100.0, high=110.0, low=90.0, close=105.0, volume=1000),
        RawQuote(date="2023-02-01", open=106.0, high=112.0, low=100.0, close=110.0, volume=1500),
    ]

    df = transform_quotes(raw_quotes)

    # We expect this test to fail since transform_quotes is not implemented yet.
    # The return should be a Polars DataFrame.
    assert isinstance(df, pl.DataFrame)

    # We can validate using ProcessedQuote by converting a row to dict
    row_dict = df.row(1, named=True)
    # Just verify it doesn't raise ValidationError
    ProcessedQuote(**row_dict)


from pathlib import Path
def test_uat_storage_persists_and_queries(tmp_path: Path) -> None:
    """
    GIVEN a beautifully completely enriched Polars DataFrame
    WHEN the repository.py module persists this exact data
    THEN it must be written as a compressed Parquet file
    AND the DuckDB SQL integration must be capable of successfully querying it immediately.
    """
    df = pl.DataFrame(
        {
            "date": ["2023-01-01"],
            "open": [100.0],
            "high": [110.0],
            "low": [90.0],
            "close": [105.0],
            "volume": [1000],
            "day_of_week": [1],
            "is_month_start": [True],
            "is_month_end": [False],
            "daily_return": [0.05],
            "intraday_return": [5.0],
            "overnight_return": [0.0],
        }
    )

    file_path = str(tmp_path / "uat_processed.parquet")
    repo = StorageRepository()

    repo.save_parquet(df, file_path)

    result_df = repo.query_duckdb(f"SELECT * FROM '{file_path}' WHERE day_of_week = 1")  # noqa: S608

    assert len(result_df) == 1
    assert result_df["date"][0] == "2023-01-01"
