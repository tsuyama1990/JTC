import os
from pathlib import Path

import pytest
import responses

from src.config.settings import get_settings
from src.ingestion.jquants_client import JQuantsClient
from src.storage.parquet_duckdb import init_duckdb_with_parquet, save_to_parquet
from src.transformation.feature_engine import compute_features, convert_to_polars


@responses.activate
def test_pipeline_integration_mock(tmp_path: Path) -> None:
    responses.add(
        responses.POST,
        "https://api.jquants.com/v1/token/auth_refresh",
        json={"idToken": "fake_id_token"},
        status=200,
    )

    mock_data = {
        "daily_quotes": [
            {
                "Date": "2023-10-02",
                "Code": "1234",
                "Open": 100,
                "High": 110,
                "Low": 90,
                "Close": 105,
                "Volume": 1000,
            },
            {
                "Date": "2023-10-03",
                "Code": "1234",
                "Open": 105,
                "High": 115,
                "Low": 95,
                "Close": 110,
                "Volume": 2000,
            },
        ]
    }

    responses.add(
        responses.GET, "https://api.jquants.com/v1/prices/daily_quotes", json=mock_data, status=200
    )

    client = JQuantsClient("fake_refresh_token")
    quotes = client.get_historical_quotes(weeks=1)

    df_raw = convert_to_polars(quotes)
    df_transformed = compute_features(df_raw)

    assert "daily_return" in df_transformed.columns
    assert len(df_transformed) == 2

    parquet_path = tmp_path / "processed_quotes.parquet"
    save_to_parquet(df_transformed, parquet_path)

    assert parquet_path.exists()

    conn = init_duckdb_with_parquet(parquet_path, table_name="test_quotes")

    result = conn.execute("SELECT COUNT(*) FROM test_quotes").fetchone()
    assert result is not None
    assert result[0] == 2

    result_avg_return = conn.execute(
        "SELECT AVG(daily_return) FROM test_quotes WHERE daily_return IS NOT NULL"
    ).fetchone()
    assert result_avg_return is not None
    assert abs(result_avg_return[0] - (110 / 105 - 1.0)) < 1e-6


@pytest.mark.live
def test_pipeline_integration_live(tmp_path: Path) -> None:
    if not os.getenv("JQUANTS_REFRESH_TOKEN"):
        pytest.skip("Live token not set")

    settings = get_settings()
    assert settings.jquants_refresh_token is not None

    client = JQuantsClient(settings.jquants_refresh_token)

    quotes = client.get_historical_quotes(weeks=1)
    assert len(quotes) > 0

    df_raw = convert_to_polars(quotes)
    df_transformed = compute_features(df_raw)

    parquet_path = tmp_path / "live_quotes.parquet"
    save_to_parquet(df_transformed, parquet_path)

    conn = init_duckdb_with_parquet(parquet_path, table_name="live_quotes")
    result = conn.execute("SELECT COUNT(*) FROM live_quotes").fetchone()
    assert result is not None
    assert result[0] == len(quotes)
