from pathlib import Path
from typing import Any

from httpx import Request, Response

from src.core.config import AppSettings
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes_to_dataframe
from src.storage.repository import DataRepository


def test_cycle01_user_acceptance_workflow(mocker: Any, tmp_path: Path) -> None:
    """
    User Acceptance Test:
    User runs the ETL script entirely as described in the requirements.
    We mock the network request for pure reliability.
    """
    settings = AppSettings(JQUANTS_REFRESH_TOKEN="test_token")

    # User configures client
    client = JQuantsClient(settings)

    # Mocking external world
    mocker.patch.object(client.client, "post", return_value=Response(200, json={"idToken": "fake"}, request=Request("POST", "url")))
    mocker.patch.object(client.client, "get", return_value=Response(
        200,
        json={
            "daily_quotes": [
                {"Date": "2023-11-01", "Open": 150.0, "High": 155.0, "Low": 145.0, "Close": 152.0, "Volume": 5000},
                {"Date": "2023-11-02", "Open": 152.0, "High": 160.0, "Low": 150.0, "Close": 158.0, "Volume": 6000}
            ]
        },
        request=Request("GET", "url")
    ))

    quotes = client.fetch_historical_quotes()
    assert len(quotes) == 2

    df = transform_quotes_to_dataframe(quotes)
    assert len(df) == 2
    assert "day_of_week" in df.columns

    db_path = tmp_path / "storage" / "db.parquet"
    repo = DataRepository(db_path)
    repo.save_processed_quotes(df)

    res = repo.query_quotes("SELECT COUNT(*) as c FROM {table}")
    assert res["c"][0] == 2
