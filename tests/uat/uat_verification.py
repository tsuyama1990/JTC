import os
import sys
from pathlib import Path

from src.domain_models.config import AppSettings
from src.jquants_client import JQuantsClient
from src.repository import query_quotes, save_quotes
from src.transformers import transform_quotes


def run_uat() -> None:
    print("Starting UAT Verification...")  # noqa: T201
    token = os.environ.get("JQUANTS_REFRESH_TOKEN")
    if not token or token in {"dummy_token", "your_refresh_token_here"}:
        print("ERROR: Valid JQUANTS_REFRESH_TOKEN is required for UAT.")  # noqa: T201
        sys.exit(1)

    settings = AppSettings(jquants_refresh_token=token)
    client = JQuantsClient(settings)

    print("1. Fetching data from JQuants...")  # noqa: T201
    quotes = client.fetch_quotes()
    print(f"   Success: Fetched {len(quotes)} raw quotes.")  # noqa: T201

    print("2. Transforming data...")  # noqa: T201
    df = transform_quotes(quotes)
    print(f"   Success: Transformed into {df.height} rows and {df.width} columns.")  # noqa: T201
    print("   Sample Data:")  # noqa: T201
    print(df.head(5))  # noqa: T201

    output_path = Path("data/processed_quotes.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"3. Saving data to {output_path}...")  # noqa: T201
    save_quotes(df, str(output_path))
    print("   Success: Data saved.")  # noqa: T201

    print("4. Querying saved data...")  # noqa: T201
    results = query_quotes(str(output_path), "SELECT * FROM data LIMIT 3")
    print(f"   Success: Retrieved {len(results)} rows via DuckDB.")  # noqa: T201

    print("UAT Verification Complete!")  # noqa: T201


if __name__ == "__main__":
    run_uat()
