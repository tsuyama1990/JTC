from src.core.config import AppSettings
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import QuoteRepository


def main() -> None:
    settings = AppSettings()
    client = JQuantsClient(refresh_token=settings.JQUANTS_REFRESH_TOKEN)
    raw_quotes = client.fetch_last_12_weeks()
    df = transform_quotes(raw_quotes)
    repo = QuoteRepository(data_dir="data")
    repo.save_processed_quotes(df)
    result_df = repo.query_quotes("SELECT * FROM processed_quotes WHERE day_of_week = 3")
    print(result_df) # noqa: T201

if __name__ == "__main__":
    main()
