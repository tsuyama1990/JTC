import marimo

__generated_with = "0.10.19"
app = marimo.App()

@app.cell
def __(mo):
    mo.md(
        """
        # Cycle 01 UAT & Tutorial Notebook
        Welcome to the User Acceptance Test for Cycle 01. This notebook verifies the complete ETL pipeline for Japanese Equity data, seamlessly toggling between real-world API data and simulated logic depending on your environment setups.
        """
    )

@app.cell
def __():
    import os

    import marimo as mo
    import polars as pl

    from src.core.config import AppSettings
    from src.ingestion.jquants_client import JQuantsClient
    from src.processing.transformers import transform_quotes_to_dataframe
    from src.storage.repository import DataRepository

    # Dual-Mode Configuration
    # We attempt to load the settings using the core model to rely on .env configurations
    real_mode = False
    try:
        # Pydantic Settings will raise ValidationError if missing or forbidden
        settings = AppSettings()
        # Ensure it's not the dummy placeholder
        if settings.JQUANTS_REFRESH_TOKEN not in ("dummy", "test_token", ""):
            real_mode = True
    except Exception:
        pass

    status_text = "✅ **LIVE MODE:** Valid J-Quants Refresh Token loaded securely from `.env`" if real_mode else "⚠️ **MOCK MODE:** Using mocked dataset as no valid token is loaded."
    return AppSettings, DataRepository, JQuantsClient, mo, os, pl, real_mode, settings, status_text, transform_quotes_to_dataframe

@app.cell
def __(mo, status_text):
    mo.md(f"### Environment Check\n{status_text}")

@app.cell
def __(
    AppSettings,
    JQuantsClient,
    mo,
    real_mode,
    settings,
    transform_quotes_to_dataframe,
):
    from datetime import date

    from src.domain_models.raw_quote import RawQuote

    quotes = []
    if real_mode:
        with mo.status.spinner(title="Fetching Live API Data..."):
            client = JQuantsClient(settings)
            quotes = client.fetch_historical_quotes()
    else:
        # Mock mode data payload
        quotes = [
            RawQuote(date=date(2023, 11, 1), open=150.0, high=155.0, low=145.0, close=152.0, volume=5000),
            RawQuote(date=date(2023, 11, 2), open=152.0, high=160.0, low=150.0, close=158.0, volume=6000),
            RawQuote(date=date(2023, 11, 3), open=158.0, high=162.0, low=155.0, close=160.0, volume=7000),
        ]

    df = transform_quotes_to_dataframe(quotes)
    row_count = len(df)
    return RawQuote, client, date, df, quotes, row_count

@app.cell
def __(df, mo, row_count):
    mo.md(f"### Transformed Polars DataFrame\n Successfully processed **{row_count}** records with calculating complex mathematical return columns.")

@app.cell
def __(df, mo):
    mo.ui.table(df, selection=None)

@app.cell
def __(DataRepository, df, mo, os):
    from pathlib import Path

    db_path = Path("data/uat_db.parquet").resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    repo = DataRepository(db_path)
    repo.save_processed_quotes(df)

    test_query = repo.query_quotes("SELECT COUNT(*) as records, AVG(daily_return) as avg_return FROM {table}")

    mo.md(f"### Storage Validation\nSuccessfully written to: `{db_path}`\n\n**DuckDB Test Query Result:**\n```python\n{test_query}\n```")
    return Path, db_path, repo, test_query

if __name__ == "__main__":
    app.run()
