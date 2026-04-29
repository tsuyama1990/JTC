import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell
def __():
    import os
    import sys

    import marimo as mo
    import polars as pl

    # Ensure src is in the path
    sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

    from repository import QuotesRepository
    from transformers import process_quotes

    from domain_models import AppSettings, ProcessedQuote, RawQuote
    from jquants_client import JQuantsClient

    mo.md("# J-Quants Data Ingestion and Transformation UAT")
    return (
        AppSettings,
        JQuantsClient,
        ProcessedQuote,
        QuotesRepository,
        RawQuote,
        mo,
        os,
        pl,
        process_quotes,
        sys,
    )


@app.cell
def __(AppSettings, mo, os):
    mo.md("## Configuration Verification")

    try:
        settings = AppSettings()
        mo.md(
            f"✅ Configuration Loaded. Token starts with: {settings.JQUANTS_REFRESH_TOKEN[:4]}..."
        )
    except Exception as e:
        mo.md(f"❌ Configuration Error: {e}")
        settings = None
    return (settings,)


@app.cell
def __(RawQuote, mo):
    mo.md("## Mock Data Generation (Network Resilient)")

    mock_quotes = [
        RawQuote(date="2023-10-30", open=100.0, high=110.0, low=95.0, close=105.0, volume=1000.0),
        RawQuote(date="2023-10-31", open=105.0, high=115.0, low=100.0, close=110.0, volume=1500.0),
        RawQuote(date="2023-11-01", open=110.0, high=120.0, low=105.0, close=115.0, volume=2000.0),
        RawQuote(date="2023-11-02", open=115.0, high=125.0, low=110.0, close=120.0, volume=2500.0),
    ]

    mo.md(f"Created {len(mock_quotes)} mock `RawQuote` objects for testing transformations.")
    return (mock_quotes,)


@app.cell
def __(mock_quotes, mo, process_quotes):
    mo.md("## Data Transformation")

    df = process_quotes(mock_quotes)

    mo.ui.table(df)
    return (df,)


@app.cell
def __(df, mo):
    mo.md("## Verification of Transformations")

    # Display the specific rows showing month transition
    transition_df = df.filter(df["date"].is_in(["2023-10-31", "2023-11-01"]))
    mo.ui.table(transition_df)
    return (transition_df,)


@app.cell
def __(QuotesRepository, df, mo, os):
    mo.md("## Storage and Query Verification")

    data_path = os.path.join(os.getcwd(), "..", "..", "data")
    repo = QuotesRepository(data_dir=data_path)

    repo.save(df)
    mo.md(f"✅ Data saved to Parquet in `{data_path}`")
    return data_path, repo


@app.cell
def __(mo, repo):
    mo.md("### Querying via DuckDB")

    queried_df = repo.query(
        "SELECT date, close, daily_return FROM 'processed_quotes.parquet' WHERE is_month_start = true"
    )
    mo.ui.table(queried_df)
    return (queried_df,)


if __name__ == "__main__":
    app.run()
