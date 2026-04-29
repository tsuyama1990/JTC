import marimo

__generated_with = "0.23.4"
app = marimo.App()


@app.cell
def __cell_1(mo):  # type: ignore
    mo.md(
        """
        # Quick Start: Environment & Authentication

        Loading J-Quants Refresh Token from `.env`.
        """
    )


@app.cell
def __cell_2():  # type: ignore
    import os

    import marimo as mo

    from src.config.settings import get_settings

    settings = get_settings()
    if settings.jquants_refresh_token:
        has_token = True
        auth_msg = mo.md("✅ **Success**: Credentials loaded successfully.")
    else:
        has_token = False
        auth_msg = mo.md(
            "⚠️ **Warning**: JQUANTS_REFRESH_TOKEN is missing or invalid. Using Mock Mode."
        )

    return auth_msg, get_settings, has_token, mo, os, settings


@app.cell
def __cell_3(has_token, mo, settings):  # type: ignore
    import random

    from src.domain_models.raw_quote import RawQuote
    from src.ingestion.jquants_client import JQuantsClient

    if has_token:
        mo.md("Fetching real data from J-Quants API...")
        client = JQuantsClient(settings.jquants_refresh_token)
        quotes = client.get_historical_quotes(weeks=1)
    else:
        mo.md("Using synthetic Mock Data...")
        quotes = [
            RawQuote(
                Date=f"2023-10-0{i}",
                Code="1234",
                Open=100 + i,
                High=110 + i,
                Low=90 + i,
                Close=105 + i,
                Volume=1000 * i,
            )
            for i in range(1, 6)
        ]

    mo.md(f"Loaded {len(quotes)} raw quotes.")
    return JQuantsClient, RawQuote, quotes, random


@app.cell
def __cell_4(mo, quotes):  # type: ignore
    from src.transformation.feature_engine import compute_features, convert_to_polars

    df_raw = convert_to_polars(quotes)
    df_features = compute_features(df_raw)

    mo.md("### Data Transformation and Feature Engineering Validation")
    return compute_features, convert_to_polars, df_features, df_raw


@app.cell
def __cell_5(df_features, mo):  # type: ignore
    mo.ui.table(df_features.head())


@app.cell
def __cell_6(df_features, mo):  # type: ignore
    from pathlib import Path

    from src.storage.parquet_duckdb import init_duckdb_with_parquet, save_to_parquet

    parquet_path = Path("data/uat_quotes.parquet")
    save_to_parquet(df_features, parquet_path)

    conn = init_duckdb_with_parquet(parquet_path, "quotes")

    res = conn.execute(
        "SELECT day_of_week, AVG(daily_return) as avg_return FROM quotes GROUP BY day_of_week ORDER BY day_of_week"
    ).df()

    mo.md("### DuckDB Aggregation Results\n\n" + str(res))
    return Path, conn, init_duckdb_with_parquet, parquet_path, res, save_to_parquet


if __name__ == "__main__":
    app.run()
