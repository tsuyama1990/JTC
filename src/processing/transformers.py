import logging

import polars as pl

from src.domain_models.quotes import RawQuote

logger = logging.getLogger(__name__)


def transform_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    """Transforms a list of RawQuote objects into an enriched Polars DataFrame."""
    if not quotes:
        logger.warning("No quotes provided for transformation. Returning empty DataFrame.")
        # Return empty dataframe with expected schema
        schema = {
            "date": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "day_of_week": pl.Int64,
            "is_month_start": pl.Boolean,
            "is_month_end": pl.Boolean,
            "daily_return": pl.Float64,
            "intraday_return": pl.Float64,
            "overnight_return": pl.Float64,
        }
        return pl.DataFrame(schema=schema)

    # Convert list of Pydantic models to a list of dicts
    data = [quote.model_dump() for quote in quotes]

    # Load into Polars DataFrame
    df = pl.DataFrame(data)

    # Ensure date is parsed correctly (sometimes timezone aware dates get loaded strangely depending on Python version)
    # Usually it works out of the box, but let's cast if needed.
    # For now, assume pl.DataFrame parses datetime objects well.

    # 1. Calendar Features
    # Extract day of week (1=Monday, 7=Sunday). We map 1-5 for trading days.
    # We use `.dt.weekday()` which returns 1 for Monday to 7 for Sunday
    df = df.with_columns(
        [
            pl.col("date").dt.weekday().alias("day_of_week"),
            # is_month_start: True if the day is the 1st of the month.
            # But wait, in trading days, the 1st trading day might not be the 1st calendar day.
            # However, usually `is_month_start` implies the calendar month start. If it means trading month start,
            # we can detect if month of current row != month of previous row.
            # Let's use the trading month start/end logic as it's more robust for market anomalies:
            (pl.col("date").dt.month() != pl.col("date").dt.month().shift(1))
            .fill_null(False)
            .alias("is_month_start"),
            (pl.col("date").dt.month() != pl.col("date").dt.month().shift(-1))
            .fill_null(False)
            .alias("is_month_end"),
        ]
    )

    # 2. Return Metrics
    # Note: Shift(1) gives the previous row.
    return df.with_columns(
        [
            (pl.col("close") / pl.col("close").shift(1) - 1.0).alias("daily_return"),
            (pl.col("close") / pl.col("open") - 1.0).alias("intraday_return"),
            (pl.col("open") / pl.col("close").shift(1) - 1.0).alias("overnight_return"),
        ]
    )
