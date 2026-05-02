"""Transformers Module."""

import polars as pl

from src.domain_models.models import RawQuote


def transform_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        return pl.DataFrame()

    df = pl.DataFrame([q.model_dump() for q in quotes])

    # Calculate features
    return df.with_columns(
        [
            (pl.col("close") / pl.col("close").shift(1) - 1).alias("daily_return"),
            (pl.col("close") / pl.col("open") - 1).alias("intraday_return"),
            (pl.col("open") / pl.col("close").shift(1) - 1).alias("overnight_return"),
            pl.col("date").dt.weekday().cast(pl.Int64).alias("day_of_week"),
            (pl.col("date") == pl.col("date").dt.month_start()).alias("is_month_start"),
            (pl.col("date") == pl.col("date").dt.month_end()).alias("is_month_end"),
        ]
    )
