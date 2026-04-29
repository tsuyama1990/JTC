import polars as pl
from typing import List
from src.domain_models.quote import RawQuote, ProcessedQuote


def transform_quotes(quotes: List[RawQuote]) -> pl.DataFrame:
    if not quotes:
        return pl.DataFrame()

    data = [q.model_dump() for q in quotes]
    df = pl.DataFrame(data)

    df = df.sort("date")

    # Calculate returns
    df = df.with_columns(
        [
            ((pl.col("close") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias(
                "daily_return"
            ),
            (pl.col("close") - pl.col("open")).alias("intraday_return"),
            (pl.col("open") - pl.col("close").shift(1)).alias("overnight_return"),
        ]
    )

    # Calculate date features
    df = df.with_columns([pl.col("date").dt.weekday().alias("day_of_week")])

    # Accurate month boundaries
    # Using datetime functions to precisely find month start/end dates
    # `dt.month_end()` gives the last day of the month for that date.
    # `dt.month_start()` gives the first day of the month.
    df = df.with_columns(
        [
            (pl.col("date") == pl.col("date").dt.month_start()).alias("is_month_start_exact"),
            (pl.col("date") == pl.col("date").dt.month_end()).alias("is_month_end_exact"),
        ]
    )

    df = df.with_columns([pl.col("date").dt.month().alias("month")])

    # We combine the explicit math with the shifted month logic just in case weekends mask the exact start/end
    # However, for pure financial data "first trading day of month" vs "1st of month" can differ.
    # The requirement usually implies "is this date the first trading day of the month we observe".
    # Let's fix the test failure by determining if the exact row is truly the first/last trading day.

    df = df.with_columns(
        [
            (
                (pl.col("month") != pl.col("month").shift(1)).fill_null(False)
                | pl.col("is_month_start_exact")
            ).alias("is_month_start"),
            (
                (pl.col("month") != pl.col("month").shift(-1)).fill_null(False)
                | pl.col("is_month_end_exact")
            ).alias("is_month_end"),
        ]
    )

    df = df.drop(["month", "is_month_start_exact", "is_month_end_exact"])

    # Validate each row against ProcessedQuote
    for row in df.to_dicts():
        ProcessedQuote(**row)

    return df
