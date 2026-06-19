import polars as pl

from src.domain_models import RawQuote


def transform_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        return pl.DataFrame()

    df = pl.DataFrame([q.model_dump() for q in quotes])

    # Sort by date
    df = df.sort("date")

    # day_of_week: Monday=1, Sunday=7 in ISO, but we need 1-5 for Mon-Fri
    df = df.with_columns(pl.col("date").dt.weekday().alias("day_of_week"))

    # is_month_start: when month is different from previous row's month
    df = df.with_columns(
        (pl.col("date").dt.month() != pl.col("date").dt.month().shift(1))
        .fill_null(False)
        .alias("is_month_start")
    )

    # is_month_end: when month is different from next row's month
    df = df.with_columns(
        (pl.col("date").dt.month() != pl.col("date").dt.month().shift(-1))
        .fill_null(False)
        .alias("is_month_end")
    )

    # Returns
    df = df.with_columns(
        [
            ((pl.col("close") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias(
                "daily_return"
            ),
            ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("intraday_return"),
            ((pl.col("open") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias(
                "overnight_return"
            ),
        ]
    )

    return df
