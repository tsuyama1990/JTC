import polars as pl

from src.domain_models.quote import RawQuote


def process_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        return pl.DataFrame()

    df = pl.DataFrame([q.model_dump() for q in quotes])

    df = df.with_columns(pl.col("date").dt.weekday().alias("day_of_week"))

    df = df.with_columns(
        (pl.col("date").dt.month() != pl.col("date").shift(1).dt.month())
        .fill_null(False)
        .alias("is_month_start"),
        (pl.col("date").dt.month() != pl.col("date").shift(-1).dt.month())
        .fill_null(False)
        .alias("is_month_end"),
    )

    return df.with_columns(
        ((pl.col("close") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias(
            "daily_return"
        ),
        ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("intraday_return"),
        ((pl.col("open") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias(
            "overnight_return"
        ),
    )
