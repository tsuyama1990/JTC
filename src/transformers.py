import polars as pl

from src.domain_models.quotes import RawQuote


def transform_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        return pl.DataFrame(
            schema={
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
        )

    # Convert to Polars DataFrame
    dicts = [q.model_dump() for q in quotes]
    df = pl.DataFrame(dicts)

    # Cast date string to date/datetime if needed
    if df["date"].dtype == pl.String:
        df = df.with_columns(pl.col("date").str.to_datetime("%Y-%m-%d"))

    return df.with_columns(
        [
            pl.col("date").dt.weekday().alias("day_of_week"),
            (pl.col("date").dt.month() != pl.col("date").dt.month().shift(1))
            .fill_null(False)
            .alias("is_month_start"),
            (pl.col("date").dt.month() != pl.col("date").dt.month().shift(-1))
            .fill_null(False)
            .alias("is_month_end"),
            ((pl.col("close") - pl.col("close").shift(1)) / pl.col("close").shift(1))
            .fill_null(0.0)
            .alias("daily_return"),
            (pl.col("close") - pl.col("open")).alias("intraday_return"),
            (pl.col("open") - pl.col("close").shift(1)).fill_null(0.0).alias("overnight_return"),
        ]
    )
