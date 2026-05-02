import polars as pl

from src.domain_models.models import RawQuote


def transform_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        # Return empty dataframe with correct schema
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "day_of_week": pl.Int32,
                "is_month_start": pl.Boolean,
                "is_month_end": pl.Boolean,
                "daily_return": pl.Float64,
                "intraday_return": pl.Float64,
                "overnight_return": pl.Float64,
            }
        )

    # Convert to list of dicts for Polars
    dicts = []
    for q in quotes:
        dicts.append({
            "date": q.date,
            "open": q.open,
            "high": q.high,
            "low": q.low,
            "close": q.close,
            "volume": q.volume,
        })

    df = pl.DataFrame(dicts)

    # Sort by date to ensure shift works correctly
    df = df.sort("date")

    # Calculate returns
    df = df.with_columns([
        (pl.col("close") / pl.col("close").shift(1) - 1.0).fill_null(float("nan")).alias("daily_return"),
        (pl.col("close") / pl.col("open") - 1.0).fill_null(float("nan")).alias("intraday_return"),
        (pl.col("open") / pl.col("close").shift(1) - 1.0).fill_null(float("nan")).alias("overnight_return"),
    ])

    # Calculate date features
    df = df.with_columns([
        pl.col("date").dt.weekday().cast(pl.Int32).alias("day_of_week"),
        (pl.col("date").dt.month_start() == pl.col("date")).alias("is_month_start"),
        (pl.col("date").dt.month_end() == pl.col("date")).alias("is_month_end"),
    ])

    return df
