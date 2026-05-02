import polars as pl

from src.domain_models.quotes import RawQuote


def transform_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        return pl.DataFrame(
            schema={
                "date": pl.Utf8,
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

    dicts = [q.model_dump() for q in quotes]
    df = pl.DataFrame(dicts)

    df = df.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("parsed_date"))
    df = df.sort("parsed_date")

    df = df.with_columns(
        [
            ((pl.col("close") / pl.col("close").shift(1)) - 1.0).alias("daily_return"),
            (pl.col("close") - pl.col("open")).alias("intraday_return"),
            (pl.col("open") - pl.col("close").shift(1)).alias("overnight_return"),
        ]
    )

    # day_of_week: Monday=1, Friday=5. Polars dt.weekday() -> 1=Monday...7=Sunday
    df = df.with_columns(pl.col("parsed_date").dt.weekday().alias("day_of_week"))

    df = df.with_columns(
        [
            (pl.col("parsed_date") == pl.col("parsed_date").dt.month_start()).alias(
                "is_month_start"
            ),
            (pl.col("parsed_date") == pl.col("parsed_date").dt.month_end()).alias("is_month_end"),
        ]
    )

    return df.drop("parsed_date")
