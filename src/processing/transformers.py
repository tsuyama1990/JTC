import polars as pl

from src.domain_models.raw_quote import RawQuote


def transform_quotes_to_dataframe(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        # Return empty dataframe with correct schema if input is empty
        schema = {
            "date": pl.Date,
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

    # Convert Pydantic models to dicts
    dicts = [q.model_dump() for q in quotes]

    # Load into Polars
    df = pl.DataFrame(dicts).sort("date")

    # Apply calculations
    return df.with_columns(
        day_of_week=pl.col("date").dt.weekday(), # 1 = Monday, 7 = Sunday
        is_month_start=(pl.col("date") == pl.col("date").dt.month_start()),
        is_month_end=(pl.col("date") == pl.col("date").dt.month_end()),
        daily_return=(pl.col("close") / pl.col("close").shift(1) - 1.0),
        intraday_return=(pl.col("close") / pl.col("open") - 1.0),
        overnight_return=(pl.col("open") / pl.col("close").shift(1) - 1.0)
    )
