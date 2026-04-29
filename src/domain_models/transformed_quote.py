import polars as pl

# Reference Schema for the transformed DataFrame as specified in CYCLE01/SPEC.md
TRANSFORMED_QUOTE_SCHEMA = {
    "Date": pl.Date,
    "Open": pl.Float64,
    "High": pl.Float64,
    "Low": pl.Float64,
    "Close": pl.Float64,
    "Volume": pl.Float64,
    "day_of_week": pl.Int8,
    "is_month_start": pl.Boolean,
    "is_month_end": pl.Boolean,
    "daily_return": pl.Float64,
    "intraday_return": pl.Float64,
    "overnight_return": pl.Float64,
}
