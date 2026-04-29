from typing import List
import polars as pl

from domain_models import RawQuote

def process_quotes(quotes: List[RawQuote]) -> pl.DataFrame:
    """
    Transforms a list of RawQuote models into an enriched Polars DataFrame.
    """
    if not quotes:
        # Return empty df with expected schema
        return pl.DataFrame({
            "date": pl.Series(dtype=pl.String),
            "open": pl.Series(dtype=pl.Float64),
            "high": pl.Series(dtype=pl.Float64),
            "low": pl.Series(dtype=pl.Float64),
            "close": pl.Series(dtype=pl.Float64),
            "volume": pl.Series(dtype=pl.Float64),
            "daily_return": pl.Series(dtype=pl.Float64),
            "intraday_return": pl.Series(dtype=pl.Float64),
            "overnight_return": pl.Series(dtype=pl.Float64),
            "day_of_week": pl.Series(dtype=pl.Int64),
            "is_month_start": pl.Series(dtype=pl.Boolean),
            "is_month_end": pl.Series(dtype=pl.Boolean),
        })

    # Convert to list of dicts for polars, ensuring we grab defined fields
    data = [q.model_dump() for q in quotes]
    df = pl.DataFrame(data)

    # Convert date strings to Date type for time operations
    df = df.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("parsed_date")
    )

    # Sort to ensure chronological order for window operations
    df = df.sort("parsed_date")

    # Enriched columns calculation
    df = df.with_columns(
        (pl.col("close") / pl.col("close").shift(1) - 1.0).alias("daily_return"),

        pl.when(pl.col("open") == 0).then(None)
          .otherwise((pl.col("close") / pl.col("open")) - 1.0).alias("intraday_return"),

        pl.when(pl.col("close").shift(1) == 0).then(None)
          .otherwise((pl.col("open") / pl.col("close").shift(1)) - 1.0).alias("overnight_return"),

        pl.col("parsed_date").dt.weekday().alias("day_of_week"),

        (pl.col("parsed_date").dt.month() != pl.col("parsed_date").shift(1).dt.month()).fill_null(False).alias("is_month_start"),
        (pl.col("parsed_date").dt.month() != pl.col("parsed_date").shift(-1).dt.month()).fill_null(False).alias("is_month_end"),
    )

    # Drop the temporary parsed_date column
    return df.drop("parsed_date")
