import polars as pl
from pydantic import ValidationError

from src.domain_models.quote import ProcessedQuote, RawQuote


def transform_quotes(quotes: list[RawQuote]) -> pl.DataFrame:
    if not quotes:
        return pl.DataFrame(schema=list(ProcessedQuote.model_fields.keys()))

    data = [quote.model_dump() for quote in quotes]
    df = pl.DataFrame(data)

    df = df.with_columns(
        pl.col("date").cast(pl.String).str.strptime(pl.Date, "%Y-%m-%d").alias("date")
    )

    df = df.sort("date")

    df = df.with_columns(
        ((pl.col("close") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias("daily_return")
    )

    df = df.with_columns(
        ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("intraday_return")
    )

    df = df.with_columns(
        ((pl.col("open") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias("overnight_return")
    )

    # Use actual date logic for month end: next calendar day has a different month
    df = df.with_columns(
        pl.col("date").dt.weekday().alias("day_of_week"),
        (pl.col("date").dt.day() == 1).alias("is_month_start"),
        (pl.col("date").dt.month() != (pl.col("date") + pl.duration(days=1)).dt.month()).alias("is_month_end")
    )

    df = df.with_columns(
        pl.col("daily_return").fill_null(0.0),
        pl.col("overnight_return").fill_null(0.0)
    )

    # Explicit validation
    dicts = df.to_dicts()
    validated_data = []
    for d in dicts:
        try:
            pq = ProcessedQuote(**d)
            validated_data.append(pq.model_dump())
        except ValidationError as e:
            # We strictly enforce validation. If one fails, the whole pipeline should raise
            err_msg = f"Validation failed for ProcessedQuote: {e}"
            raise ValueError(err_msg) from e

    # Return a dataframe built from validated data to ensure exact types
    return pl.DataFrame(validated_data)
