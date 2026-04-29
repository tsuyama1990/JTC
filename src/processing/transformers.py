import polars as pl

from src.domain_models import ProcessedQuote, RawQuote


def process_quotes(quotes: list[RawQuote]) -> list[ProcessedQuote]:
    """
    Transforms a list of RawQuote objects into a Polars DataFrame,
    calculates derived financial features, and returns a list of ProcessedQuote objects.
    """
    if not quotes:
        return []

    quotes_dicts = [q.model_dump() for q in quotes]

    df = pl.DataFrame(quotes_dicts)

    df = df.sort("date")

    df = df.with_columns(
        daily_return=pl.col("close").pct_change(),
        intraday_return=pl.col("close") - pl.col("open"),
        overnight_return=pl.col("open") - pl.col("close").shift(1)
    )

    df = df.with_columns(
        day_of_week=pl.col("date").dt.weekday(),
        is_month_start=(pl.col("date").dt.month() != pl.col("date").dt.month().shift(1)),
        is_month_end=(pl.col("date").dt.month() != pl.col("date").dt.month().shift(-1))
    )

    df = df.with_columns(
        is_month_start=pl.col("is_month_start").fill_null(True),
        is_month_end=pl.col("is_month_end").fill_null(True)
    )

    processed_dicts = df.to_dicts()

    return [ProcessedQuote(**d) for d in processed_dicts]
