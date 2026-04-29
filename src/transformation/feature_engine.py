import polars as pl

from src.domain_models.raw_quote import RawQuote


def convert_to_polars(quotes: list[RawQuote]) -> pl.DataFrame:
    dicts = [q.model_dump() for q in quotes]

    df = pl.DataFrame(dicts)

    if df.is_empty():
        return df

    if df["Date"].dtype != pl.Date:
        df = df.with_columns(pl.col("Date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))

    return df


def compute_features(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    if "Code" in df.columns:
        df = df.sort(["Code", "Date"])
        group_by_cols = ["Code"]
    else:
        df = df.sort(["Date"])
        group_by_cols = None

    df = df.with_columns(
        pl.col("Date").dt.weekday().cast(pl.Int8).alias("day_of_week"),
        (pl.col("Date").dt.day() == 1).alias("is_month_start"),
        (pl.col("Date").dt.month() != (pl.col("Date") + pl.duration(days=1)).dt.month()).alias(
            "is_month_end"
        ),
    )

    if group_by_cols:
        prev_close = pl.col("Close").shift(1).over(group_by_cols)
    else:
        prev_close = pl.col("Close").shift(1)

    safe_prev_close = pl.when(prev_close == 0).then(None).otherwise(prev_close)
    safe_open = pl.when(pl.col("Open") == 0).then(None).otherwise(pl.col("Open"))

    return df.with_columns(
        (pl.col("Close") / safe_prev_close - 1.0).alias("daily_return"),
        (pl.col("Close") / safe_open - 1.0).alias("intraday_return"),
        (pl.col("Open") / safe_prev_close - 1.0).alias("overnight_return"),
    )
