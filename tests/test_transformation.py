import polars as pl

from src.domain_models.raw_quote import RawQuote
from src.transformation.feature_engine import compute_features, convert_to_polars


def test_convert_to_polars() -> None:
    quotes = [
        RawQuote(
            Date="2023-01-01", Code="1234", Open=100, High=110, Low=90, Close=105, Volume=1000
        ),
        RawQuote(
            Date="2023-01-02", Code="1234", Open=105, High=115, Low=95, Close=110, Volume=2000
        ),
    ]
    df = convert_to_polars(quotes)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2
    assert df["Date"].dtype == pl.Date
    assert df["Open"][0] == 100.0


def test_convert_to_polars_empty() -> None:
    df = convert_to_polars([])
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


def test_compute_features() -> None:
    quotes = [
        RawQuote(
            Date="2023-09-29", Code="1234", Open=100, High=110, Low=90, Close=105, Volume=1000
        ),
        RawQuote(
            Date="2023-10-02", Code="1234", Open=105, High=115, Low=95, Close=110, Volume=2000
        ),
        RawQuote(
            Date="2023-10-31", Code="1234", Open=110, High=120, Low=100, Close=115, Volume=3000
        ),
    ]

    df_raw = convert_to_polars(quotes)
    df = compute_features(df_raw)

    assert "day_of_week" in df.columns
    assert "daily_return" in df.columns

    assert df.filter(pl.col("Date") == pl.date(2023, 9, 29))["day_of_week"][0] == 5
    assert df.filter(pl.col("Date") == pl.date(2023, 10, 2))["day_of_week"][0] == 1

    assert not df.filter(pl.col("Date") == pl.date(2023, 9, 29))["is_month_start"][0]

    assert df.filter(pl.col("Date") == pl.date(2023, 10, 31))["is_month_end"][0]
    assert not df.filter(pl.col("Date") == pl.date(2023, 10, 2))["is_month_end"][0]

    row = df.filter(pl.col("Date") == pl.date(2023, 10, 2))
    assert abs(row["daily_return"][0] - (110 / 105 - 1.0)) < 1e-6
    assert abs(row["intraday_return"][0] - (110 / 105 - 1.0)) < 1e-6
    assert abs(row["overnight_return"][0] - 0.0) < 1e-6

    row_first = df.filter(pl.col("Date") == pl.date(2023, 9, 29))
    assert row_first["daily_return"][0] is None
    assert row_first["overnight_return"][0] is None
    assert abs(row_first["intraday_return"][0] - (105 / 100 - 1.0)) < 1e-6


def test_compute_features_zero_division() -> None:
    quotes = [
        RawQuote(Date="2023-01-01", Code="1234", Open=0, High=0, Low=0, Close=0, Volume=0),
        RawQuote(Date="2023-01-02", Code="1234", Open=0, High=0, Low=0, Close=0, Volume=0),
    ]
    df_raw = convert_to_polars(quotes)
    df = compute_features(df_raw)

    row = df.filter(pl.col("Date") == pl.date(2023, 1, 2))
    assert row["daily_return"][0] is None
    assert row["intraday_return"][0] is None
    assert row["overnight_return"][0] is None


def test_compute_features_empty() -> None:
    df = compute_features(pl.DataFrame())
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0
