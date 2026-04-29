import math
from datetime import date

import polars as pl

from src.domain_models import RawQuote
from src.processing.transformers import transform_quotes


def test_transform_quotes() -> None:
    # 2023-01-31 is Tuesday (month end)
    # 2023-02-01 is Wednesday (month start)
    quotes = [
        RawQuote(
            date=date(2023, 1, 31),
            code="1234",
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1000.0,
        ),
        RawQuote(
            date=date(2023, 2, 1),
            code="1234",
            open=106.0,
            high=115.0,
            low=102.0,
            close=110.0,
            volume=1200.0,
        ),
    ]

    df = transform_quotes(quotes)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2

    # Check flags
    assert df.select("is_month_end").to_series()[0] == True
    assert df.select("is_month_start").to_series()[0] == False
    assert df.select("is_month_end").to_series()[1] == False
    assert df.select("is_month_start").to_series()[1] == True

    # Check day of week
    assert df.select("day_of_week").to_series()[0] == 2  # Tuesday
    assert df.select("day_of_week").to_series()[1] == 3  # Wednesday

    # Check returns
    # daily_return = (close - prev_close) / prev_close
    daily_returns = df.select("daily_return").to_series().to_list()
    assert daily_returns[0] is None or math.isnan(daily_returns[0])
    assert abs(daily_returns[1] - ((110.0 - 105.0) / 105.0)) < 1e-6

    # intraday_return = (close - open) / open
    intraday_returns = df.select("intraday_return").to_series().to_list()
    assert abs(intraday_returns[0] - ((105.0 - 100.0) / 100.0)) < 1e-6
    assert abs(intraday_returns[1] - ((110.0 - 106.0) / 106.0)) < 1e-6

    # overnight_return = (open - prev_close) / prev_close
    overnight_returns = df.select("overnight_return").to_series().to_list()
    assert overnight_returns[0] is None or math.isnan(overnight_returns[0])
    assert abs(overnight_returns[1] - ((106.0 - 105.0) / 105.0)) < 1e-6
