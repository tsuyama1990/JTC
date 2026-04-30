import math

import polars as pl

from src.domain_models.quotes import RawQuote


def test_transform_quotes() -> None:
    from src.transformers import transform_quotes

    # Jan 1 2024 is Monday. We'll use sequence to test dates.
    # Jan 31 2024 is Wednesday (month end).
    quotes = [
        RawQuote(date="2024-01-30", open=100.0, high=110.0, low=90.0, close=105.0, volume=1000),
        RawQuote(date="2024-01-31", open=105.0, high=115.0, low=95.0, close=110.25, volume=1200),
        RawQuote(date="2024-02-01", open=110.0, high=120.0, low=100.0, close=108.0, volume=1500),
    ]

    df = transform_quotes(quotes)

    assert isinstance(df, pl.DataFrame)

    # 2024-01-30 is Tuesday (2)
    # 2024-01-31 is Wednesday (3)
    # 2024-02-01 is Thursday (4)
    assert df["day_of_week"].to_list() == [2, 3, 4]

    # 2024-01-31 is month end
    assert df["is_month_end"].to_list() == [False, True, False]

    # 2024-02-01 is month start
    assert df["is_month_start"].to_list() == [False, False, True]

    # Daily Return is pct_change of close
    # For Jan 31: (110.25 - 105.0) / 105.0 = 0.05
    assert math.isclose(df["daily_return"].to_list()[1], 0.05, rel_tol=1e-5)

    # Intraday return is close - open
    # For Jan 30: 105.0 - 100.0 = 5.0
    assert math.isclose(df["intraday_return"].to_list()[0], 5.0, rel_tol=1e-5)

    # Overnight return is open - previous day's close
    # For Jan 31: 105.0 - 105.0 = 0.0
    assert math.isclose(df["overnight_return"].to_list()[1], 0.0, rel_tol=1e-5)
    # For Feb 1: 110.0 - 110.25 = -0.25
    assert math.isclose(df["overnight_return"].to_list()[2], -0.25, rel_tol=1e-5)

    # Also first element of daily return and overnight return should be null (or 0, handling depends on polars pct_change, but typically null which we should fill with 0.0 to match the float requirement and validate strictly).
    assert df["daily_return"].to_list()[0] == 0.0
    assert df["overnight_return"].to_list()[0] == 0.0
