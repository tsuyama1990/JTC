import pytest

from src.domain_models.quotes import RawQuote
from src.processing.transformers import transform_quotes


def test_transform_quotes() -> None:
    # 2023-01-31 is Tuesday (day_of_week 2), month end
    # 2023-02-01 is Wednesday (day_of_week 3), month start
    # 2023-02-02 is Thursday (day_of_week 4)
    raw_quotes = [
        RawQuote(date="2023-01-31", open=100.0, high=110.0, low=90.0, close=105.0, volume=1000),
        RawQuote(date="2023-02-01", open=106.0, high=112.0, low=100.0, close=110.0, volume=1500),
        RawQuote(date="2023-02-02", open=108.0, high=115.0, low=105.0, close=112.0, volume=1200),
    ]

    df = transform_quotes(raw_quotes)

    # Validate output schema via ProcessedQuote?
    # The output is a pl.DataFrame according to the spec, but we can check if fields exist
    assert "daily_return" in df.columns
    assert "intraday_return" in df.columns
    assert "overnight_return" in df.columns
    assert "day_of_week" in df.columns
    assert "is_month_start" in df.columns
    assert "is_month_end" in df.columns

    # Verify daily return (pct_change equivalent: (close / prev_close) - 1)
    # Day 1: null
    # Day 2: 110 / 105 - 1 = 0.047619...
    daily_returns = df["daily_return"].to_list()
    assert daily_returns[0] is None
    assert pytest.approx(daily_returns[1], 0.0001) == (110.0 / 105.0) - 1.0

    # Verify intraday return (close - open)
    # Day 1: 105 - 100 = 5.0
    intraday_returns = df["intraday_return"].to_list()
    assert intraday_returns[0] == 5.0
    assert intraday_returns[1] == 4.0

    # Verify overnight return (open - prev_close)
    # Day 1: null
    # Day 2: 106 - 105 = 1.0
    overnight_returns = df["overnight_return"].to_list()
    assert overnight_returns[0] is None
    assert overnight_returns[1] == 1.0

    # Verify day_of_week (1-5 for Mon-Fri)
    # 2023-01-31 = Tue (2)
    # 2023-02-01 = Wed (3)
    # 2023-02-02 = Thu (4)
    days = df["day_of_week"].to_list()
    assert days == [2, 3, 4]

    # Verify month start / end
    is_month_start = df["is_month_start"].to_list()
    is_month_end = df["is_month_end"].to_list()

    assert is_month_start == [False, True, False]
    assert is_month_end == [True, False, False]
