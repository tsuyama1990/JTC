import pytest
from transformers import process_quotes

from domain_models import RawQuote


def test_process_quotes_valid() -> None:
    quotes = [
        RawQuote(date="2023-10-31", open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0),
        RawQuote(date="2023-11-01", open=106.0, high=115.0, low=100.0, close=110.0, volume=1200.0),
        RawQuote(date="2023-11-02", open=110.0, high=120.0, low=105.0, close=115.0, volume=1500.0),
    ]

    df = process_quotes(quotes)

    # Calculate expected values manually
    # Row 0: daily_return=None, intraday_return=(105-100)/100=0.05, overnight_return=None, day_of_week=2 (Tue), is_month_start=False, is_month_end=True
    # Row 1: daily_return=(110-105)/105=0.047619, intraday_return=(110-106)/106=0.037735, overnight_return=(106-105)/105=0.009523, day_of_week=3 (Wed), is_month_start=True, is_month_end=False
    # Row 2: daily_return=(115-110)/110=0.045454, intraday_return=(115-110)/110=0.045454, overnight_return=(110-110)/110=0.0, day_of_week=4 (Thu), is_month_start=False, is_month_end=False

    assert len(df) == 3

    # Check date column
    assert df["date"].to_list() == ["2023-10-31", "2023-11-01", "2023-11-02"]

    # Check day of week
    assert df["day_of_week"].to_list() == [2, 3, 4]

    # Check month start/end flags
    assert df["is_month_end"].to_list() == [True, False, False]
    assert df["is_month_start"].to_list() == [False, True, False]

    # Check returns (allow small float diffs)
    daily = df["daily_return"].to_list()
    assert daily[0] is None
    assert daily[1] == pytest.approx((110.0 - 105.0) / 105.0)
    assert daily[2] == pytest.approx((115.0 - 110.0) / 110.0)

    intra = df["intraday_return"].to_list()
    assert intra[0] == pytest.approx((105.0 - 100.0) / 100.0)
    assert intra[1] == pytest.approx((110.0 - 106.0) / 106.0)
    assert intra[2] == pytest.approx((115.0 - 110.0) / 110.0)

    overnight = df["overnight_return"].to_list()
    assert overnight[0] is None
    assert overnight[1] == pytest.approx((106.0 - 105.0) / 105.0)
    assert overnight[2] == pytest.approx((110.0 - 110.0) / 110.0)


def test_process_quotes_zero_division() -> None:
    quotes = [
        RawQuote(date="2023-10-31", open=0.0, high=110.0, low=0.0, close=0.0, volume=1000.0),
        RawQuote(date="2023-11-01", open=106.0, high=115.0, low=100.0, close=110.0, volume=1200.0),
    ]

    df = process_quotes(quotes)

    # intraday return for day 0 should handle div by zero (open is 0.0)
    intra = df["intraday_return"].to_list()
    assert intra[0] is None  # Or 0, or nan, but should not crash

    # overnight return for day 1 should handle div by zero (prev close is 0.0)
    overnight = df["overnight_return"].to_list()
    assert overnight[1] is None  # Or should not crash
