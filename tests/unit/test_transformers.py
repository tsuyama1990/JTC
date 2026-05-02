from datetime import date

from src.domain_models.models import RawQuote

# We will implement transform_quotes in src/processing/transformers.py
# that takes list[RawQuote] and returns a pl.DataFrame matching ProcessedQuote schema.


def test_transform_quotes_logic() -> None:
    # This test will fail until we implement the logic
    quotes = [
        RawQuote(
            date=date(2023, 1, 31),
            open=100.0,
            high=110.0,
            low=90.0,
            close=100.0,
            volume=1000,
        ),
        RawQuote(
            date=date(2023, 2, 1),
            open=105.0,
            high=115.0,
            low=95.0,
            close=110.0,
            volume=1200,
        ),
        RawQuote(
            date=date(2023, 2, 2),
            open=110.0,
            high=120.0,
            low=100.0,
            close=110.0,
            volume=1500,
        ),
    ]

    # Expected calculations:
    # Row 0: Jan 31 (Tuesday, day 2). is_month_start=False, is_month_end=True
    # daily_return: None, intraday: (100/100)-1 = 0, overnight: None
    # Row 1: Feb 1 (Wednesday, day 3). is_month_start=True, is_month_end=False
    # daily_return: (110/100)-1 = 0.1, intraday: (110/105)-1 = 0.047619..., overnight: (105/100)-1 = 0.05
    # Row 2: Feb 2 (Thursday, day 4). is_month_start=False, is_month_end=False
    # daily_return: (110/110)-1 = 0.1, intraday: (110/110)-1 = 0.1, overnight: (110/110)-1 = 0.0

    from src.processing.transformers import transform_quotes

    df = transform_quotes(quotes)

    assert df.height == 3

    # Check day of week (1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri)
    assert df["day_of_week"][0] == 2
    assert df["day_of_week"][1] == 3
    assert df["day_of_week"][2] == 4

    # Check month start/end
    assert not df["is_month_start"][0]
    assert df["is_month_end"][0]

    assert df["is_month_start"][1]
    assert not df["is_month_end"][1]

    # Check returns
    # Due to floating point math, use approx for comparison
    import math
    assert math.isnan(df["daily_return"][0]) or df["daily_return"][0] is None
    assert math.isnan(df["overnight_return"][0]) or df["overnight_return"][0] is None

    assert abs(df["daily_return"][1] - 0.10) < 1e-6
    assert abs(df["intraday_return"][1] - (110.0 / 105.0 - 1.0)) < 1e-6
    assert abs(df["overnight_return"][1] - 0.05) < 1e-6

    assert abs(df["daily_return"][2] - 0.0) < 1e-6
    assert abs(df["intraday_return"][2] - 0.0) < 1e-6
    assert abs(df["overnight_return"][2] - 0.0) < 1e-6
