from datetime import date
from math import isclose

from src.domain_models import RawQuote
from src.processing.transformers import process_quotes


def test_process_quotes_empty() -> None:
    assert process_quotes([]) == []

def test_process_quotes_valid() -> None:
    # 2023-01-02 is Monday (day_of_week=1 in polars 1-7 scheme)
    quotes = [
        RawQuote(
            date=date(2023, 1, 2), open=100.0, high=110.0, low=90.0, close=105.0, volume=100
        ),
        RawQuote(
            date=date(2023, 1, 3), open=105.0, high=115.0, low=100.0, close=110.0, volume=200
        ),
        RawQuote(
            date=date(2023, 2, 1), open=110.0, high=120.0, low=105.0, close=115.0, volume=300
        ),
    ]

    processed = process_quotes(quotes)

    assert len(processed) == 3

    # 2023-01-02 (Monday)
    q1 = processed[0]
    assert q1.date == date(2023, 1, 2)
    assert q1.day_of_week == 1
    assert q1.is_month_start is True  # shift doesn't exist, fill_null=True
    assert q1.is_month_end is False  # next month is 1, != is False
    assert q1.daily_return is None  # first row
    assert q1.intraday_return == 105.0 - 100.0
    assert q1.overnight_return is None  # prev close doesn't exist

    # 2023-01-03 (Tuesday)
    q2 = processed[1]
    assert q2.date == date(2023, 1, 3)
    assert q2.day_of_week == 2
    assert q2.is_month_start is False
    assert q2.is_month_end is True  # next month is 2, 1 != 2
    assert q2.daily_return is not None
    assert isclose(q2.daily_return, (110.0 - 105.0) / 105.0)
    assert q2.intraday_return == 110.0 - 105.0
    assert q2.overnight_return == 105.0 - 105.0  # open - prev_close

    # 2023-02-01 (Wednesday)
    q3 = processed[2]
    assert q3.date == date(2023, 2, 1)
    assert q3.day_of_week == 3
    assert q3.is_month_start is True  # prev month is 1, 2 != 1
    assert q3.is_month_end is True  # shift doesn't exist, fill_null=True
    assert q3.daily_return is not None
    assert isclose(q3.daily_return, (115.0 - 110.0) / 110.0)
    assert q3.intraday_return == 115.0 - 110.0
    assert q3.overnight_return == 110.0 - 110.0  # open - prev_close
