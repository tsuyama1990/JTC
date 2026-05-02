from datetime import date

import pytest

from src.domain_models.raw_quote import RawQuote
from src.processing.transformers import transform_quotes_to_dataframe


def test_transform_empty_list() -> None:
    df = transform_quotes_to_dataframe([])
    assert len(df) == 0
    assert "daily_return" in df.columns

def test_transform_quotes_logic() -> None:
    quotes = [
        RawQuote(date=date(2023, 1, 31), open=100.0, high=105.0, low=95.0, close=100.0, volume=10), # Tuesday, Month End
        RawQuote(date=date(2023, 2, 1), open=102.0, high=110.0, low=101.0, close=110.0, volume=20), # Wednesday, Month Start
        RawQuote(date=date(2023, 2, 2), open=110.0, high=115.0, low=105.0, close=105.0, volume=30), # Thursday
    ]

    df = transform_quotes_to_dataframe(quotes)

    assert len(df) == 3

    # Check flags for first row (Jan 31)
    row_0 = df.row(0, named=True)
    assert row_0["day_of_week"] == 2
    assert row_0["is_month_start"] is False
    assert row_0["is_month_end"] is True
    assert row_0["daily_return"] is None
    assert row_0["intraday_return"] == 0.0
    assert row_0["overnight_return"] is None

    # Check flags for second row (Feb 1)
    row_1 = df.row(1, named=True)
    assert row_1["day_of_week"] == 3
    assert row_1["is_month_start"] is True
    assert row_1["is_month_end"] is False
    assert pytest.approx(row_1["daily_return"]) == 0.10 # 110 / 100 - 1
    assert pytest.approx(row_1["intraday_return"]) == 0.07843137 # 110 / 102 - 1
    assert pytest.approx(row_1["overnight_return"]) == 0.02 # 102 / 100 - 1
