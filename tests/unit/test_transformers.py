from datetime import date

import pytest

from src.domain_models.quote import RawQuote
from src.transformers import transform_quotes


def test_transform_quotes_calculations() -> None:
    quotes = [
        RawQuote(
            date=date(2023, 10, 31), open=100.0, high=110.0, low=90.0, close=100.0, volume=1000
        ),  # Tuesday, End of month
        RawQuote(
            date=date(2023, 11, 1), open=105.0, high=115.0, low=95.0, close=110.0, volume=1000
        ),  # Wednesday, Start of month
    ]
    df = transform_quotes(quotes)

    assert df.height == 2

    # First row check
    row1 = df.row(0, named=True)
    assert row1["day_of_week"] == 2
    assert row1["is_month_start"] is False
    assert row1["is_month_end"] is True
    assert row1["daily_return"] is None
    assert row1["intraday_return"] == 0.0  # 100.0 - 100.0
    assert row1["overnight_return"] is None

    # Second row check
    row2 = df.row(1, named=True)
    assert row2["day_of_week"] == 3
    assert row2["is_month_start"] is True
    assert row2["is_month_end"] is False
    assert row2["daily_return"] == pytest.approx(0.1)  # (110.0 - 100.0) / 100.0
    assert row2["intraday_return"] == pytest.approx(5.0)  # 110.0 - 105.0
    assert row2["overnight_return"] == pytest.approx(5.0)  # 105.0 - 100.0
