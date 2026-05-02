from datetime import date

import polars as pl
from polars.testing import assert_frame_equal

from src.domain_models.models import RawQuote
from src.transformers import transform_quotes


def test_transformers_calculate_metrics() -> None:
    quotes = [
        RawQuote(
            date=date(2023, 1, 3), # Tuesday
            open=100.0,
            high=105.0,
            low=95.0,
            close=100.0,
            volume=1000,
        ),
        RawQuote(
            date=date(2023, 1, 4), # Wednesday
            open=102.0,
            high=115.0,
            low=100.0,
            close=110.0,
            volume=1200,
        ),
        RawQuote(
            date=date(2023, 1, 31), # Tuesday, month end
            open=110.0,
            high=115.0,
            low=108.0,
            close=112.0,
            volume=1500,
        ),
        RawQuote(
            date=date(2023, 2, 1), # Wednesday, month start
            open=112.0,
            high=120.0,
            low=110.0,
            close=118.0,
            volume=2000,
        ),
    ]

    result_df = transform_quotes(quotes)

    expected_data = {
        "date": [date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 31), date(2023, 2, 1)],
        "open": [100.0, 102.0, 110.0, 112.0],
        "high": [105.0, 115.0, 115.0, 120.0],
        "low": [95.0, 100.0, 108.0, 110.0],
        "close": [100.0, 110.0, 112.0, 118.0],
        "volume": [1000, 1200, 1500, 2000],
        "daily_return": [None, 0.1, 0.01818181818181818, 0.05357142857142857],
        "intraday_return": [0.0, 0.0784313725490196, 0.01818181818181818, 0.05357142857142857],
        "overnight_return": [None, 0.02, 0.0, 0.0],
        "day_of_week": [2, 3, 2, 3],
        "is_month_start": [False, False, False, True],
        "is_month_end": [False, False, True, False],
    }

    expected_df = pl.DataFrame(expected_data)

    assert_frame_equal(result_df, expected_df, check_exact=False)

def test_transformers_empty_input() -> None:
    quotes: list[RawQuote] = []
    result_df = transform_quotes(quotes)
    assert result_df.is_empty()
