from datetime import date
from math import isclose

from src.domain_models import RawQuote
from src.processing.transformers import process_quotes


def test_scenario_2_transformations() -> None:
    """
    Scenario 2: Precise Feature Engineering and Transformations.
    Validating the Polars processing explicitly.
    """
    # GIVEN a completely raw dataset
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

    # WHEN the highly complex transformation process entirely finishes
    processed = process_quotes(quotes)

    # THEN resulting DataFrame must correctly map day_of_week
    # 2023-01-02 was a Monday -> 1
    assert processed[0].day_of_week == 1
    assert processed[1].day_of_week == 2
    assert processed[2].day_of_week == 3

    # AND is_month_start/is_month_end are mathematically correct
    assert processed[0].is_month_start is True
    assert processed[0].is_month_end is False
    assert processed[1].is_month_start is False
    assert processed[1].is_month_end is True
    assert processed[2].is_month_start is True
    assert processed[2].is_month_end is True

    # AND returns are precise
    assert processed[0].daily_return is None
    assert processed[0].intraday_return == 105.0 - 100.0

    assert processed[1].daily_return is not None
    assert isclose(processed[1].daily_return, (110.0 - 105.0) / 105.0)
    assert processed[1].intraday_return == 110.0 - 105.0
    assert processed[1].overnight_return == 105.0 - 105.0
