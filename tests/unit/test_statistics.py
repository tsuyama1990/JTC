import numpy as np
import pandas as pd

from src.analysis.statistics import evaluate_day_of_week_returns
from src.domain_models import StatResult


def test_evaluate_day_of_week_returns_normal() -> None:
    # Create synthetic dataframe
    np.random.seed(42)
    dates = pd.date_range("2023-01-01", periods=100)
    # Day 0 is Monday
    # Make Mondays have higher returns
    returns = np.random.normal(0.0, 0.01, size=100)
    returns[dates.dayofweek == 0] += 0.05

    df = pd.DataFrame({"Date": dates, "Return": returns})

    result = evaluate_day_of_week_returns(df, target_day=0)

    assert isinstance(result, StatResult)
    assert result.target_day == 0
    assert result.t_statistic > 0
    assert 0 <= result.p_value <= 1.0
    assert result.is_significant is True  # Based on synthetic data


def test_evaluate_day_of_week_returns_insignificant() -> None:
    np.random.seed(42)
    dates = pd.date_range("2023-01-01", periods=100)
    # No difference for Mondays
    returns = np.random.normal(0.0, 0.01, size=100)

    df = pd.DataFrame({"Date": dates, "Return": returns})

    result = evaluate_day_of_week_returns(df, target_day=0)

    assert isinstance(result, StatResult)
    assert result.target_day == 0
    assert 0 <= result.p_value <= 1.0
    assert result.is_significant is False


def test_evaluate_day_of_week_returns_zero_variance() -> None:
    # Edge case where variance is perfectly zero (flatline)
    dates = pd.date_range("2023-01-01", periods=100)
    returns = np.zeros(100)

    df = pd.DataFrame({"Date": dates, "Return": returns})

    result = evaluate_day_of_week_returns(df, target_day=0)

    assert isinstance(result, StatResult)
    assert result.target_day == 0
    # Expected to gracefully handle and return p=1.0, not significant
    assert result.p_value == 1.0
    assert result.is_significant is False
