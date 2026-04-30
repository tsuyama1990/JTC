import numpy as np
import pandas as pd
from src.analysis.statistics import evaluate_day_anomaly

from src.domain_models.statistics import StatResult


def test_evaluate_day_anomaly_significant() -> None:
    # Create synthetic data where target_day has a significantly different mean
    np.random.seed(42)
    # Day 1: Mean = 0.05, std = 0.01
    day1_returns = np.random.normal(0.05, 0.01, 100)
    # Other days: Mean = 0.0, std = 0.01
    other_returns = np.random.normal(0.0, 0.01, 400)

    returns = np.concatenate([day1_returns, other_returns])
    days = np.concatenate([np.ones(100), np.zeros(400) + 2])

    df = pd.DataFrame({"return": returns, "day_of_week": days})

    result = evaluate_day_anomaly(df, target_day=1)

    assert isinstance(result, StatResult)
    assert result.target_day == 1
    assert result.is_significant is True
    assert result.p_value < 0.05
    assert result.t_statistic > 0.0


def test_evaluate_day_anomaly_insignificant() -> None:
    # Create synthetic data where target_day has the same mean
    np.random.seed(42)
    day1_returns = np.random.normal(0.0, 0.01, 100)
    other_returns = np.random.normal(0.0, 0.01, 400)

    returns = np.concatenate([day1_returns, other_returns])
    days = np.concatenate([np.ones(100), np.zeros(400) + 2])

    df = pd.DataFrame({"return": returns, "day_of_week": days})

    result = evaluate_day_anomaly(df, target_day=1)

    assert isinstance(result, StatResult)
    assert result.target_day == 1
    assert result.is_significant is False
    assert result.p_value >= 0.05


def test_evaluate_day_anomaly_zero_variance() -> None:
    # Edge case: zero variance
    df = pd.DataFrame({"return": [0.0] * 500, "day_of_week": [1] * 100 + [2] * 400})

    result = evaluate_day_anomaly(df, target_day=1)

    assert isinstance(result, StatResult)
    assert result.p_value == 1.0
    assert result.is_significant is False
    assert result.t_statistic == 0.0
