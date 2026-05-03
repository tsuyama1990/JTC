import numpy as np
import polars as pl

from src.analysis.statistics import evaluate_calendar_anomaly
from src.domain_models.schemas import StatResult


def test_evaluate_calendar_anomaly_significant() -> None:
    # Create deterministic synthetic data where day 1 is significantly higher
    n_samples = 100

    # Day 1 (target) returns: normal distribution around 0.05
    target_returns = np.random.normal(loc=0.05, scale=0.01, size=n_samples)

    # Other days returns: normal distribution around 0.00
    other_returns = np.random.normal(loc=0.00, scale=0.01, size=n_samples)

    # Create dataframe
    df_target = pl.DataFrame({"day_of_week": [1] * n_samples, "daily_return": target_returns})
    df_other = pl.DataFrame({"day_of_week": [2] * n_samples, "daily_return": other_returns})

    df = pl.concat([df_target, df_other])

    result = evaluate_calendar_anomaly(df, target_day=1)

    assert isinstance(result, StatResult)
    assert result.target_day == 1
    assert result.is_significant is True
    assert result.p_value < 0.05


def test_evaluate_calendar_anomaly_not_significant() -> None:
    # Create deterministic synthetic data where day 1 is NOT significantly different
    n_samples = 100

    # Day 1 and other days returns: both normal distribution around 0.00
    target_returns = np.random.normal(loc=0.00, scale=0.01, size=n_samples)
    other_returns = np.random.normal(loc=0.00, scale=0.01, size=n_samples)

    # Create dataframe
    df_target = pl.DataFrame({"day_of_week": [1] * n_samples, "daily_return": target_returns})
    df_other = pl.DataFrame({"day_of_week": [2] * n_samples, "daily_return": other_returns})

    df = pl.concat([df_target, df_other])

    result = evaluate_calendar_anomaly(df, target_day=1)

    assert isinstance(result, StatResult)
    assert result.target_day == 1
    assert result.is_significant is False
    assert result.p_value >= 0.05


def test_evaluate_calendar_anomaly_zero_variance() -> None:
    n_samples = 50
    # Zero variance data
    target_returns = np.zeros(n_samples)
    other_returns = np.zeros(n_samples)

    df_target = pl.DataFrame({"day_of_week": [1] * n_samples, "daily_return": target_returns})
    df_other = pl.DataFrame({"day_of_week": [2] * n_samples, "daily_return": other_returns})

    df = pl.concat([df_target, df_other])

    result = evaluate_calendar_anomaly(df, target_day=1)

    assert isinstance(result, StatResult)
    assert result.p_value == 1.0
    assert result.t_statistic == 0.0
    assert result.is_significant is False


def test_evaluate_calendar_anomaly_insufficient_data() -> None:
    # Insufficient target data
    df = pl.DataFrame({"day_of_week": [1, 2, 2, 2], "daily_return": [0.01, -0.01, 0.02, 0.00]})

    result = evaluate_calendar_anomaly(df, target_day=1)
    assert result.p_value == 1.0
    assert result.is_significant is False

    # Insufficient other data
    df2 = pl.DataFrame({"day_of_week": [1, 1, 1, 2], "daily_return": [0.01, -0.01, 0.02, 0.00]})

    result2 = evaluate_calendar_anomaly(df2, target_day=1)
    assert result2.p_value == 1.0
    assert result2.is_significant is False
