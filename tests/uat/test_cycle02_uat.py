import numpy as np
import polars as pl
import pytest

from src.analysis.backtest import run_simulation
from src.analysis.statistics import evaluate_calendar_anomaly
from src.core.exceptions import BacktestCalculationError
from src.domain_models.schemas import BacktestMetrics, StatResult


def test_uat_scenario_1_statistical_significance_validation() -> None:
    """
    GIVEN a local database containing historical dataset covering twelve weeks
    AND the user commands to statistically evaluate returns specifically on Mondays vs other days
    WHEN the statsmodels engine finishes executing
    THEN the system must return a StatResult Pydantic object
    AND the object must contain a p_value between 0.0 and 1.0
    AND the is_significant flag is determined purely by p_value < 0.05
    """

    # 1. Create 12 weeks of synthetic data (60 days)
    n_weeks = 12
    days = 5
    total_days = n_weeks * days

    # Day 1 is Monday.
    days_arr = [(i % 5) + 1 for i in range(total_days)]

    # Introduce small random noise, but make Monday (1) slightly better
    returns = []
    for day in days_arr:
        if day == 1:
            returns.append(np.random.normal(0.01, 0.005))
        else:
            returns.append(np.random.normal(0.00, 0.005))

    df = pl.DataFrame({"day_of_week": days_arr, "daily_return": returns})

    # Execute analysis on Monday (day 1)
    result = evaluate_calendar_anomaly(df, target_day=1)

    # Validation
    assert isinstance(result, StatResult)
    assert 0.0 <= result.p_value <= 1.0

    # Verify logical consistency
    expected_significance = result.p_value < 0.05
    assert result.is_significant == expected_significance


def test_uat_scenario_2_algorithmic_simulation() -> None:
    """
    GIVEN an enriched Polars DataFrame and explicit boolean signals
    AND a defined parameter set including transaction fees 0.1% and 1,000,000 JPY
    WHEN the vectorbt backtesting engine finishes simulating
    THEN the system must output a properly structured BacktestMetrics Pydantic model
    AND the max_drawdown float field must be negative or zero
    """
    # Create simple predictable data
    prices = [100.0, 102.0, 101.0, 105.0, 104.0]
    df = pl.DataFrame({"Close": prices})

    # Enter before jumps, exit before dips
    entries = pl.Series([True, False, True, False, False])
    exits = pl.Series([False, True, False, True, False])

    # Execute simulation
    metrics = run_simulation(df, entries, exits, initial_cash=1000000.0, fees=0.001)

    assert isinstance(metrics, BacktestMetrics)
    assert metrics.max_drawdown <= 0.0
    assert metrics.total_return > 0.0  # Should be profitable


def test_uat_scenario_3_zero_variance_graceful_handling() -> None:
    """
    GIVEN an edge-case test dataset where the historical prices flatline (variance is zero)
    WHEN the system attempts to calculate t-statistic and p-value
    THEN it must gracefully catch potential errors
    AND return a StatResult where p_value is safe (like 1.0) avoiding a crash
    """
    df = pl.DataFrame({"day_of_week": [1, 2] * 5, "daily_return": [0.0] * 10})

    # This shouldn't crash
    result = evaluate_calendar_anomaly(df, target_day=1)

    assert isinstance(result, StatResult)
    assert result.p_value == 1.0
    assert result.is_significant is False


def test_uat_scenario_4_nan_risk_metric_graceful_handling() -> None:
    """
    GIVEN a robust configuration explicitly requiring output heavily validated
    WHEN the vectorbt backtesting engine unexpectedly returns a NaN due to lack of trades
    THEN the system must gracefully handle this exception before returning (e.g. raising BacktestCalculationError)
    """
    prices = [100.0, 101.0, 102.0]
    df = pl.DataFrame({"Close": prices})

    # Zero trades
    entries = pl.Series([False, False, False])
    exits = pl.Series([False, False, False])

    with pytest.raises(BacktestCalculationError) as exc_info:
        run_simulation(df, entries, exits)

    assert "Insufficient trades occurred" in str(exc_info.value)
