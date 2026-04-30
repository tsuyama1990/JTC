import numpy as np
import polars as pl
import pytest

from src.analysis.backtest import BacktestCalculationError, run_backtest
from src.analysis.statistics import evaluate_day_anomaly
from src.domain_models.backtest import BacktestMetrics
from src.domain_models.statistics import StatResult


def test_uat_scenario_1_statistical_significance() -> None:
    # GIVEN a fully populated dataset (simulated)
    np.random.seed(42)
    # 60 days (12 weeks)
    days = list(range(1, 6)) * 12
    # Mondays have an anomaly
    returns = np.where(
        np.array(days) == 1, np.random.normal(0.05, 0.01, 60), np.random.normal(0.0, 0.01, 60)
    )

    df = pl.DataFrame({"return": returns, "day_of_week": days})

    # WHEN the statsmodels evaluation engine finishes executing
    stat_result = evaluate_day_anomaly(df.to_pandas(), target_day=1)

    # THEN it must return exactly one perfectly valid StatResult
    assert isinstance(stat_result, StatResult)
    # AND p_value is between 0.0 and 1.0
    assert 0.0 <= stat_result.p_value <= 1.0
    # AND is_significant is correctly flagged
    assert stat_result.is_significant is (stat_result.p_value < 0.05)


def test_uat_scenario_2_algorithmic_simulation() -> None:
    # GIVEN a valid Polars DataFrame
    days = list(range(1, 6)) * 20
    prices = [100.0 + i * 5 for i in range(100)]  # steady uptrend
    df = pl.DataFrame({"close": prices, "day_of_week": days})

    # AND parameters
    fees = 0.001
    initial_cash = 1000000.0

    # WHEN vectorbt backtesting engine finishes
    metrics = run_backtest(df, entry_day=1, exit_day=5, initial_cash=initial_cash, fees=fees)

    # THEN output structured BacktestMetrics
    assert isinstance(metrics, BacktestMetrics)
    # AND total_return correctly calculated (float)
    assert isinstance(metrics.total_return, float)
    # AND max_drawdown is strictly negative or exactly zero
    assert metrics.max_drawdown <= 0.0
    # AND sharpe_ratio is a mathematically sound float
    assert isinstance(metrics.sharpe_ratio, float)


def test_uat_scenario_3_edge_case_zero_variance() -> None:
    # GIVEN dataset where prices flatline (zero variance)
    df = pl.DataFrame({"return": [0.0] * 100, "day_of_week": list(range(1, 6)) * 20})

    # WHEN statsmodels calculation runs
    # THEN engine must gracefully catch error and return safe defaults
    stat_result = evaluate_day_anomaly(df.to_pandas(), target_day=1)

    assert isinstance(stat_result, StatResult)
    assert stat_result.p_value == 1.0
    assert stat_result.is_significant is False
    assert stat_result.t_statistic == 0.0


def test_uat_scenario_4_graceful_nan_handling() -> None:
    # GIVEN vectorbt engine returns NaN due to lack of trades
    df = pl.DataFrame(
        {
            "close": [100.0] * 100,
            "day_of_week": [1] * 100,  # only mondays, so exit day (5) is never hit
        }
    )

    # WHEN backtest runs
    # THEN it must raise BacktestCalculationError
    with pytest.raises(BacktestCalculationError, match="Insufficient trades occurred"):
        run_backtest(df, entry_day=1, exit_day=5, initial_cash=1000000.0, fees=0.001)
