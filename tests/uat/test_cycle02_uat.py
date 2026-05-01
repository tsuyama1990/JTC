import numpy as np
import pandas as pd

from src.analysis.backtest import run_backtest
from src.analysis.statistics import evaluate_day_of_week_returns
from src.domain_models import BacktestMetrics, StatResult


def test_uat_scenario_1_statistical_significance() -> None:
    """
    UAT Scenario 1: Verify statistical evaluation generates a valid StatResult.
    """
    np.random.seed(42)
    dates = pd.date_range("2023-01-01", periods=200)
    # Significant difference for Mondays
    returns = np.random.normal(0.0, 0.01, size=200)
    returns[dates.dayofweek == 0] += 0.05

    df = pd.DataFrame({"Date": dates, "Return": returns})

    result = evaluate_day_of_week_returns(df, target_day=0)

    # Assert UAT requirements
    assert isinstance(result, StatResult)
    assert 0.0 <= result.p_value <= 1.0
    # Because of our manual manipulation, it should be significant
    assert result.is_significant is True
    assert result.p_value < 0.05


def test_uat_scenario_2_algorithmic_simulation() -> None:
    """
    UAT Scenario 2: Verify algorithmic backtesting generates valid BacktestMetrics.
    """
    dates = pd.date_range("2023-01-01", periods=200)
    prices = np.linspace(100, 300, 200)

    df = pd.DataFrame(
        {
            "Date": dates,
            "Close": prices,
            "entries": dates.dayofweek == 0,  # Monday
            "exits": dates.dayofweek == 4,  # Friday
        }
    )

    result = run_backtest(df, fee=0.001, initial_cash=1_000_000.0)

    # Assert UAT requirements
    assert isinstance(result, BacktestMetrics)
    assert result.total_return > 0
    assert result.max_drawdown <= 0.0
    assert result.sharpe_ratio > 1.0
