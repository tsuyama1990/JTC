import numpy as np
import pandas as pd

from src.analysis.backtest import run_backtest
from src.domain_models import BacktestMetrics


def test_run_backtest_normal() -> None:
    dates = pd.date_range("2023-01-01", periods=100)
    # create price increasing smoothly
    prices = np.linspace(100, 200, 100)

    # Enter every Monday (dayofweek==0), Exit every Friday (dayofweek==4)
    entries = dates.dayofweek == 0
    exits = dates.dayofweek == 4

    df = pd.DataFrame({"Date": dates, "Close": prices, "entries": entries, "exits": exits})

    result = run_backtest(df, fee=0.001, initial_cash=1000000.0)

    assert isinstance(result, BacktestMetrics)
    # Given smooth uptrend, we expect positive return
    assert result.total_return > 0
    assert result.max_drawdown <= 0
    assert 0 <= result.win_rate <= 100
    assert result.sharpe_ratio > 1.0  # smooth uptrend, high sharpe


def test_run_backtest_no_trades() -> None:
    dates = pd.date_range("2023-01-01", periods=100)
    prices = np.linspace(100, 200, 100)

    # Overly strict rules, never enter
    entries = np.zeros(100, dtype=bool)
    exits = np.zeros(100, dtype=bool)

    df = pd.DataFrame({"Date": dates, "Close": prices, "entries": entries, "exits": exits})

    result = run_backtest(df, fee=0.001, initial_cash=1000000.0)

    assert isinstance(result, BacktestMetrics)
    assert result.total_return == 0.0
    assert result.max_drawdown == 0.0
    assert result.win_rate == 0.0
    assert result.sharpe_ratio == 0.0
