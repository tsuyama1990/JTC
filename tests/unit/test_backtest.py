import polars as pl
import pytest

from src.analysis.backtest import BacktestCalculationError, run_backtest
from src.domain_models.backtest import BacktestMetrics


def test_run_backtest_successful() -> None:
    # Synthetic data: 4 weeks of data (20 days)
    # Price starts at 100, goes up by 10 every day.
    # Monday = 1, Friday = 5
    days = list(range(1, 6)) * 4
    prices = [100.0 + i * 10.0 for i in range(20)]

    df = pl.DataFrame({"close": prices, "day_of_week": days})

    # Entry on Monday (1), Exit on Friday (5)
    # Fee = 0.1%, Initial cash = 1000000
    metrics = run_backtest(df=df, entry_day=1, exit_day=5, initial_cash=1000000.0, fees=0.001)

    assert isinstance(metrics, BacktestMetrics)
    assert metrics.total_return > 0.0
    assert metrics.max_drawdown <= 0.0
    assert 0.0 <= metrics.win_rate <= 100.0
    assert metrics.sharpe_ratio != 0.0


def test_run_backtest_no_trades() -> None:
    # Data with no matching entry/exit days
    days = [2, 3, 4] * 6
    prices = [100.0 + i for i in range(18)]

    df = pl.DataFrame({"close": prices, "day_of_week": days})

    # We expect a custom BacktestCalculationError to be raised because no trades happened,
    # resulting in NaN metrics.
    with pytest.raises(BacktestCalculationError):
        run_backtest(df=df, entry_day=1, exit_day=5, initial_cash=1000000.0, fees=0.001)
