import polars as pl
import pytest

from src.analysis.backtest import run_simulation
from src.core.exceptions import BacktestCalculationError
from src.domain_models.schemas import BacktestMetrics


def test_run_simulation_successful() -> None:
    # Predictable price trend (uptrend)
    prices = [100.0, 102.0, 105.0, 103.0, 108.0, 110.0]

    df = pl.DataFrame({"Close": prices})

    # Enter at 0, exit at 2 (profit), Enter at 3, exit at 5 (profit)
    entries = pl.Series([True, False, False, True, False, False])
    exits = pl.Series([False, False, True, False, False, True])

    metrics = run_simulation(df, entries, exits)

    assert isinstance(metrics, BacktestMetrics)
    assert metrics.total_return > 0.0
    assert metrics.win_rate == 100.0  # Both trades should be profitable
    assert metrics.max_drawdown <= 0.0


def test_run_simulation_insufficient_trades() -> None:
    prices = [100.0, 101.0, 102.0, 103.0, 104.0]

    df = pl.DataFrame({"Close": prices})

    # Overly strict entry rules - no trades occur
    entries = pl.Series([False, False, False, False, False])
    exits = pl.Series([False, False, False, False, False])

    with pytest.raises(BacktestCalculationError) as exc_info:
        run_simulation(df, entries, exits)

    assert "Insufficient trades occurred" in str(exc_info.value)


def test_run_simulation_missing_close_column() -> None:
    df = pl.DataFrame({"Open": [100.0, 101.0]})
    entries = pl.Series([True, False])
    exits = pl.Series([False, True])

    with pytest.raises(ValueError, match="must contain a 'Close' column"):
        run_simulation(df, entries, exits)
