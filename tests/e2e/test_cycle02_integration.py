import numpy as np
import polars as pl
import pytest

from src.analysis.backtest import run_simulation
from src.analysis.statistics import evaluate_calendar_anomaly
from src.domain_models.schemas import BacktestMetrics, StatResult


@pytest.fixture
def mock_duckdb_data() -> pl.DataFrame:
    """
    Simulates the data retrieved from DuckDB repository (from Cycle 1).
    This data exhibits a strong "Monday" (day 1) anomaly.
    """
    n_weeks = 12
    prices = []
    day_of_week = []
    daily_returns = []

    current_price = 100.0

    for _week in range(n_weeks):
        for day in range(1, 6):  # Monday=1 to Friday=5
            day_of_week.append(day)

            # Strong Monday returns with some noise to ensure non-zero variance
            if day == 1:
                ret = 0.05 + np.random.normal(0, 0.001)
            else:
                ret = 0.00 + np.random.normal(0, 0.001)

            current_price *= 1 + ret
            prices.append(current_price)
            daily_returns.append(ret)

    return pl.DataFrame(
        {"day_of_week": day_of_week, "daily_return": daily_returns, "Close": prices}
    )


def test_cycle02_end_to_end_analysis(mock_duckdb_data: pl.DataFrame) -> None:
    """
    Tests the integration between the statsmodels and vectorbt components
    without requiring physical Parquet files.
    """

    # --- PHASE 1: Statistical Analysis ---

    # We evaluate the anomaly for Monday (target_day=1)
    stat_result = evaluate_calendar_anomaly(mock_duckdb_data, target_day=1)

    assert isinstance(stat_result, StatResult)
    assert stat_result.target_day == 1
    assert stat_result.is_significant is True  # Because we embedded a 5% weekly return
    assert 0.0 <= stat_result.p_value < 0.05

    # --- PHASE 2: Backtest Simulation ---

    # Now we use the statistical finding to define a trading strategy.
    # Since Monday is highly significant, we enter on Friday (day 5) to capture Monday's return
    # and exit on Monday (day 1) after the return is realized.

    entries = mock_duckdb_data["day_of_week"] == 5
    exits = mock_duckdb_data["day_of_week"] == 1

    metrics = run_simulation(
        df=mock_duckdb_data, entries=entries, exits=exits, initial_cash=1_000_000.0, fees=0.001
    )

    assert isinstance(metrics, BacktestMetrics)
    assert metrics.total_return > 0.0
    assert metrics.win_rate > 50.0  # Should be high due to our synthetic data
    assert metrics.max_drawdown <= 0.0
