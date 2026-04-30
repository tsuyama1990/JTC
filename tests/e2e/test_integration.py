from typing import Any
from unittest.mock import patch

import numpy as np
import polars as pl

from src.analysis.backtest import run_backtest
from src.analysis.statistics import evaluate_day_anomaly
from src.domain_models.backtest import BacktestMetrics
from src.domain_models.statistics import StatResult


@patch("src.storage.repository.get_historical_data")
def test_full_pipeline_integration(mock_get_data: Any) -> None:
    # 1. Mock DuckDB retrieval
    days = list(range(1, 6)) * 20  # 100 days
    # Create an anomaly on Monday (target_day=1)
    # Mondays have positive returns, other days 0
    np.random.seed(42)
    returns = np.where(
        np.array(days) == 1, np.random.normal(0.05, 0.01, 100), np.random.normal(0.0, 0.01, 100)
    )

    # Prices that reflect the returns
    prices = [100.0]
    for r in returns:
        prices.append(prices[-1] * (1 + r))
    prices = prices[1:]

    mock_df = pl.DataFrame({"return": returns, "close": prices, "day_of_week": days})

    mock_get_data.return_value = mock_df

    # 2. Pipeline execution
    import src.storage.repository

    df = src.storage.repository.get_historical_data()

    # 3. Statistical testing
    stat_result = evaluate_day_anomaly(df.to_pandas(), target_day=1)
    assert isinstance(stat_result, StatResult)
    assert stat_result.target_day == 1
    assert stat_result.is_significant is True  # We designed it to be significant

    # 4. Backtesting Engine
    # Only if significant, we run backtest
    if stat_result.is_significant:
        # Buy on Friday, Sell on Monday
        backtest_metrics = run_backtest(
            df=df, entry_day=5, exit_day=1, initial_cash=1000000.0, fees=0.001
        )
        assert isinstance(backtest_metrics, BacktestMetrics)
        assert backtest_metrics.total_return > 0.0
