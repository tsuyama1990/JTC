import numpy as np
import polars as pl
import vectorbt as vbt

from src.core.exceptions import BacktestCalculationError
from src.domain_models.schemas import BacktestMetrics


def run_simulation(
    df: pl.DataFrame,
    entries: pl.Series,
    exits: pl.Series,
    initial_cash: float = 1_000_000.0,
    fees: float = 0.001,
) -> BacktestMetrics:
    """
    Runs a backtest simulation using vectorbt.

    Args:
        df: Polars DataFrame containing historical prices. Must have 'Close' column.
        entries: Boolean Series indicating entry signals.
        exits: Boolean Series indicating exit signals.
        initial_cash: Starting cash for the simulation.
        fees: Transaction fees as a decimal (e.g., 0.001 for 0.1%).

    Returns:
        BacktestMetrics containing the simulation results.

    Raises:
        BacktestCalculationError: If insufficient trades occur or valid metrics cannot be computed.
    """

    # Convert Polars to Pandas for vectorbt compatibility
    pdf = df.to_pandas()

    if "Close" not in pdf.columns:
        msg = "DataFrame must contain a 'Close' column for price data."
        raise ValueError(msg)

    price = pdf["Close"]

    # Convert signal Series to Pandas Series
    entries_pd = entries.to_pandas()
    exits_pd = exits.to_pandas()

    # Run simulation
    portfolio = vbt.Portfolio.from_signals(
        close=price,
        entries=entries_pd,
        exits=exits_pd,
        init_cash=initial_cash,
        fees=fees,
        freq="1D",
    )

    # Extract metrics
    total_return = portfolio.total_return()
    annualized_return = portfolio.annualized_return()
    max_drawdown = portfolio.max_drawdown()
    win_rate = portfolio.trades.win_rate()
    sharpe_ratio = portfolio.sharpe_ratio()

    # Handle NaNs or lack of trades
    if np.isnan(win_rate):
        # Fallback if no trades are made or win rate is NaN
        msg = "Insufficient trades occurred to compute valid risk metrics."
        raise BacktestCalculationError(msg)

    # Convert potential NaNs in other metrics to safe 0.0 before validating via Schema
    total_return = (
        0.0 if np.isnan(total_return) else float(total_return * 100)
    )  # Ensure it's a percentage
    annualized_return = 0.0 if np.isnan(annualized_return) else float(annualized_return * 100)
    max_drawdown = 0.0 if np.isnan(max_drawdown) else float(max_drawdown * 100)
    win_rate = float(win_rate * 100)  # vectorbt returns win_rate as decimal
    sharpe_ratio = 0.0 if np.isnan(sharpe_ratio) else float(sharpe_ratio)

    return BacktestMetrics(
        total_return=total_return,
        annualized_return=annualized_return,
        max_drawdown=max_drawdown,
        win_rate=win_rate,
        sharpe_ratio=sharpe_ratio,
    )
