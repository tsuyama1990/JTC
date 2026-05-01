import numpy as np
import pandas as pd
import vectorbt as vbt

from src.domain_models import BacktestMetrics


def run_backtest(
    df: pd.DataFrame, fee: float = 0.001, initial_cash: float = 1_000_000.0
) -> BacktestMetrics:
    """
    Runs a vectorbt backtest given a DataFrame with 'Close', 'entries', and 'exits' columns.
    Returns validated BacktestMetrics.
    """

    # If no entries are generated, gracefully return zeroed metrics
    if not df["entries"].any():
        return BacktestMetrics(
            total_return=0.0,
            annualized_return=0.0,
            max_drawdown=0.0,
            win_rate=0.0,
            sharpe_ratio=0.0,
        )

    # Initialize Portfolio
    portfolio = vbt.Portfolio.from_signals(
        close=df["Close"],
        entries=df["entries"],
        exits=df["exits"],
        fees=fee,
        init_cash=initial_cash,
        freq="1D",  # Assuming daily data for annualization
    )

    # Extract metrics
    total_return = float(portfolio.total_return())
    annualized_return = float(portfolio.annualized_return())
    max_drawdown = float(portfolio.max_drawdown())

    # win_rate is a method of trades
    win_rate = float(portfolio.trades.win_rate())
    sharpe_ratio = float(portfolio.sharpe_ratio())

    # Handle NaNs that vectorbt might return if there are no trades or insufficient data
    if np.isnan(total_return):
        total_return = 0.0
    if np.isnan(annualized_return):
        annualized_return = 0.0
    if np.isnan(max_drawdown):
        max_drawdown = 0.0
    if np.isnan(win_rate):
        win_rate = 0.0
    if np.isnan(sharpe_ratio):
        sharpe_ratio = 0.0

    return BacktestMetrics(
        total_return=total_return * 100,
        annualized_return=annualized_return * 100,
        max_drawdown=max_drawdown * 100,
        win_rate=win_rate * 100
        if win_rate <= 1.0
        else win_rate,  # win_rate from vectorbt is a ratio 0-1
        sharpe_ratio=sharpe_ratio,
    )
