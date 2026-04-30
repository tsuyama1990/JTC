import numpy as np
import polars as pl
import vectorbt as vbt

from src.domain_models.backtest import BacktestMetrics


class BacktestCalculationError(Exception):
    """Raised when backtest metrics cannot be calculated due to insufficient trades."""


def run_backtest(
    df: pl.DataFrame,
    entry_day: int,
    exit_day: int,
    initial_cash: float,
    fees: float,
) -> BacktestMetrics:
    """
    Runs a VectorBT backtest based on specific entry and exit days of the week.
    Returns BacktestMetrics object.
    """
    if "close" not in df.columns or "day_of_week" not in df.columns:
        msg = "DataFrame must contain 'close' and 'day_of_week' columns"
        raise ValueError(msg)

    pdf = df.to_pandas()

    entries = pdf["day_of_week"] == entry_day
    exits = pdf["day_of_week"] == exit_day

    if not entries.any() or not exits.any():
        msg = "Insufficient trades occurred to compute valid risk metrics."
        raise BacktestCalculationError(msg)

    pf = vbt.Portfolio.from_signals(
        close=pdf["close"],
        entries=entries,
        exits=exits,
        init_cash=initial_cash,
        fees=fees,
        freq="1D",
    )

    trades = pf.trades
    if len(trades) == 0:
        msg = "Insufficient trades occurred to compute valid risk metrics."
        raise BacktestCalculationError(msg)

    try:
        # win_rate() returns a float between 0 and 1, we multiply by 100 for the schema
        win_rate_val = trades.win_rate()
        win_rate = (
            0.0 if win_rate_val is None or np.isnan(win_rate_val) else float(win_rate_val * 100.0)
        )

        total_return = float(pf.total_return() * 100.0)

        ann_return_val = pf.annualized_return()
        annualized_return = (
            0.0
            if ann_return_val is None or np.isnan(ann_return_val)
            else float(ann_return_val * 100.0)
        )

        max_dd_val = pf.max_drawdown()
        max_drawdown = (
            0.0 if max_dd_val is None or np.isnan(max_dd_val) else float(max_dd_val * 100.0)
        )

        sharpe_val = pf.sharpe_ratio()
        sharpe_ratio = 0.0 if sharpe_val is None or np.isnan(sharpe_val) else float(sharpe_val)

    except Exception as e:
        msg = "Failed to calculate metrics due to internal error."
        raise BacktestCalculationError(msg) from e

    if np.isnan(total_return) or np.isnan(max_drawdown) or np.isnan(win_rate):
        msg = "Insufficient trades occurred to compute valid risk metrics."
        raise BacktestCalculationError(msg)

    return BacktestMetrics(
        total_return=total_return,
        annualized_return=annualized_return,
        max_drawdown=max_drawdown,
        win_rate=win_rate,
        sharpe_ratio=sharpe_ratio,
    )
