import polars as pl
from statsmodels.stats.weightstats import ttest_ind

from src.domain_models.schemas import StatResult


def evaluate_calendar_anomaly(
    df: pl.DataFrame, target_day: int, return_col: str = "daily_return"
) -> StatResult:
    """
    Evaluates statistical significance of returns on a specific day vs all other days.

    Args:
        df: Polars DataFrame containing data. Must have 'day_of_week' (1-5 typically) and `return_col` columns.
        target_day: The integer representing the day of the week to analyze.
        return_col: The column containing the return metric to evaluate.

    Returns:
        StatResult object containing the t-statistic, p-value, and significance flag.
    """

    target_returns = (
        df.filter(pl.col("day_of_week") == target_day)
        .select(return_col)
        .to_series()
        .drop_nulls()
        .to_numpy()
    )
    other_returns = (
        df.filter(pl.col("day_of_week") != target_day)
        .select(return_col)
        .to_series()
        .drop_nulls()
        .to_numpy()
    )

    if len(target_returns) < 2 or len(other_returns) < 2:
        return StatResult(
            target_day=target_day,
            t_statistic=0.0,
            p_value=1.0,
            is_significant=False,
        )

    var_target = target_returns.var(ddof=1)
    var_other = other_returns.var(ddof=1)

    if var_target == 0.0 or var_other == 0.0:
        return StatResult(
            target_day=target_day,
            t_statistic=0.0,
            p_value=1.0,
            is_significant=False,
        )

    tstat, pvalue, _ = ttest_ind(target_returns, other_returns, usevar="unequal")

    return StatResult(
        target_day=target_day,
        t_statistic=float(tstat),
        p_value=float(pvalue),
        is_significant=bool(pvalue < 0.05),
    )
