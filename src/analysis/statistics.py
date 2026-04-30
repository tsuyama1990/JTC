import numpy as np
import pandas as pd
from statsmodels.stats.weightstats import ttest_ind

from src.domain_models.statistics import StatResult


def evaluate_day_anomaly(df: pd.DataFrame, target_day: int, alpha: float = 0.05) -> StatResult:
    """
    Evaluates whether the returns on the target_day are significantly different
    from the returns on all other days using Welch's t-test (unequal variances).
    """
    if "return" not in df.columns or "day_of_week" not in df.columns:
        msg = "DataFrame must contain 'return' and 'day_of_week' columns."
        raise ValueError(msg)

    target_returns = df[df["day_of_week"] == target_day]["return"].values
    other_returns = df[df["day_of_week"] != target_day]["return"].values

    if len(target_returns) < 2 or len(other_returns) < 2:
        return StatResult(
            target_day=target_day,
            t_statistic=0.0,
            p_value=1.0,
            is_significant=False,
        )

    # Check for zero variance edge case
    if np.var(target_returns) == 0.0 and np.var(other_returns) == 0.0:
        return StatResult(
            target_day=target_day,
            t_statistic=0.0,
            p_value=1.0,
            is_significant=False,
        )

    try:
        t_statistic, p_value, _ = ttest_ind(
            target_returns,
            other_returns,
            alternative="two-sided",
            usevar="unequal",
        )

        # Handle nan returned by statsmodels in extreme cases
        if np.isnan(t_statistic) or np.isnan(p_value):
            t_statistic = 0.0
            p_value = 1.0

    except Exception:
        t_statistic = 0.0
        p_value = 1.0

    return StatResult(
        target_day=target_day,
        t_statistic=float(t_statistic),
        p_value=float(p_value),
        is_significant=bool(p_value < alpha),
    )
