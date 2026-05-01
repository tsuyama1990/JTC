import numpy as np
import pandas as pd
from statsmodels.stats.weightstats import ttest_ind

from src.domain_models import StatResult


def evaluate_day_of_week_returns(df: pd.DataFrame, target_day: int = 0) -> StatResult:
    """
    Evaluates if the returns on a specific target day (e.g., 0 for Monday)
    are statistically significantly different from returns on other days.
    """
    # Ensure Date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
        df["Date"] = pd.to_datetime(df["Date"])

    target_returns = df.loc[df["Date"].dt.dayofweek == target_day, "Return"].values
    other_returns = df.loc[df["Date"].dt.dayofweek != target_day, "Return"].values

    if len(target_returns) == 0 or len(other_returns) == 0:
        return StatResult(target_day=target_day, t_statistic=0.0, p_value=1.0, is_significant=False)

    # Check for zero variance
    if np.var(target_returns) == 0 and np.var(other_returns) == 0:
        return StatResult(target_day=target_day, t_statistic=0.0, p_value=1.0, is_significant=False)

    # Perform Welch's t-test (robust to unequal variances)
    t_stat, p_val, _ = ttest_ind(target_returns, other_returns, usevar="unequal")

    # Handle NaN values returned by statsmodels in extreme edge cases
    if np.isnan(t_stat) or np.isnan(p_val):
        t_stat = 0.0
        p_val = 1.0

    # Convert to standard Python float for Pydantic
    t_stat = float(t_stat)
    p_val = float(p_val)

    return StatResult(
        target_day=target_day, t_statistic=t_stat, p_value=p_val, is_significant=p_val < 0.05
    )
