from pydantic import BaseModel, ConfigDict, Field, field_validator


class StatResult(BaseModel):
    """
    Schema for statistical test results comparing target day returns against others.
    """
    model_config = ConfigDict(extra="forbid")

    target_day: int = Field(..., description="Target day of the week (e.g., 0 for Monday, 4 for Friday)")
    t_statistic: float = Field(..., description="T-statistic from the t-test")
    p_value: float = Field(..., description="P-value from the t-test")
    is_significant: bool = Field(..., description="True if statistically significant, False otherwise")

    @field_validator("p_value")
    @classmethod
    def validate_p_value(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            msg = "p_value must be between 0.0 and 1.0 inclusive."
            raise ValueError(msg)
        return v


class BacktestMetrics(BaseModel):
    """
    Schema for evaluating the algorithmic simulation results.
    """
    model_config = ConfigDict(extra="forbid")

    total_return: float = Field(..., description="Total cumulative return as a percentage")
    annualized_return: float = Field(..., description="Annualized return")
    max_drawdown: float = Field(..., description="Maximum drawdown from peak to trough as a percentage")
    win_rate: float = Field(..., description="Win rate of the trades as a percentage")
    sharpe_ratio: float = Field(..., description="Sharpe ratio of the strategy")

    @field_validator("max_drawdown")
    @classmethod
    def validate_max_drawdown(cls, v: float) -> float:
        if v > 0:
            msg = "max_drawdown must be a negative number or zero."
            raise ValueError(msg)
        return v

    @field_validator("win_rate")
    @classmethod
    def validate_win_rate(cls, v: float) -> float:
        if not (0.0 <= v <= 100.0):
            msg = "win_rate must be between 0.0 and 100.0 inclusive."
            raise ValueError(msg)
        return v
