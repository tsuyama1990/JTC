from pydantic import BaseModel, ConfigDict, field_validator


class BacktestMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_return: float
    annualized_return: float
    max_drawdown: float
    win_rate: float
    sharpe_ratio: float

    @field_validator("max_drawdown")
    @classmethod
    def validate_max_drawdown(cls, v: float) -> float:
        if v > 0:
            msg = "max_drawdown must be a negative number or zero"
            raise ValueError(msg)
        return v

    @field_validator("win_rate")
    @classmethod
    def validate_win_rate(cls, v: float) -> float:
        if not (0.0 <= v <= 100.0):
            msg = "win_rate must be between 0.0 and 100.0 inclusive"
            raise ValueError(msg)
        return v
