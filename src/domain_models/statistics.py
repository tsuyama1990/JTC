from pydantic import BaseModel, ConfigDict, Field, field_validator


class StatResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_day: int
    t_statistic: float
    p_value: float = Field(ge=0.0, le=1.0)
    is_significant: bool

    @field_validator("p_value")
    @classmethod
    def validate_p_value(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            msg = "p_value must be between 0.0 and 1.0 inclusive"
            raise ValueError(msg)
        return v
