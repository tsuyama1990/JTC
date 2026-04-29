from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    # SPEC requires: `model_config = ConfigDict(extra="forbid")`
    model_config = ConfigDict(extra="forbid", strict=False)

    Date: str | date
    Code: str | None = None
    Open: float | int | None = None
    High: float | int | None = None
    Low: float | int | None = None
    Close: float | int | None = None
    Volume: float | int = Field(ge=0)
    TurnoverValue: float | int | None = None
    AdjustmentHigh: float | int | None = None
    AdjustmentLow: float | int | None = None
    AdjustmentOpen: float | int | None = None
    AdjustmentClose: float | int | None = None
    AdjustmentFactor: float | int | None = None
    AdjustmentVolume: float | int | None = None
    AdjustmentTurnoverValue: float | int | None = None
    MorningClose: float | int | None = None
    MorningHigh: float | int | None = None
    MorningLow: float | int | None = None
    MorningOpen: float | int | None = None
    MorningTurnoverValue: float | int | None = None
    MorningVolume: float | int | None = None
    AfternoonClose: float | int | None = None
    AfternoonHigh: float | int | None = None
    AfternoonLow: float | int | None = None
    AfternoonOpen: float | int | None = None
    AfternoonTurnoverValue: float | int | None = None
    AfternoonVolume: float | int | None = None
    LowerLimit: float | int | None = None
    UpperLimit: float | int | None = None

    @model_validator(mode="after")
    def check_high_low(self) -> "RawQuote":
        msg = "High cannot be less than Low"
        if self.High is not None and self.Low is not None and self.High < self.Low:
            raise ValueError(msg)
        return self
