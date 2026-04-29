from datetime import date
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int = Field(ge=0)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def check_high_is_highest(self) -> Self:
        if self.high < self.low:
            msg = f"high ({self.high}) cannot be less than low ({self.low})"
            raise ValueError(msg)
        if self.high < self.open:
            msg = f"high ({self.high}) cannot be less than open ({self.open})"
            raise ValueError(msg)
        if self.high < self.close:
            msg = f"high ({self.high}) cannot be less than close ({self.close})"
            raise ValueError(msg)
        return self

class ProcessedQuote(RawQuote):
    day_of_week: int = Field(ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
    daily_return: float | None = None
    intraday_return: float
    overnight_return: float | None = None

    model_config = ConfigDict(extra="forbid")
