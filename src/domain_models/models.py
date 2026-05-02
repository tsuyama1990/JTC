from datetime import date
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def check_high_is_maximum(self) -> Self:
        if self.high < self.low:
            msg = "high price cannot be less than low price"
            raise ValueError(msg)
        if self.high < self.open:
            msg = "high price cannot be less than open price"
            raise ValueError(msg)
        if self.high < self.close:
            msg = "high price cannot be less than close price"
            raise ValueError(msg)
        return self


class ProcessedQuote(RawQuote):
    day_of_week: int = Field(ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
    daily_return: float
    intraday_return: float
    overnight_return: float

    model_config = ConfigDict(extra="forbid")
