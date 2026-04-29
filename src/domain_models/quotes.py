from datetime import date
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: date
    code: str
    open: float
    high: float
    low: float
    close: float
    volume: float

    @model_validator(mode="after")
    def check_high_low(self) -> Self:
        if self.high < self.low:
            msg = "high price must be greater than or equal to low price"
            raise ValueError(msg)
        return self


class ProcessedQuote(RawQuote):
    day_of_week: int = Field(ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
    daily_return: float
    intraday_return: float
    overnight_return: float
