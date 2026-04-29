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
    def validate_prices(self) -> Self:
        err_msg_low = "High price must be >= low price"
        if self.high < self.low:
            raise ValueError(err_msg_low)
        err_msg_open = "High price must be >= open price"
        if self.high < self.open:
            raise ValueError(err_msg_open)
        err_msg_close = "High price must be >= close price"
        if self.high < self.close:
            raise ValueError(err_msg_close)
        return self

class ProcessedQuote(RawQuote):
    day_of_week: int = Field(ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
    daily_return: float | None
    intraday_return: float
    overnight_return: float | None

    model_config = ConfigDict(extra="forbid")
