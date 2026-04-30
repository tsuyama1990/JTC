from datetime import date, datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    """Raw quote model reflecting the data received from J-Quants API."""

    model_config = ConfigDict(extra="forbid")

    date: date | datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    @model_validator(mode="after")
    def validate_price_invariants(self) -> Self:
        """Validate critical market invariants."""
        msg_high_low = "High price must be greater than or equal to low price."
        if self.high < self.low:
            raise ValueError(msg_high_low)
        msg_high_open = "High price must be greater than or equal to open price."
        if self.high < self.open:
            raise ValueError(msg_high_open)
        msg_high_close = "High price must be greater than or equal to close price."
        if self.high < self.close:
            raise ValueError(msg_high_close)
        msg_low_open = "Low price must be less than or equal to open price."
        if self.low > self.open:
            raise ValueError(msg_low_open)
        msg_low_close = "Low price must be less than or equal to close price."
        if self.low > self.close:
            raise ValueError(msg_low_close)
        return self


class ProcessedQuote(RawQuote):
    """Processed quote model encompassing calculated statistical features."""

    day_of_week: int = Field(ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
    daily_return: float
    intraday_return: float
    overnight_return: float
