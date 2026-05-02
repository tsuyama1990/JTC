from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @model_validator(mode="after")
    def validate_high_prices(self) -> "RawQuote":
        if self.high < self.low:
            msg = "High price cannot be strictly less than low price"
            raise ValueError(msg)
        if self.high < self.open:
            msg = "High price cannot be strictly less than open price"
            raise ValueError(msg)
        if self.high < self.close:
            msg = "High price cannot be strictly less than close price"
            raise ValueError(msg)
        return self

class ProcessedQuote(RawQuote):
    model_config = ConfigDict(extra="forbid")

    day_of_week: int = Field(ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
    daily_return: float | None = None
    intraday_return: float | None = None
    overnight_return: float | None = None
