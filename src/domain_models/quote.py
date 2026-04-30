from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: str | date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @model_validator(mode='after')
    def validate_prices(self) -> 'RawQuote':
        if self.high < self.low:
            err_msg = "high price must be >= low price"
            raise ValueError(err_msg)
        if self.high < self.open:
            err_msg = "high price must be >= open price"
            raise ValueError(err_msg)
        if self.high < self.close:
            err_msg = "high price must be >= close price"
            raise ValueError(err_msg)
        return self

class ProcessedQuote(RawQuote):
    model_config = ConfigDict(extra="forbid")

    day_of_week: int = Field(ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
    daily_return: float
    intraday_return: float
    overnight_return: float
