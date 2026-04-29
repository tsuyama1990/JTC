from pydantic import BaseModel, ConfigDict, Field, model_validator


class RawQuote(BaseModel):
    """Raw quote model straight from J-Quants API."""

    model_config = ConfigDict(extra="forbid")

    date: str = Field(..., description="Trade date in YYYY-MM-DD format")
    open: float | None = Field(default=None, description="Open price")
    high: float | None = Field(default=None, description="High price")
    low: float | None = Field(default=None, description="Low price")
    close: float | None = Field(default=None, description="Close price")
    volume: float | None = Field(default=None, description="Trading volume")

    @model_validator(mode="after")
    def validate_high_low(self) -> "RawQuote":
        if self.high is not None and self.low is not None and self.high < self.low:
            msg = "high price must be greater than or equal to low price"
            raise ValueError(msg)
        return self


class ProcessedQuote(BaseModel):
    """Processed quote model with calculated features."""

    model_config = ConfigDict(extra="forbid")

    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None

    daily_return: float | None
    intraday_return: float | None
    overnight_return: float | None
    day_of_week: int = Field(..., ge=1, le=5)
    is_month_start: bool
    is_month_end: bool
