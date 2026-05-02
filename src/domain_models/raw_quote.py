from datetime import date

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
    def validate_prices(self) -> "RawQuote":
        if self.high < self.low:
            msg = "High price cannot be lower than low price."
            raise ValueError(msg)
        if self.high < self.open:
            msg = "High price cannot be lower than open price."
            raise ValueError(msg)
        if self.high < self.close:
            msg = "High price cannot be lower than close price."
            raise ValueError(msg)
        if self.low > self.open:
            msg = "Low price cannot be higher than open price."
            raise ValueError(msg)
        if self.low > self.close:
            msg = "Low price cannot be higher than close price."
            raise ValueError(msg)
        return self
