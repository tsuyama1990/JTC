from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict


class ProcessedQuote(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    day_of_week: Literal[1, 2, 3, 4, 5]
    is_month_start: bool
    is_month_end: bool
    daily_return: float | None  # Can be None for the first row
    intraday_return: float
    overnight_return: float | None # Can be None for the first row

    model_config = ConfigDict(extra="forbid")
