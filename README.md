# Japanese Stock Market Anomaly Analytics (PoC)

An advanced Proof of Concept for robustly fetching, verifying, and transforming historical Japanese stock market quotes. It establishes a perfectly decoupled ETL (Extract, Transform, Load) pipeline using rigorous domain model contracts.

## Features

- **Robust Ingestion**: Fetches the last 12 weeks of daily quotes from the J-Quants Free API, gracefully handling transient network errors and rate limits with exponential backoff retries.
- **Strict Data Integrity**: Ensures structural and logical correctness using robust Pydantic schemas right at the system boundaries. Validates constraints like `High >= Low`.
- **Lightning Fast Transformations**: Uses the high-performance Polars library to compute daily returns, intraday returns, overnight returns, and precise calendar flags (day of the week, month start, and month end).
- **Optimized Storage**: Saves the fully enriched historical datasets persistently in the deeply compressed Parquet format.
- **SQL Analytics Ready**: Integrated directly with DuckDB, allowing complex analysis scripts to natively query the Parquet files using familiar SQL interfaces.

## Setup

1. **Clone the repository**

2. **Initialize Environment**
   ```bash
   uv sync
   ```

3. **Configure Secrets**
   Copy the `.env.example` file to `.env` and provide your J-Quants Refresh Token:
   ```bash
   cp .env.example .env
   # Edit .env with your JQUANTS_REFRESH_TOKEN
   ```

## Usage

You can use the core components to build custom execution flows. Here's a typical example fetching quotes, transforming them, and saving to your filesystem.

```python
import polars as pl

from src.domain_models.config import AppSettings
from src.jquants_client import JQuantsClient
from src.transformers import transform_quotes
from src.repository import QuoteRepository

# 1. Configuration (Fails fast if token is missing)
settings = AppSettings()

# 2. Extract
client = JQuantsClient(settings.JQUANTS_REFRESH_TOKEN)
raw_quotes = client.fetch_daily_quotes()

# 3. Transform
processed_df = transform_quotes(raw_quotes)

# 4. Load
repo = QuoteRepository()
repo.save(processed_df, "data/processed_quotes.parquet")

# 5. Analyze
df_mondays = repo.query(
    "data/processed_quotes.parquet",
    "SELECT * FROM 'data/processed_quotes.parquet' WHERE day_of_week = 1"
)
print(df_mondays)
```

## Structure

- `src/domain_models/`: Pydantic domain schemas (`RawQuote`, `ProcessedQuote`, `AppSettings`).
- `src/jquants_client.py`: API integration handling authentication, rate limits, and JSON parsing.
- `src/transformers.py`: Polars expressions to build essential analytics features.
- `src/repository.py`: File I/O for saving to Parquet and retrieving queries via DuckDB.
- `tests/`: Comprehensive unit, e2e, and User Acceptance Test (UAT) suites covering critical behaviors.
