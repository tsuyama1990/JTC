# J-Quants Market Data Pipeline

## Overview
This system provides an end-to-end Extract, Transform, Load (ETL) pipeline specifically designed for ingesting historical Japanese stock price data from the J-Quants API. It applies data validation via Pydantic, financial metric transformations using Polars, and stores the highly-optimized results in Parquet files capable of being instantly queried using DuckDB.

## Features
- **Secure Authentication**: Handles dynamic retrieval of API ID tokens from a configured refresh token.
- **Robust Ingestion**: Employs an exponential backoff retry mechanism (using `tenacity`) specifically designed to gracefully overcome transient errors (HTTP 429, 500+).
- **Fast Transformations**: Analyzes daily quote data to generate essential indicators:
  - Day of the Week logic mapped correctly from historical dates.
  - Boolean Flags for tracking `is_month_start` and `is_month_end`.
  - Intraday, Daily, and Overnight percentage returns utilizing highly parallelized Polars arithmetic.
- **Efficient Storage**: Persists data locally into Parquet (`data/processed_quotes.parquet`) while exposing a DuckDB integration to easily execute standard SQL.
- **Strict Validation**: All data is mathematically verified and validated via Pydantic structures (e.g., ensuring invariants like `high >= low`).

## Installation

Ensure you have Python 3.12+ and `uv` installed.

1. Clone this repository.
2. Setup the environment dependencies:
   ```bash
   uv sync
   ```

3. Configure your API secrets:
   ```bash
   cp .env.example .env
   # Add your specific refresh token inside .env:
   # JQUANTS_REFRESH_TOKEN=your-token-here
   ```

## Usage

The pipeline can be executed via standard python scripts or explored interactively. All core ingestion flows are available via `JQuantsClient`, processors through `transformers.py`, and localized query access through `StorageRepository`.

```python
from datetime import datetime, timezone, timedelta
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import process_quotes
from src.storage.repository import StorageRepository
import os

# Initialize client securely
client = JQuantsClient(refresh_token=os.environ["JQUANTS_REFRESH_TOKEN"])

# Fetch last 12 weeks of data (e.g. Toyota - 72030)
end_date = datetime.now(timezone.utc)
start_date = end_date - timedelta(weeks=12)
raw_quotes = client.fetch_daily_quotes("72030", start_date, end_date)

# Process highly-optimized metrics
df = process_quotes(raw_quotes)

# Store and query data
repo = StorageRepository("data/processed_quotes.parquet")
repo.save(df)

# Execute analytics instantly via SQL DuckDB engine
print(repo.query("SELECT * FROM 'data' WHERE day_of_week = 1"))
```

## Structure
```
src/
├── core/            # Configuration and Exception handling
├── domain_models/   # Strict Pydantic Data Structures
├── ingestion/       # Live External APIs (J-Quants Client)
├── processing/      # Polars Financial Math & Logic
└── storage/         # DuckDB Data persistence
```
