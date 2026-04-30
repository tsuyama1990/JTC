# Japanese Stock Calendar Anomaly Validation System

A robust, mathematically sound Proof of Concept (PoC) for ingesting, transforming, and analyzing calendar anomalies within Japanese equity markets. It leverages modern Python tools (Polars, DuckDB) to process live data from the J-Quants API.

## Features

- **J-Quants API Integration:** Connects seamlessly to the J-Quants API (Free tier) to pull historical daily stock quotes dynamically spanning the last 12 weeks. Handles rate limits and network errors with exponential backoff.
- **High-Performance Transformations:** Utilizes Polars to efficiently calculate and add crucial market features, such as exact trading days (`day_of_week`), `is_month_start`, `is_month_end`, `daily_return`, `intraday_return`, and `overnight_return`.
- **Permanent Local Storage:** Enriched data is persisted into an optimized Parquet file format.
- **DuckDB SQL Querying:** Embedded DuckDB engine allows rapid, SQL-based data querying without additional infrastructure.
- **Strict Data Integrity:** Employs Pydantic `BaseModel` for validation at boundaries.

## Installation

```bash
# Initialize and sync the virtual environment using uv
uv sync
```

## Setup
Create your configuration file by copying the example:

```bash
cp .env.example .env
```
Ensure that you set the value for `JQUANTS_REFRESH_TOKEN` in the `.env` file before running the application.

## Usage

You can ingest, transform, and store quotes directly using the pipeline components in Python.

```python
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import StorageRepository
import os
from dotenv import load_dotenv

load_dotenv()
token = os.environ.get("JQUANTS_REFRESH_TOKEN")

# 1. Fetch data
client = JQuantsClient(refresh_token=token)
raw_quotes = client.fetch_historical_data()

# 2. Transform into a Polars DataFrame
df = transform_quotes(raw_quotes)

# 3. Persist and Query
repo = StorageRepository()
repo.save_data(df)

# Execute SQL via DuckDB
result = repo.query_data("SELECT * FROM 'data/processed_quotes.parquet' WHERE day_of_week = 1 LIMIT 5")
print(result)
```

## Structure
- `src/domain_models/`: Pydantic definitions and Configs.
- `src/ingestion/`: J-Quants HTTP client.
- `src/processing/`: Polars feature transformation engine.
- `src/storage/`: DuckDB & Parquet persistence logic.
- `tests/`: Comprehensive unit, integration, and E2E Pytest suites.
