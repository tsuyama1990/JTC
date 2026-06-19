# J-Quants Data Pipeline

A robust data pipeline for fetching, validating, and transforming daily stock quotes from the J-Quants API into an optimized Parquet dataset using DuckDB and Polars.

## Features

- **Robust External Ingestion:** Fetches the last 12 weeks of daily quotes with intelligent retry algorithms and exponential backoff.
- **Strict Data Validation:** Utilizes Pydantic schemas to strictly validate financial boundaries (e.g. `high >= low`) and enforce structure.
- **High-Performance Transformation:** Uses Polars to rapidly compute daily, intraday, and overnight returns, as well as metadata features like `day_of_week`, `is_month_start`, and `is_month_end`.
- **Local Storage Integration:** Persists the enriched data into efficiently compressed Parquet files.
- **SQL Analytics:** Easily query the resulting Parquet files with standard SQL using DuckDB.

## Installation

1. Ensure you have [`uv`](https://github.com/astral-sh/uv) installed on your system.
2. Clone this repository.
3. Install dependencies:
   ```bash
   uv sync
   ```
4. Copy the environment variables example file and fill in your details:
   ```bash
   cp .env.example .env
   ```
   *Note: Add your actual J-Quants refresh token to the `.env` file.*

## Usage

You can fetch and transform quotes directly using the pipeline components. Below is a minimal script example to get started:

```python
import os
from src.clients.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import save_quotes, query_quotes

# 1. Initialize client with your token
client = JQuantsClient(refresh_token=os.environ["JQUANTS_REFRESH_TOKEN"])

# 2. Fetch the last 12 weeks of data for a specific stock code (e.g., "8697" for JPX)
quotes = client.fetch_quotes("8697")

# 3. Transform the raw data into enriched Polars DataFrame
df = transform_quotes(quotes)

# 4. Save to a local Parquet file
save_quotes(df, "data/processed_quotes.parquet")

# 5. Query the saved data with DuckDB
result = query_quotes("data/processed_quotes.parquet", "SELECT * FROM 'data/processed_quotes.parquet' WHERE day_of_week = 1")
print(result)
```

## Structure

- `src/domain_models/`: Pydantic models enforcing data contracts.
- `src/clients/`: Robust API clients with resilient retry mechanisms.
- `src/processing/`: Lightning-fast data transformations powered by Polars.
- `src/storage/`: Clean abstraction for writing Parquet and querying with DuckDB.
- `tests/`: Comprehensive unit, E2E, and UAT test suites.
