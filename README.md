# Japanese Stock Market Analysis Pipeline

A highly robust, end-to-end data pipeline designed to ingest, process, and persist Japanese stock market data. This tool efficiently fetches daily quotes, engineers critical financial features, and securely stores the structured data for highly optimized quantitative analysis and backtesting.

## Features

* **Secure External Data Ingestion:** Safely retrieves daily market quotes via the J-Quants API using robust 2-step authentication.
* **Resilient Network Communication:** Incorporates complex retry logic with exponential backoff to handle rate limits and transient network failures effortlessly.
* **Advanced Feature Engineering:** Utilizes Polars to efficiently calculate sophisticated metrics including daily, intraday, and overnight returns, as well as critical date-based flags (day of week, month boundaries).
* **Strict Data Validation:** Employs precise Pydantic domain models to relentlessly enforce market invariants and guarantee absolutely clean data at all pipeline stages.
* **Highly Optimized Storage:** Persists processed data in highly compressed Parquet format, immediately accessible for complex SQL queries directly via an integrated DuckDB connection.

## Installation

Ensure you have [uv](https://docs.astral.sh/uv/) installed to handle dependencies.

```bash
# Clone the repository
git clone <repository_url>
cd <repository_directory>

# Synchronize dependencies
uv sync
```

## Setup Configuration

Copy the example environment file and add your actual API credentials:

```bash
cp .env.example .env
```

Open `.env` and configure your refresh token:
```env
JQUANTS_REFRESH_TOKEN=your_actual_jquants_token_here
```

## Usage

Here is a simple Python snippet demonstrating how to use the pipeline to fetch data, process it, and query the results:

```python
import os
from src.core.config import AppSettings
from src.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.repository.repository import save_quotes, query_quotes

# 1. Load settings (automatically reads from .env)
settings = AppSettings()

# 2. Fetch raw data from J-Quants
client = JQuantsClient(refresh_token=settings.JQUANTS_REFRESH_TOKEN)
print("Fetching quotes...")
raw_quotes = client.fetch_daily_quotes(code="86970") # E.g., Japan Exchange Group

# 3. Process into a Polars DataFrame with advanced features
print("Processing data...")
df = transform_quotes(raw_quotes)

# 4. Save to Parquet format
file_path = "data/processed_quotes.parquet"
save_quotes(df, file_path)
print(f"Data saved to {file_path}")

# 5. Query the saved data using DuckDB
result = query_quotes(file_path, "SELECT * FROM data LIMIT 5")
print("\nQuery Results:")
print(result)
```

## Project Structure

```text
├── src/
│   ├── core/              # Configuration and strict environment management
│   ├── domain_models/     # Pydantic schemas (RawQuote, ProcessedQuote)
│   ├── processing/        # Polars transformations and feature engineering
│   ├── repository/        # Parquet persistence and DuckDB queries
│   └── jquants_client.py  # J-Quants API client with retry and validation
├── tests/                 # Unit, Integration, and UAT specifications
├── .env.example           # Template for required secrets
├── pyproject.toml         # Dependencies and tool configurations
└── README.md              # User documentation
```
