# J-Quants Stock Data Pipeline

## Overview
This system is a robust, reliable, and secure data pipeline for fetching, transforming, and persistently storing Japanese stock market data using the free tier of the J-Quants API. It is designed to provide highly accurate historical daily price quotes continuously covering the past twelve weeks, forming a rock-solid foundation for advanced quantitative research and calendar anomaly analysis.

## Features
- **Automated Data Ingestion**: Seamlessly fetches daily quotes from the J-Quants API for the last 12 weeks.
- **Robust Authentication & Error Handling**: Fully handles the two-step token exchange protocol and uses exponential backoff to handle rate limits and transient network errors gracefully.
- **Feature Engineering Engine**: Uses Polars for lightning-fast calculations of important statistical features, including:
  - Daily, intraday, and overnight returns.
  - Categorical date features (day of week, start of month, end of month).
- **Strict Data Validation**: Employs Pydantic domain models to validate raw data from the API and the engineered features before storing them.
- **Optimized Storage**: Saves the fully enriched dataset as highly compressed Parquet files for fast I/O.
- **SQL Analytics Interface**: Deeply integrates DuckDB, allowing you to query your processed Parquet data using standard SQL.

## Installation

Ensure you have Python 3.12+ and `uv` installed.

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd <repo-dir>
   ```

2. Sync dependencies:
   ```bash
   uv sync
   ```

3. Configure your environment:
   Create a `.env` file in the root directory and add your J-Quants refresh token:
   ```env
   JQUANTS_REFRESH_TOKEN=your_refresh_token_here
   ```

## Usage

You can use the pipeline programmatically in your own scripts or notebooks. Here is a basic example showing how to ingest data and query it:

```python
import datetime
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import QuoteRepository
from src.core.config import AppSettings

# 1. Load Configurations
settings = AppSettings()

# 2. Ingest Data
client = JQuantsClient(refresh_token=settings.JQUANTS_REFRESH_TOKEN)
# Fetches the last 12 weeks of data dynamically
raw_quotes = client.fetch_last_12_weeks()

# 3. Process & Enrich
df = transform_quotes(raw_quotes)

# 4. Save to Storage
repo = QuoteRepository(data_dir="data")
repo.save_processed_quotes(df)

# 5. Query using DuckDB
# Find all Wednesday returns
result_df = repo.query_quotes("SELECT * FROM processed_quotes WHERE day_of_week = 3")
print(result_df)
```

## Structure
```text
src/
├── core/             # Configuration and custom exceptions
├── domain_models/    # Pydantic schemas (RawQuote, ProcessedQuote)
├── ingestion/        # J-Quants API client with retry logic
├── processing/       # Polars transformation engine
└── storage/          # Parquet saving and DuckDB query interface
```
